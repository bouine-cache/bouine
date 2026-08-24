package cache

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/bouine-cache/bouine/internal/observability/tracing"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// streamBufPool reuses bytes.Buffer instances for tee-ing origin response
// bodies to the cache store during streaming. Buffers larger than 1 MiB
// are discarded to prevent the pool from pinning oversized buffers.
var streamBufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

const maxStreamBufRetain = 1 << 20

// streamFetchResult carries the origin response state needed to stream
// the body to the client while concurrently buffering it for the cache.
// When buffered is true, the body is already in resp.Body() (the client
// doesn't support streaming) and callers should use the buffered path.
type streamFetchResult struct {
	StatusCode int
	Header     headerLookup
	resp       *fasthttp.Response // body stream still open (or buffered)
	req        *fasthttp.Request  // for release after stream
	sem        chan struct{}      // semaphore to release after stream
	cancel     context.CancelFunc
	buffered   bool // true when resp.BodyStream() is nil (test clients)
}

// inflightStream tracks an in-progress streaming fetch so that
// concurrent requests for the same key (singleflight followers) can
// wait for the leader's body to be fully buffered and then serve
// the buffered result instead of issuing a duplicate origin fetch.
type inflightStream struct {
	done chan struct{} // closed when body is fully buffered
	res  fetchResult   // set by leader before closing done
	err  error         // set by leader on fetch error
}

// doFetchStream starts an origin fetch with response body streaming
// enabled (resp.StreamBody = true). It reads the response headers but
// does NOT read the body — the body is read inside a SetBodyStreamWriter
// callback that runs during ctx.Response.WriteTo(conn).
//
// If the client doesn't support body streaming (resp.BodyStream() is
// nil after Do), the buffered flag is set and resp.Body() contains the
// full body. In this case, callers should use the buffered path.
//
// The semaphore and request/response pools are released inside the
// stream writer (for streaming mode) or by the caller (for buffered mode).
func (h *Handler) doFetchStream(ctx *fasthttp.RequestCtx) (*streamFetchResult, error) {
	if h.fastClient == nil {
		return nil, fmt.Errorf("no fast client configured")
	}
	bgCtx := context.Background()
	spanCtx, span := tracing.StartSpan(bgCtx, "bouine.origin")

	select {
	case h.fetchSem <- struct{}{}:
	case <-spanCtx.Done():
		span.End()
		return nil, fmt.Errorf("origin fetch semaphore: %w", spanCtx.Err())
	}

	fetchCtx, cancel := context.WithCancel(spanCtx)
	if h.fetchTimeout > 0 {
		timer := time.AfterFunc(h.fetchTimeout, cancel)
		defer timer.Stop()
	}

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	resp.StreamBody = true

	req.Header.SetMethodBytes(ctx.Method())
	req.SetRequestURIBytes(ctx.RequestURI())
	req.Header.SetHostBytes(ctx.Host())
	for k, v := range ctx.Request.Header.All() {
		req.Header.AddBytesKV(k, v)
	}
	tracing.InjectFastHTTP(fetchCtx, req)

	if err := h.fastClient.Do(fetchCtx, req, resp); err != nil {
		cancel()
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
		<-h.fetchSem
		tracing.RecordError(span, err)
		span.End()
		return nil, fmt.Errorf("origin fetch: %w", err)
	}

	// Check Content-Length against maxResponseBytes if available.
	if h.maxResponseBytes > 0 {
		if cl := resp.Header.ContentLength(); cl > 0 && int64(cl) > h.maxResponseBytes {
			cancel()
			fasthttp.ReleaseRequest(req)
			fasthttp.ReleaseResponse(resp)
			<-h.fetchSem
			span.End()
			return nil, fmt.Errorf("upstream response exceeds %d bytes", h.maxResponseBytes)
		}
	}

	sf := &streamFetchResult{
		StatusCode: resp.StatusCode(),
		Header:     fromFastHTTPHeader(&resp.Header),
		resp:       resp,
		req:        req,
		sem:        h.fetchSem,
		cancel:     cancel,
		buffered:   !resp.IsBodyStream(),
	}

	// In buffered mode, the span ends now (no stream writer to end it later).
	if sf.buffered {
		span.End()
	}

	return sf, nil
}

// releaseStreamFetch cleans up pooled resources. For streaming mode,
// call after the body stream has been fully consumed. For buffered mode,
// call immediately.
func releaseStreamFetch(sf *streamFetchResult) {
	sf.cancel()
	if sf.resp != nil {
		_ = sf.resp.CloseBodyStream()
		fasthttp.ReleaseRequest(sf.req)
		fasthttp.ReleaseResponse(sf.resp)
	}
	<-sf.sem
}

// streamBypass fetches the origin response and streams it directly to
// the client without buffering. Used for BYPASS path where the response
// is not cached.
func (h *Handler) streamBypass(ctx *fasthttp.RequestCtx, xCacheHeader string) {
	sf, err := h.doFetchStream(ctx)
	if err != nil {
		ctx.Error("upstream error", fasthttp.StatusBadGateway)
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b(xCacheHeader))
		return
	}

	// Copy origin response headers, skipping bouine attribution headers.
	dst := &ctx.Response.Header
	for k, v := range sf.resp.Header.All() {
		if bytes.Equal(k, []byte(header.XCache)) || bytes.Equal(k, []byte(header.XCacheSource)) {
			continue
		}
		dst.AddBytesKV(k, v)
	}
	dst.SetCanonical(header.S2b(header.XCache), header.S2b(xCacheHeader))
	ctx.SetStatusCode(sf.StatusCode)

	isHEAD := bytes.Equal(ctx.Method(), []byte("HEAD"))

	if isHEAD {
		releaseStreamFetch(sf)
		return
	}

	if sf.buffered {
		// Client doesn't support streaming — write body directly.
		_, _ = ctx.Write(sf.resp.Body())
		releaseStreamFetch(sf)
		return
	}

	// Stream the origin body directly to the client.
	bodyStream := sf.resp.BodyStream()
	ctx.Response.SetBodyStreamWriter(func(w *bufio.Writer) {
		_, writeErr := io.Copy(w, bodyStream)
		if writeErr != nil {
			h.logger.Debug("stream bypass: body copy error", "error", writeErr)
		}
		releaseStreamFetch(sf)
	})
}

// streamMiss fetches the origin response, streams it to the client while
// concurrently buffering for cache storage. Handles singleflight: the
// leader streams, followers wait for the buffered result.
func (h *Handler) streamMiss(
	ctx *fasthttp.RequestCtx,
	primaryKey api.Key,
	ri RequestInfo,
	inflight *inflightStream,
) {
	sf, err := h.doFetchStream(ctx)
	if err != nil {
		inflight.err = err
		inflight.res = fetchResult{Err: err}
		close(inflight.done)
		ctx.Error("upstream error", fasthttp.StatusBadGateway)
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
		ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
		return
	}

	// Set up response headers for the client.
	dst := &ctx.Response.Header
	sf.Header.CopyToFastHTTP(dst)
	dst.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
	dst.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
	resMap := sf.Header.ToMap()
	if resMap.Get(header.Age) == "" {
		dst.SetCanonical(header.S2b(header.Age), header.S2b("0"))
	}
	ctx.SetStatusCode(sf.StatusCode)

	isHEAD := bytes.Equal(ctx.Method(), []byte("HEAD"))

	// Determine cacheability before streaming.
	cacheable := h.isResponseCacheable(sf, ri, resMap)

	if sf.buffered || isHEAD || !cacheable {
		h.streamMissBuffered(ctx, sf, inflight, primaryKey, ri, resMap, isHEAD, cacheable)
		return
	}

	// Fall back to synchronous buffering when total streaming buffer
	// memory exceeds the cap. The streaming path (SetBodyStreamWriter)
	// defers buffer release to a post-handler callback, so under
	// slow-origin conditions all fetchSem slots can fill with live
	// buffers that GC cannot collect. The buffered path reads the body
	// synchronously and releases it before the handler returns,
	// preventing accumulation. See status-0-investigation.md.
	if h.maxStreamingBufferBytes > 0 && h.streamingBufferBytes.Load() >= h.maxStreamingBufferBytes {
		if h.StreamingFallbackInc != nil {
			h.StreamingFallbackInc.Inc()
		}
		h.streamMissBuffered(ctx, sf, inflight, primaryKey, ri, resMap, isHEAD, cacheable)
		return
	}

	h.streamMissTee(ctx, sf, inflight, primaryKey, ri, resMap)
}

// isResponseCacheable checks whether the origin response should be cached.
func (h *Handler) isResponseCacheable(sf *streamFetchResult, ri RequestInfo, resMap header.Map) bool {
	parsed := newParsedResponse(sf.StatusCode, ri.Header, resMap)
	cacheable := parsed.isCacheableWithDefault(h.negativeTTL, h.defaultTTL)
	if cacheable && !h.allowSetCookie && resMap.Get(header.SetCookie) != "" {
		return false
	}
	if cacheable && h.maxObjectSize > 0 {
		if cl := sf.resp.Header.ContentLength(); cl > 0 && int64(cl) > h.maxObjectSize {
			return false
		}
	}
	return cacheable
}

// streamMissBuffered handles the non-streaming fallback: the client
// doesn't support body streaming, the request is HEAD, or the response
// is not cacheable. The body is already in resp.Body().
func (h *Handler) streamMissBuffered(
	ctx *fasthttp.RequestCtx,
	sf *streamFetchResult,
	inflight *inflightStream,
	primaryKey api.Key,
	ri RequestInfo,
	resMap header.Map,
	isHEAD, cacheable bool,
) {
	// Check body size against maxResponseBytes (Content-Length may not
	// have been available).
	if h.maxResponseBytes > 0 && int64(len(sf.resp.Body())) > h.maxResponseBytes {
		inflight.res = fetchResult{Err: fmt.Errorf("upstream response exceeds %d bytes", h.maxResponseBytes)}
		close(inflight.done)
		ctx.Error("upstream error", fasthttp.StatusBadGateway)
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
		ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
		releaseStreamFetch(sf)
		return
	}

	bodyCopy := make([]byte, len(sf.resp.Body()))
	copy(bodyCopy, sf.resp.Body())

	res := fetchResult{
		StatusCode: sf.StatusCode,
		Header:     sf.Header,
		Body:       bodyCopy,
	}
	inflight.res = res
	close(inflight.done)

	if !isHEAD {
		ctx.Response.SetBodyRaw(res.Body)
	}

	if cacheable && !isHEAD {
		if h.maxObjectSize > 0 && int64(len(bodyCopy)) > h.maxObjectSize {
			releaseStreamFetch(sf)
			return
		}
		storeKey := primaryKey
		if vary := resMap.Get(header.Vary); vary != "" {
			storeKey = VariantKey(primaryKey, vary, ri.Header, h.policy)
			if storeKey != primaryKey {
				if !h.reserveVariantSlot(ctx, primaryKey, storeKey) {
					releaseStreamFetch(sf)
					return
				}
			}
		}
		obj := h.storeStreamedObject(ctx, storeKey, ri, res, resMap)
		if storeKey != primaryKey {
			primaryObj := obj.CloneForReturn(obj.Body)
			primaryObj.Key = primaryKey
			h.storeObject(ctx, primaryKey, primaryObj, ri, false, 0)
			h.forwardToOwnerIfRemote(ctx, primaryObj)
		}
	}
	releaseStreamFetch(sf)
}

// streamMissTee handles the streaming cacheable path: tee origin body
// to client + buffer for cache storage using SetBodyStreamWriter.
func (h *Handler) streamMissTee(
	ctx *fasthttp.RequestCtx,
	sf *streamFetchResult,
	inflight *inflightStream,
	primaryKey api.Key,
	ri RequestInfo,
	resMap header.Map,
) {
	storeKey := primaryKey
	if vary := resMap.Get(header.Vary); vary != "" {
		storeKey = VariantKey(primaryKey, vary, ri.Header, h.policy)
		if storeKey != primaryKey {
			if !h.reserveVariantSlot(ctx, primaryKey, storeKey) {
				h.streamMissNoCache(ctx, sf, inflight)
				return
			}
		}
	}

	finalStoreKey := storeKey
	bodyStream := sf.resp.BodyStream()
	buf := streamBufPool.Get().(*bytes.Buffer)
	buf.Reset()

	// Reserve streaming buffer memory. We account for the worst-case
	// buffer size (maxResponseBytes × 2 for bytes.Buffer over-allocation)
	// upfront and release it when the tee callback completes. This
	// prevents the total from growing unbounded under slow-origin
	// conditions where all fetchSem slots fill with live buffers.
	reserveBytes := h.maxResponseBytes * 2
	h.streamingBufferBytes.Add(reserveBytes)

	ctx.Response.SetBodyStreamWriter(func(w *bufio.Writer) {
		defer h.streamingBufferBytes.Add(-reserveBytes)
		h.teeStreamToClient(w, bodyStream, buf, sf, inflight, finalStoreKey, primaryKey, ri, resMap)
	})
}

// teeStreamToClient reads from the origin body stream, writes to both
// the client (w) and a buffer for cache storage. Called from inside
// SetBodyStreamWriter.
func (h *Handler) teeStreamToClient(
	w *bufio.Writer,
	bodyStream io.Reader,
	buf *bytes.Buffer,
	sf *streamFetchResult,
	inflight *inflightStream,
	storeKey, primaryKey api.Key,
	ri RequestInfo,
	resMap header.Map,
) {
	var totalBytes int64
	tee := io.TeeReader(bodyStream, buf)
	chunk := make([]byte, 32*1024)
	cacheExceeded := false
	for {
		n, readErr := tee.Read(chunk)
		if n > 0 {
			totalBytes += int64(n)
			if h.maxResponseBytes > 0 && totalBytes > h.maxResponseBytes {
				cacheExceeded = true
				_, _ = w.Write(chunk[:n])
				_, _ = io.Copy(w, bodyStream)
				break
			}
			_, wErr := w.Write(chunk[:n])
			if wErr != nil {
				break
			}
			_ = w.Flush()
		}
		if readErr != nil {
			break
		}
	}

	bodyCopy := make([]byte, buf.Len())
	copy(bodyCopy, buf.Bytes())

	inflight.res = fetchResult{
		StatusCode: sf.StatusCode,
		Header:     sf.Header,
		Body:       bodyCopy,
	}
	close(inflight.done)

	if !cacheExceeded && !(h.maxObjectSize > 0 && totalBytes > h.maxObjectSize) { //nolint:staticcheck // QF1001: readability
		res := inflight.res
		bgCtx := context.Background()
		obj := h.storeStreamedObject(bgCtx, storeKey, ri, res, resMap)
		if storeKey != primaryKey {
			primaryObj := obj.CloneForReturn(obj.Body)
			primaryObj.Key = primaryKey
			h.storeObject(bgCtx, primaryKey, primaryObj, ri, false, 0)
			h.forwardToOwnerIfRemote(bgCtx, primaryObj)
		}
	}

	if buf.Cap() <= maxStreamBufRetain {
		buf.Reset()
		streamBufPool.Put(buf)
	}

	releaseStreamFetch(sf)
}

// streamMissNoCache streams the origin response to the client without
// buffering for cache storage. Used when variant cap is exceeded.
func (h *Handler) streamMissNoCache(
	ctx *fasthttp.RequestCtx,
	sf *streamFetchResult,
	inflight *inflightStream,
) {
	bodyStream := sf.resp.BodyStream()
	ctx.Response.SetBodyStreamWriter(func(w *bufio.Writer) {
		_, writeErr := io.Copy(w, bodyStream)
		if writeErr != nil {
			h.logger.Debug("stream miss: body copy error (no cache)", "error", writeErr)
		}
		inflight.res = fetchResult{
			StatusCode: sf.StatusCode,
			Header:     sf.Header,
		}
		close(inflight.done)
		releaseStreamFetch(sf)
	})
}

// storeStreamedObject builds and stores a cache object from a fetchResult.
// Returns the built object so callers can clone it for primary key storage.
func (h *Handler) storeStreamedObject(
	ctx context.Context,
	key api.Key,
	ri RequestInfo,
	res fetchResult,
	resMap header.Map,
) *api.Object {
	obj := buildObject(key, ri, res, resMap, h.negativeTTL, h.defaultTTL, h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.policy, time.Now())
	h.storeObject(ctx, key, obj, ri, false, 0)
	h.forwardToOwnerIfRemote(ctx, obj)
	return obj
}
