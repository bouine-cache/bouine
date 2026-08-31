package cache

import (
	"bufio"
	"bytes"
	"context"
	"errors"
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
	resp       *fasthttp.Response // body stream still open (or buffered)
	req        *fasthttp.Request  // for release after stream
	sem        chan struct{}      // semaphore to release after stream
	Header     headerLookup
	StatusCode int
	buffered   bool // true when resp.BodyStream() is nil (test clients)
}

// inflightStream tracks an in-progress streaming fetch so that
// concurrent requests for the same key (singleflight followers) can
// wait for the leader's body to be fully buffered and then serve
// the buffered result instead of issuing a duplicate origin fetch.
type inflightStream struct {
	done chan struct{} // closed when the fetch finished or shed
	res  fetchResult   // set by leader before closing done; Err set on failure/shed
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
	spanCtx, span := tracing.StartSpan(context.Background(), "bouine.origin")

	if err := h.acquireFetchSlot(); err != nil {
		span.End()
		return nil, err
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
	tracing.InjectFastHTTP(spanCtx, req)

	// Deadline-based fetch via doFastFetch (FastClient.DoDeadline): no
	// context.WithTimeout, no timer goroutine, and the deadline is
	// actually enforced on production transports via fasthttp's
	// kernel-level connection deadlines. The previous context.WithCancel
	// + time.AfterFunc never reached production transports (WithCancel
	// has no deadline, so transport.Client.Do fell back to a fixed 60s
	// DoTimeout) — fetch_timeout was silently ignored in production.
	// The conn read deadline persists into the body stream, bounding
	// the total fetch (header + body) per the fetch_timeout contract.
	if err := h.doFastFetch(req, resp); err != nil {
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
		if errors.Is(err, ErrFetchShed) {
			h.writeShed503(ctx, xCacheHeader)
			return
		}
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
		// Client doesn't support streaming — take ownership of the body
		// buffer instead of copying it into the ctx's own buffer. BYPASS
		// bodies are never stored, so the pool-slack concern doesn't
		// apply, and the release path drops the response's copy
		// immediately (peak in-flight body memory is halved).
		ctx.Response.SetBodyRaw(takeResponseBody(sf.resp))
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
		inflight.res = fetchResult{Err: err}
		close(inflight.done)
		if errors.Is(err, ErrFetchShed) {
			h.writeShed503(ctx, "MISS")
			return
		}
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
	if len(sf.resp.Header.Peek(header.Age)) == 0 {
		dst.SetCanonical(header.S2b(header.Age), header.S2b("0"))
	}
	ctx.SetStatusCode(sf.StatusCode)

	isHEAD := bytes.Equal(ctx.Method(), []byte("HEAD"))

	// Determine cacheability before streaming — and before building the
	// header.Map. The pre-check reads the raw fasthttp response headers
	// (Peek + ParseCacheControlBytes, zero-alloc). Non-cacheable misses
	// (no-store, private, uncacheable status, oversized) never materialize
	// the Map at all: every non-cacheable miss saves the FromFastHTTP
	// conversion plus header-entry interning that only cacheable storage
	// needs.
	cacheable := h.isResponseCacheableBytes(sf, ri)
	var resMap header.Map
	if cacheable {
		resMap = sf.Header.ToMap()
	}

	if sf.buffered || isHEAD || !cacheable {
		h.streamMissBuffered(ctx, sf, inflight, primaryKey, ri, resMap, isHEAD, cacheable)
		return
	}

	// Fall back to synchronous buffering when total streaming buffer
	// memory already exceeds the cap. This catches staggered arrivals
	// (new misses after earlier streams have accumulated buffer bytes).
	// Under a simultaneous burst (the slow-origin OOMKill scenario), all
	// 32 slots pass this check because no buffer has started growing yet;
	// the in-loop check inside teeStreamToClient is the primary defense
	// in that case. The buffered path reads the body synchronously and
	// releases it before the handler returns, preventing accumulation.
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

// isResponseCacheableBytes is isResponseCacheable over the raw fasthttp
// response headers, without building a header.Map first. It mirrors the
// Map-based path exactly: CDN-Cache-Control precedence (RFC 9211), the
// blocking directives (no-store, private, Vary:*, Pragma, Set-Cookie,
// request Authorization), explicit freshness (max-age/s-maxage/Expires),
// heuristic freshness (Last-Modified on heuristically-cacheable status),
// negative caching, and the operator default-TTL fallback. Every check
// reads via Peek/ParseCacheControlBytes — zero allocations.
//
// The one deliberate difference from the Map-based path: request-side
// headers (Authorization) are checked via ri.Header, exactly as before —
// ri.Header is already materialized on the miss path.
func (h *Handler) isResponseCacheableBytes(sf *streamFetchResult, ri RequestInfo) bool {
	hdr := &sf.resp.Header
	status := sf.StatusCode

	// CDN-Cache-Control overrides Cache-Control for shared caches
	// (RFC 9211). Reuse the Map path when present: merging multiple
	// values and validating token characters needs the joined value,
	// and CDN-CC is rare enough that the Map build is acceptable.
	if cdn := hdr.Peek(header.CDNCacheControl); len(cdn) > 0 {
		return h.isResponseCacheable(sf, ri, sf.Header.ToMap())
	}

	// Multiple Cache-Control field lines combine per RFC 9110 §5.2;
	// the joined value lives in a stack buffer when it fits.
	var ccBuf [512]byte
	ccVal := joinedCacheControlBytes(hdr, &ccBuf)
	respCC := ParseCacheControlBytes(ccVal)
	if h.isBytesBlocked(hdr, respCC, ri, status) {
		return false
	}
	if !h.hasBytesFreshness(hdr, respCC, status) {
		return false
	}
	// Set-Cookie with explicit freshness is cacheable per the Map path's
	// handler gate only when allowSetCookie is set — handled above.
	if h.maxObjectSize > 0 {
		if cl := hdr.ContentLength(); cl > 0 && int64(cl) > h.maxObjectSize {
			return false
		}
	}
	return true
}

// isBytesBlocked mirrors isCacheBlocked over raw header bytes: no-store
// without must-understand, private, Vary:*, the Set-Cookie handler gate,
// request Authorization without shared-cache opt-in, and Pragma: no-cache
// without explicit freshness.
func (h *Handler) isBytesBlocked(hdr *fasthttp.ResponseHeader, respCC Directives, ri RequestInfo, status int) bool {
	if h.isBlockedByDirectives(hdr, respCC, status) {
		return true
	}
	// Request Authorization without explicit shared-cache opt-in
	// (mirrors isCacheBlocked's request-side check).
	if ri.Header.Get(header.Authorization) != "" {
		if !respCC.Public && !respCC.MustRevalidate && !respCC.SMaxAgeSet {
			return true
		}
	}
	return isBlockedByPragmaBytes(hdr, respCC)
}

// isBlockedByDirectives covers the response-side blocking directives:
// no-store without must-understand, private, Vary:*, and the handler's
// Set-Cookie gate.
func (h *Handler) isBlockedByDirectives(hdr *fasthttp.ResponseHeader, respCC Directives, status int) bool {
	if respCC.NoStore {
		if !respCC.MustUnderstand || !isUnderstoodStatus(status) {
			return true
		}
	}
	if respCC.Private {
		return true
	}
	// Vary: * — "always fails to match" (RFC 9111 §4.1); bouine refuses
	// to store such responses. Multiple Vary field lines combine per
	// RFC 9110 §5.2, so a "*" on any line must be visible.
	var varyBuf [512]byte
	if varyContainsStarBytes(joinedVaryBytes(hdr, &varyBuf)) {
		return true
	}
	// Set-Cookie: the handler gate is stricter than isBlockedBySetCookie —
	// when allowSetCookie is false (default), ANY Set-Cookie response is
	// not stored, even with explicit freshness (matches the Map-based
	// handler gate in isResponseCacheable, which overrides the MAY-store
	// reading in isBlockedBySetCookie).
	return !h.allowSetCookie && len(hdr.Peek(header.SetCookie)) > 0
}

// isBlockedByPragmaBytes mirrors isBlockedByPragma over raw header bytes:
// Pragma: no-cache blocks when there is no explicit freshness signal
// (skipped under CDN-CC per RFC 9211).
func isBlockedByPragmaBytes(hdr *fasthttp.ResponseHeader, respCC Directives) bool {
	if !bytes.Equal(hdr.Peek(header.Pragma), []byte("no-cache")) {
		return false
	}
	return !respCC.MaxAgeSet && !respCC.SMaxAgeSet &&
		len(hdr.Peek(header.Expires)) == 0 &&
		len(hdr.Peek(header.LastModified)) == 0
}

// hasBytesFreshness mirrors the freshness half of
// parsedResponse.isCacheableWithDefault over raw header bytes: explicit
// freshness (max-age/s-maxage/valid Expires), heuristic freshness
// (Last-Modified on heuristically-cacheable status or CC public), negative
// caching, and the operator default-TTL fallback for heuristically-cacheable
// statuses.
func (h *Handler) hasBytesFreshness(hdr *fasthttp.ResponseHeader, respCC Directives, status int) bool {
	if respCC.MaxAgeSet || respCC.SMaxAgeSet {
		return true
	}
	if exp := hdr.Peek(header.Expires); len(exp) > 0 && !parseHTTPDate(string(exp)).IsZero() {
		return true
	}
	// Heuristic freshness: Last-Modified AND heuristically-cacheable
	// status (or CC public) — RFC 9111 §4.2.2.
	if len(hdr.Peek(header.LastModified)) > 0 &&
		(isHeuristicStatus(status) || respCC.Public) {
		return true
	}
	if h.negativeTTL > 0 && IsNegativeCacheable(status) {
		return true
	}
	// Operator default-TTL fallback: only heuristically-cacheable
	// statuses qualify (mirrors isCacheableWithDefault).
	return h.defaultTTL > 0 && isHeuristicStatus(status)
}

// joinedCacheControlBytes returns the effective Cache-Control value from
// a raw response header: all Cache-Control field lines joined with ", "
// per RFC 9110 §5.2 (multiple field lines are equivalent to a single
// comma-separated list). The common single-line case returns fasthttp's
// zero-copy Peek; multi-line headers append into the caller's buffer —
// stack-backed while the joined value fits 512 bytes, growing to the
// heap past that (rare; headers are capped at 8 KiB per line).
func joinedCacheControlBytes(hdr *fasthttp.ResponseHeader, buf *[512]byte) []byte {
	all := hdr.PeekAll(header.CacheControl)
	switch len(all) {
	case 0:
		return nil
	case 1:
		return all[0]
	}
	// RFC 9110 §5.2: multiple field lines combine into one list. The
	// first line may be empty (e.g. "Cache-Control:" followed by a
	// second line); it still contributes a (empty) list element.
	joined := buf[:0]
	joined = append(joined, all[0]...)
	for _, v := range all[1:] {
		joined = append(joined, ',', ' ')
		joined = append(joined, v...)
	}
	return joined
}

// joinedVaryBytes returns the effective Vary value from a raw response
// header: all Vary field lines joined with ", " per RFC 9110 §5.2, so a
// "Vary: *" on any line is visible to varyContainsStarBytes. Single-line
// Vary (the common case) is the zero-copy Peek; multi-line joins append
// into the caller's buffer, spilling to the heap past 512 bytes.
func joinedVaryBytes(hdr *fasthttp.ResponseHeader, buf *[512]byte) []byte {
	all := hdr.PeekAll(header.Vary)
	switch len(all) {
	case 0:
		return nil
	case 1:
		return all[0]
	}
	// RFC 9110 §5.2 join; the first line may be empty ("Vary:" then a
	// second line carrying the real tokens) and must not short-circuit
	// the join.
	joined := buf[:0]
	joined = append(joined, all[0]...)
	for _, v := range all[1:] {
		joined = append(joined, ',', ' ')
		joined = append(joined, v...)
	}
	return joined
}

// varyContainsStarBytes reports whether a raw Vary header value contains
// the "*" token (RFC 9111 §4.1). Byte-level mirror of varyContainsStar:
// split on commas, trim whitespace, compare to "*".
func varyContainsStarBytes(vary []byte) bool {
	for len(vary) > 0 {
		// Find the end of this comma-separated field.
		end := 0
		for end < len(vary) && vary[end] != ',' {
			end++
		}
		token := vary[:end]
		// Trim leading and trailing OWS.
		for len(token) > 0 && (token[0] == ' ' || token[0] == '\t') {
			token = token[1:]
		}
		for len(token) > 0 && (token[len(token)-1] == ' ' || token[len(token)-1] == '\t') {
			token = token[:len(token)-1]
		}
		if len(token) == 1 && token[0] == '*' {
			return true
		}
		if end < len(vary) {
			end++ // skip the comma
		}
		vary = vary[end:]
	}
	return false
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

	// Body ownership: when the response will be stored, copy into an
	// exact-size slice so the hot tier does not pin fasthttp's doubling
	// slack for the object's lifetime. When it will never be stored
	// (non-cacheable miss), steal the buffer via SwapBody: the body is
	// transient (written to this client and released singleflight
	// followers) and the steal removes a full-body memcpy plus halves
	// peak in-flight body memory.
	var body []byte
	if cacheable {
		body = make([]byte, len(sf.resp.Body()))
		copy(body, sf.resp.Body())
	} else {
		body = takeResponseBody(sf.resp)
	}

	// Owned header.Map: followers read res.Header after close(done),
	// concurrently with releaseStreamFetch returning the pooled response
	// to its sync.Pool (see teeStreamToClient for the same fix). On the
	// cacheable path resMap is already an owned Map built by streamMiss
	// (ToMap detaches from the pooled response) — reuse it instead of a
	// second FromFastHTTP conversion. Non-cacheable misses arrive with a
	// zero Map (streamMiss skipped the build), so build one here: it is
	// only 2 allocs and only on the never-stored path, but followers
	// still read it after close(done) and must not touch the pooled
	// response.
	owned := resMap
	if owned.Len() == 0 {
		owned = sf.Header.ToMap()
	}
	res := fetchResult{
		StatusCode: sf.StatusCode,
		Header:     fromHeaderMap(owned),
		Body:       body,
	}
	inflight.res = res
	close(inflight.done)

	if !isHEAD {
		ctx.Response.SetBodyRaw(res.Body)
	}

	if cacheable && !isHEAD {
		if h.maxObjectSize > 0 && int64(len(res.Body)) > h.maxObjectSize {
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

// takeResponseBody transfers ownership of resp's body into an
// independently-owned []byte and leaves resp empty, so the pooled
// response can be released immediately without invalidating the
// returned slice. The returned slice must be treated as immutable:
// it may be shared by the client response and singleflight
// followers, which only read it. It must NOT be stored in the
// cache — SwapBody returns fasthttp's pooled buffer with its
// doubling-growth slack, which the hot tier would pin for the
// object's lifetime (see TestFetchStoresRightSizedBody).
//
// fasthttp's client read path represents the body as either a pooled
// bytebufferpool buffer (buffered fetches) or a stream (StreamBody).
// SwapBody returns the former directly and drains the latter into a
// fresh buffer first — either way the body is handed over with no
// copy by the caller. The response returns to fasthttp's pool with an
// empty body, halving peak in-flight body memory for transient
// bodies and removing the full-body memcpy the defensive copy did.
//
// Steady-state allocation count is unchanged: the buffer fasthttp
// would have reused is taken, so the next read through that pool slot
// allocates a fresh one. What disappears is one full-body memcpy per
// transient fetch.
//
// Precondition: resp's body was produced by fasthttp's client read
// path. SwapBody discards a body set via SetBodyRaw; no FastClient in
// this codebase returns SetBodyRaw responses.
func takeResponseBody(resp *fasthttp.Response) []byte {
	if resp == nil {
		return nil
	}
	return resp.SwapBody(nil)
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

	ctx.Response.SetBodyStreamWriter(func(w *bufio.Writer) {
		h.teeStreamToClient(w, bodyStream, buf, sf, inflight, finalStoreKey, primaryKey, ri, resMap)
	})
}

// teeStreamToClient reads from the origin body stream, writes to both
// the client (w) and a buffer for cache storage. Called from inside
// SetBodyStreamWriter.
//
// Memory safety: the global streamingBufferBytes counter is updated
// incrementally as the tee buffer grows, and a closure-based defer
// subtracts the final value on exit. An in-loop check against
// maxStreamingBufferBytes stops buffering (but continues streaming to
// the client) once the global cap is exceeded, preventing all 32
// fetchSem slots from accumulating full buffers simultaneously under
// slow-origin conditions. Note that bytes.Buffer grows by doubling, so
// actual allocated memory can be up to 2x the tracked Len(); the global
// cap is set at 50% of the theoretical worst case to account for this.
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
	cacheExceeded := false
	clientDisconnected := false

	bufCap := h.maxResponseBytes
	if bufCap <= 0 {
		bufCap = defaultMaxResponseBytes
	}

	lastReported := 0
	defer func() {
		h.streamingBufferBytes.Add(-int64(lastReported))
	}()

	chunk := make([]byte, 32*1024)
	for {
		n, readErr := bodyStream.Read(chunk)
		if n > 0 {
			totalBytes += int64(n)

			_, wErr := w.Write(chunk[:n])
			if wErr != nil {
				clientDisconnected = true
				break
			}
			_ = w.Flush()

			if !cacheExceeded {
				cacheExceeded = h.shouldStopBuffering(buf, int64(n), totalBytes, bufCap)
				if !cacheExceeded {
					buf.Write(chunk[:n])
					if buf.Len() > lastReported {
						h.streamingBufferBytes.Add(int64(buf.Len() - lastReported))
						lastReported = buf.Len()
					}
				}
			}
		}
		if readErr != nil {
			break
		}
	}

	// Copy the tee buffer into an exact-size slice. Stealing the buffer's
	// backing array instead (buf.Bytes()) would persist the doubling-growth
	// slack in the stored cache object — the hot tier pins that slack for
	// the object's lifetime. The copy keeps stored bodies right-sized.
	bodyCopy := make([]byte, buf.Len())
	copy(bodyCopy, buf.Bytes())

	// Followers read inflight.res.Header after close(inflight.done),
	// concurrently with our releaseStreamFetch(sf) below, which returns
	// the pooled *fasthttp.Response to its sync.Pool (where another
	// goroutine may Reset it). Publishing sf.Header — a live pointer
	// into that pooled response — raced with the pool release. Convert
	// to an owned header.Map before publishing so followers never touch
	// the pooled object. The conversion cost is miss-path only.
	inflight.res = fetchResult{
		StatusCode: sf.StatusCode,
		Header:     fromHeaderMap(sf.Header.ToMap()),
		Body:       bodyCopy,
	}
	close(inflight.done)

	if !cacheExceeded && !clientDisconnected && !(h.maxObjectSize > 0 && totalBytes > h.maxObjectSize) { //nolint:staticcheck // QF1001: readability
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
		// Owned header.Map — releaseStreamFetch below returns the pooled
		// response to its sync.Pool while followers may still read the
		// published result (see teeStreamToClient for the same fix).
		inflight.res = fetchResult{
			StatusCode: sf.StatusCode,
			Header:     fromHeaderMap(sf.Header.ToMap()),
		}
		close(inflight.done)
		releaseStreamFetch(sf)
	})
}

// shouldStopBuffering checks per-stream, total-body, and global caps
// before buffering the next chunk. Returns true if buffering should
// stop (the response will still be streamed to the client, just not
// cached). Increments the fallback counter when the global cap triggers.
func (h *Handler) shouldStopBuffering(buf *bytes.Buffer, chunkLen int64, totalBytes, bufCap int64) bool {
	switch {
	case int64(buf.Len())+chunkLen > bufCap:
		return true
	case h.maxResponseBytes > 0 && totalBytes > h.maxResponseBytes:
		return true
	case h.maxStreamingBufferBytes > 0 && h.streamingBufferBytes.Load() >= h.maxStreamingBufferBytes:
		if h.StreamingFallbackInc != nil {
			h.StreamingFallbackInc.Inc()
		}
		return true
	}
	return false
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
