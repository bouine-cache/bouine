package cache

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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
	resp       *fasthttp.Response // body stream still open (or buffered); nil on the upstream-fallback path
	req        *fasthttp.Request  // for release after stream
	sem        chan struct{}      // semaphore to release after stream
	span       tracing.Span       // ended by releaseStreamFetch, the funnel every exit path takes
	body       []byte             // upstream-fallback body; empty on the FastClient paths
	Header     headerLookup
	StatusCode int
	buffered   bool // true when resp.BodyStream() is nil (test clients)
	// Delay is the time from sending the origin request to receiving
	// the response headers (RFC 9111 §4.2.3 response_delay). It feeds
	// the corrected initial age of the served and stored response.
	// Zero on the upstream-fallback path (in-process handler).
	Delay time.Duration
}

// inflightStream tracks an in-progress streaming fetch so that
// concurrent requests for the same key (singleflight followers) can
// wait for the leader's body to be fully buffered and then serve
// the buffered result instead of issuing a duplicate origin fetch.
type inflightStream struct {
	done   chan struct{} // closed when the fetch finished or shed
	stored chan struct{} // closed when the fill settled in the store (ADR-0066)
	res    fetchResult   // set by leader before closing done; Err set on failure/shed
	// stored closes when the fill has settled in the store: after the
	// leader's store attempt on a cacheable fill, or at the leader's
	// verdict when nothing will be stored (fetch error, shed, SSE
	// unshareable, oversize, non-cacheable, incomplete body — client
	// disconnect or origin mid-body error). Fill-flight followers
	// (ADR-0066) wait on stored and then re-enter the request pipeline;
	// byte-handoff followers keep waiting on done only (unchanged).
	// Every path that closes done also closes stored, exactly once —
	// pinned by TestFetchAndStore_FillFollowerTerminatesForEachVerdict.
	// followers counts callers that loaded this stream from the
	// inflight table (i.e. parked as followers). Written only by
	// loadOrStore's follower branch (under the shard lock) before the
	// leader can observe it at publish time; read by the leader after
	// close-adjacent publication. atomic so the leader's read is
	// race-detector-clean without holding the shard lock.
	followers atomic.Int32
}

// newInflightStream returns a fresh, unpublished inflight stream. The
// leader paths in stream.go close both channels on every verdict.
func newInflightStream() *inflightStream {
	return &inflightStream{
		done:   make(chan struct{}),
		stored: make(chan struct{}),
	}
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
		if h.upstream != nil {
			// Upstream fallback (issue #598): a cached-static-route
			// handler is wired with Upstream (the staticfile handler)
			// and no FastClient. The upstream response is fully
			// buffered in a scratch RequestCtx, so the result carries
			// the buffered flag and every caller takes the buffered
			// branch; resp stays nil, which releaseStreamFetch and the
			// buffered-only call sites tolerate.
			res := h.fetchViaUpstream(ctx)
			if res.Err != nil {
				return nil, res.Err
			}
			return &streamFetchResult{
				StatusCode: res.StatusCode,
				Header:     res.Header,
				buffered:   true,
				body:       res.Body,
			}, nil
		}
		return nil, fmt.Errorf("no fast client configured")
	}
	// Span context stored by the middleware, never the RequestCtx. The
	// span rides the result and ends in releaseStreamFetch, which every
	// exit path funnels through.
	spanCtx, span := tracing.StartOriginSpan(
		tracing.SpanContextFromRequest(ctx),
		ctx.Method(), ctx.Path(), h.poolName, h.routeName,
	)

	if err := h.acquireFetchSlot(); err != nil {
		span.End()
		return nil, err
	}

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	resp.StreamBody = true

	req.Header.SetMethodBytes(ctx.Method())
	req.SetRequestURIBytes(h.originURI(ctx.RequestURI()))
	req.Header.SetHostBytes(ctx.Host())
	for k, v := range ctx.Request.Header.All() {
		req.Header.AddBytesKV(k, v)
	}
	// Normalize AE to the bucket token: the tee branch of this fetch
	// stores via buildObject, so the stored variant must match the
	// key's bucket claim (see rewriteOutboundAE). The SSE-only branch
	// never stores; the rewrite is harmless there.
	h.rewriteOutboundAE(&req.Header)
	// Client-identity headers (request.forwarded, issue #769) on the
	// outbound copy only — never the ctx, so the cache key, the Vary
	// variant key, and the stored RequestInfo stay untouched. Covers
	// miss, bypass, and SSE fetches on this path.
	h.applyForwardedCtx(&req.Header, ctx)
	// Forward the request body (POST-style SSE carries the prompt/payload
	// in the body). The pooled request outlives the handler on streaming
	// paths (released inside the body-stream writer), so the body is
	// copied into a request-owned slice instead of aliasing the ctx's
	// buffer, which the server reuses for the next request on the conn.
	// GET/HEAD requests carry no body and skip the copy.
	if body := ctx.Request.Body(); len(body) > 0 {
		bodyCopy := make([]byte, len(body))
		copy(bodyCopy, body)
		req.SetBodyRaw(bodyCopy)
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
	fetchStart := time.Now()
	if err := h.doFastFetch(req, resp); err != nil {
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
		<-h.fetchSem
		tracing.RecordError(span, err)
		span.End()
		return nil, fmt.Errorf("origin fetch: %w", err)
	}

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
		span:       span,
		buffered:   !resp.IsBodyStream(),
		Delay:      time.Since(fetchStart),
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
	if sf.sem != nil {
		<-sf.sem
	}
	// The span ends here for both modes: every caller funnels through
	// this release. Span.End is idempotent, so an earlier inline End on
	// an error path is safe; the nil guard covers results that never
	// started a span.
	if sf.span != nil {
		sf.span.End()
	}
}

// releaseFetchSlotEarly returns the fetch-semaphore slot while the origin
// connection stays open for a long-lived stream (SSE). A live stream
// buffers nothing after the headers, so it no longer needs the memory the
// slot bounds (concurrency × maxResponseBytes); holding it for the
// stream's lifetime would let a few dozen concurrent streams starve every
// other request on the route. releaseStreamFetch skips the already
// released slot via the nil guard.
func (h *Handler) releaseFetchSlotEarly(sf *streamFetchResult) {
	if sf.sem != nil {
		<-sf.sem
		sf.sem = nil
	}
}

// streamChunkSize is the copy granularity for unbuffered body streaming.
// SSE events are small (tens to hundreds of bytes), so each read returns
// whatever is available and is flushed immediately; the 32 KiB cap only
// bounds the per-stream scratch allocation.
const streamChunkSize = 32 * 1024

// streamCopyFlush copies r into w in chunks, flushing after every write so
// each event (or chunk) crosses the SetBodyStreamWriter pipe as soon as it
// is read. Without the per-write flush, small events accumulate in the
// 4 KiB bufio pipe buffer and the client sees them in multi-kilobyte
// batches — or only at stream end.
func streamCopyFlush(w *bufio.Writer, r io.Reader) error {
	buf := make([]byte, streamChunkSize)
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				return writeErr
			}
			if err := w.Flush(); err != nil {
				return err
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return readErr
		}
	}
}

// streamBypass fetches the origin response and streams it directly to
// the client without buffering. Used for BYPASS path where the response
// is not cached. A dispatched fetch (success or error) is attributed to
// the origin; the shed 503 (writeShed503) and the no-client-no-upstream
// 502 keep the empty source — the origin was never reached — which the
// metrics layer labels "bouine".
func (h *Handler) streamBypass(ctx *fasthttp.RequestCtx, xCacheHeader string) {
	if h.fastClient == nil {
		// Upstream fallback (issue #598): a cached-static-route handler
		// is wired with Upstream (the staticfile handler) and no
		// FastClient. Run the upstream handler in-process: its response
		// is already in ctx.Response, so only the attribution headers
		// and rewrites remain. The origin-bound URI is applied in place
		// first (see handleBypassFast).
		if h.upstream != nil {
			if u := h.originURI(ctx.RequestURI()); !bytes.Equal(u, ctx.RequestURI()) {
				ctx.Request.SetRequestURIBytes(u)
			}
			h.upstream(ctx)
			ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b(xCacheHeader))
			ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
			h.applyResponseRewrites(&ctx.Response.Header)
			return
		}
		// ctx.Error resets the response, so the attribution headers are
		// written after it. No X-Cache-Source: neither a client nor an
		// upstream is wired, so no fetch was dispatched — the metrics
		// layer labels this response "bouine", not "origin".
		ctx.Error("upstream error: no fast client configured", fasthttp.StatusBadGateway)
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b(xCacheHeader))
		h.applyResponseRewrites(&ctx.Response.Header)
		return
	}
	sf, err := h.doFetchStream(ctx)
	if err != nil {
		if errors.Is(err, ErrFetchShed) {
			h.writeShed503(ctx, xCacheHeader)
			return
		}
		writeUpstreamError(ctx, err)
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b(xCacheHeader))
		ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
		h.applyResponseRewrites(&ctx.Response.Header)
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
	dst.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
	ctx.SetStatusCode(sf.StatusCode)
	h.applyResponseRewrites(dst)

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

	// Stream the origin body directly to the client. Flush per chunk so
	// events (SSE) and small streamed bodies are delivered immediately
	// instead of batching in the 4 KiB bufio pipe buffer.
	bodyStream := sf.resp.BodyStream()
	ctx.Response.SetBodyStreamWriter(func(w *bufio.Writer) {
		if writeErr := streamCopyFlush(w, bodyStream); writeErr != nil {
			h.logger.Debug("stream bypass: body copy error",
				"upstream_pool", h.logUpstreamPool(), "route", h.routeName,
				"error", writeErr)
		}
		releaseStreamFetch(sf)
	})
}

// streamMiss fetches the origin response, streams it to the client while
// concurrently buffering for cache storage. Handles singleflight: the
// leader streams, followers wait for the buffered result.
//
//nolint:gocyclo // 16: SSE/unshareable + upstream-fallback cacheability branches mirror the FastClient paths; splitting them would hide the symmetry
func (h *Handler) streamMiss(
	ctx *fasthttp.RequestCtx,
	primaryKey api.Key,
	ri RequestInfo,
	inflight *inflightStream,
) {
	sf, err := h.doFetchStream(ctx)
	if err != nil {
		publishInflightErr(inflight, err)
		if errors.Is(err, ErrFetchShed) {
			h.writeShed503(ctx, "MISS")
			// The shed must not be a lost re-warm: schedule the bounded
			// background refill (the followers just resolved above get
			// their 503, the store still gets the object).
			h.triggerShedRefill(ri, primaryKey)
			return
		}
		writeUpstreamError(ctx, err)
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
		ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
		h.applyResponseRewrites(&ctx.Response.Header)
		return
	}

	dst := &ctx.Response.Header
	sf.Header.CopyToFastHTTP(dst)
	// Under sync the origin's CDC-family lines never reach a downstream
	// cache (ADR-0063 I3); the synthesized cap below is authoritative.
	if h.syncCDC {
		removeCDCFamily(dst)
	}
	dst.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
	dst.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
	// RFC 9111 §4.2.3 / §5.1: a cache generating a response computes Age
	// from the corrected initial age — the origin's Age plus the
	// response delay, floored by apparent age from Date skew. Overwrites
	// any origin Age copied above with the corrected value.
	setCorrectedAgeHeader(dst, sf.Header, sf.Delay)
	ctx.SetStatusCode(sf.StatusCode)
	h.applyResponseRewrites(dst)

	isHEAD := bytes.Equal(ctx.Method(), []byte("HEAD"))

	// SSE without the request hint (client did not announce Accept:
	// text/event-stream): stream it unbuffered rather than let the buffered
	// fallback read an endless body to EOF (which stalls the request until
	// fetch_timeout). Best-effort — the origin connection's read deadline
	// was armed before the headers arrived, so these streams remain bounded
	// by fetch_timeout; the supported configuration is the Accept hint
	// (ADR-0042).
	if !sf.buffered && !isHEAD && header.IsEventStreamContentType(sf.resp.Header.Peek(header.ContentType)) {
		// Nothing is buffered, so there is no result followers can share:
		// release them at header time with the unshareable sentinel so each
		// fetches its own response instead of waiting for this stream to
		// end and then receiving an empty body (see fetchAndStore).
		publishStreamUnshareable(inflight)
		h.streamMissNoCache(ctx, sf)
		return
	}

	// Determine cacheability before streaming — and before building the
	// header.Map. The pre-check reads the raw fasthttp response headers
	// (Peek + ParseCacheControlBytes, zero-alloc). Non-cacheable misses
	// (no-store, private, uncacheable status, oversized) never materialize
	// the Map at all: every non-cacheable miss saves the FromFastHTTP
	// conversion plus header-entry interning that only cacheable storage
	// needs.
	// The upstream-fallback result (issue #598) has no raw fasthttp
	// response: cacheability is evaluated over the buffered Map path.
	var cacheable bool
	var resMap header.Map
	if sf.resp != nil {
		cacheable = h.isResponseCacheableBytes(sf, ri)
		// Under sync the fill cap needs resMap even when the operator
		// storage gates (Set-Cookie, max object size) block storage: the
		// cap applies to every RFC-cacheable fill, stored or not
		// (ADR-0063 behavior matrix, Fill / MISS row).
		if cacheable || h.syncCDC {
			resMap = sf.Header.ToMap()
		}
	} else {
		resMap = sf.Header.ToMap()
		cacheable = h.isResponseCacheable(sf, ri, resMap)
	}

	// Downstream freshness sync (ADR-0063): the fill cap must be
	// complete at header-processing time — the streaming paths flush
	// the head before the body (and the concurrent store) run, so the
	// final TTL is computed here once (fillFinalTTL) and threaded to
	// the store path as a buildObject preset: the cap advertised
	// downstream and the stored clock are the same number.
	// Uncacheable-to-shared fills never synthesize (no bouine clock
	// exists; RFC 9111 §5.2.2.5, §5.2.2.7).
	var fillTTL *time.Duration
	if h.syncCDC {
		parsed := newParsedResponse(sf.StatusCode, ri.Header, resMap)
		if parsed.isCacheableWithDefault(h.neg, h.defaultTTL) {
			ttl := h.fillFinalTTLFor(resMap, sf.Delay, parsed, time.Now())
			var cdcBuf [88]byte
			dst.SetCanonical(header.S2b(header.CDNCacheControl),
				appendFillCDCDirectives(cdcBuf[:0], parsed.respCC, originEffectiveMaxAge(parsed.respCC, resMap), ttl))
			fillTTL = &ttl
		}
	}

	if sf.buffered || isHEAD || !cacheable {
		h.streamMissBuffered(ctx, sf, inflight, primaryKey, ri, resMap, isHEAD, cacheable, fillTTL)
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
		h.streamMissBuffered(ctx, sf, inflight, primaryKey, ri, resMap, isHEAD, cacheable, fillTTL)
		return
	}

	h.streamMissTee(ctx, sf, inflight, primaryKey, ri, resMap, fillTTL)
}

// isResponseCacheable checks whether the origin response should be cached.
func (h *Handler) isResponseCacheable(sf *streamFetchResult, ri RequestInfo, resMap header.Map) bool {
	parsed := newParsedResponse(sf.StatusCode, ri.Header, resMap)
	cacheable := parsed.isCacheableWithDefault(h.neg, h.defaultTTL)
	if cacheable && !h.allowSetCookie && clientStateInMap(resMap) {
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
// Map-based path exactly: CDN-Cache-Control precedence (RFC 9213), the
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

	// A 304 is only ever valid in reply to a conditional request (RFC
	// 9110 §15.4.5) and carries no body — never storable from a miss
	// fill; mirrors IsCacheable's guard.
	if status == fasthttp.StatusNotModified {
		return false
	}

	// CDN-Cache-Control overrides Cache-Control for shared caches
	// (RFC 9213). Reuse the Map path when present: merging multiple
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
// without must-understand, private, Vary:*, the client-state handler
// gate (Set-Cookie / Set-Login), request Authorization without
// shared-cache opt-in, and Pragma: no-cache
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
// client-state gate (Set-Cookie / Set-Login).
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
	// Client-state headers (Set-Cookie, Set-Login): the handler gate is
	// stricter than isBlockedByClientState — when allowSetCookie is
	// false (default), ANY response carrying them is not stored, even
	// with explicit freshness (matches the Map-based handler gate in
	// isResponseCacheable, which overrides the MAY-store reading in
	// isBlockedByClientState). ADR-0012 / ADR-0067.
	return !h.allowSetCookie && clientStateInFastHTTP(hdr)
}

// isBlockedByPragmaBytes mirrors isBlockedByPragma over raw header bytes:
// Pragma: no-cache blocks when there is no explicit freshness signal
// (skipped under CDN-CC per RFC 9213).
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
	// Validator-bearing no-cache responses are storable with zero
	// freshness — mirrors the Map path's branch in parsedResponse.
	// isCacheable (RFC 9111 §5.2.2.4 forbids reuse without validation,
	// §3 permits storing).
	if respCC.NoCache && len(hdr.Peek(header.ETag)) > 0 && isHeuristicStatus(status) {
		return true
	}
	if h.neg.Cacheable(status) {
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

// joinedVary returns the effective Vary header value from the stored
// header.Map: all Vary field lines joined with ", " per RFC 9110 §5.2.
// Vary is a list-based field, so a Vary split across multiple field
// lines is equivalent to one comma-joined value. The variant key and
// the refresh registry must see every field name — Get (first line
// only) silently dropped the later lines and collapsed distinct
// variants (e.g. "Vary: Accept-Encoding" + "Vary: X-Region" stored a
// key that ignored X-Region, serving one market's body to another).
// Single-line Vary returns the stored value directly.
func joinedVary(h header.Map) string {
	return h.GetAll(header.Vary)
}

// effectiveVary returns the Vary value the variant key is built from:
// the response's joined Vary field lines, unioned with the route's
// include_headers allow-list (cache.key.include_headers). An include
// header participates in the variant key exactly as if the origin had
// listed it in Vary — the union is never a replacement. The value is
// stored as VaryValue/VaryKey, so the hit path, the fast path, and
// every peer gate (which read the stored value) need no changes, and
// an include-listed header inherits the value normalization each
// variant-key construction site already applies to Vary fields.
//
// A nil policy or empty include list returns the joined value
// unchanged — the zero-allocation passthrough that keeps the
// include-free miss path on its alloc budget (Handler_CacheMiss_
// Cacheable). An absent response Vary with a non-empty include list
// yields the include list itself: the variant key then selects
// variants by those request headers, and a request header that is
// absent hashes as an empty value (one variant), matching RFC 9111
// Vary semantics.
//
// On the store paths it can never emit "*": config validation rejects
// "*" in include_headers, and responses carrying "Vary: *" are refused
// storage by isCacheBlocked, so their value never reaches the union.
// The 304-revalidation path is the one exception — a 304 runs before
// any cacheability gate, and MergeHeaders304 copies its Vary lines
// into the stored header wholesale, so a "Vary: *" 304 can produce a
// union containing "*". That is fail-safe downstream (every
// variant-key constructor returns the primary key on varyContainsStar,
// and refreshFrom304 blanks VaryKey for a "*" union), so the damage
// is failed hits until the object is re-fetched, never a wrong body.
func effectiveVary(h header.Map, policy *KeyPolicy) string {
	joined := joinedVary(h)
	if (policy == nil || len(policy.includeHeaders) == 0) && !policy.hasCookiePresence() {
		return joined
	}
	// Union into a lowercase, trimmed, deduplicated, sorted field list
	// joined with ", " per RFC 9110 §5.2. Sorting makes the result
	// deterministic regardless of the origin's field order, so
	// VaryValue/VaryKey stay byte-identical across the store sites and
	// across cluster nodes; dedup collapses a field the origin also
	// lists in Vary (NewKeyPolicy already dedupes the include side).
	// The synthetic cookie-presence field (issue #768) unions in the
	// same way: its hash contribution is computed by the variant-key
	// builders (presence bits, never raw cookie values).
	fields := make([]string, 0, strings.Count(joined, ",")+1+len(policy.includeHeaders)+1)
	if joined != "" {
		for f := range strings.SplitSeq(joined, ",") {
			fields = append(fields, strings.TrimSpace(strings.ToLower(f)))
		}
	}
	fields = append(fields, policy.includeHeaders...)
	if policy.hasCookiePresence() {
		fields = append(fields, cookiePresenceField)
	}
	sort.Strings(fields)
	uniq := fields[:0]
	for i, f := range fields {
		if i == 0 || f != fields[i-1] {
			uniq = append(uniq, f)
		}
	}
	return strings.Join(uniq, ", ")
}

// streamMissBuffered handles the non-streaming fallback: the client
// doesn't support body streaming, the request is HEAD, or the response
// is not cacheable. The body is already in resp.Body().
//
//nolint:gocyclo // 16: Vary-variant storage branches mirror the store path; splitting would hide the ownership rules
func (h *Handler) streamMissBuffered(
	ctx *fasthttp.RequestCtx,
	sf *streamFetchResult,
	inflight *inflightStream,
	primaryKey api.Key,
	ri RequestInfo,
	resMap header.Map,
	isHEAD, cacheable bool,
	fillTTL *time.Duration,
) {
	// Check body size against maxResponseBytes (Content-Length may not
	// have been available). The upstream-fallback result (issue #598)
	// has no raw response; its body size was already bounded by
	// fetchViaUpstream's conversion.
	if h.maxResponseBytes > 0 && sf.resp != nil && int64(len(sf.resp.Body())) > h.maxResponseBytes {
		inflight.res = fetchResult{Err: fmt.Errorf("upstream response exceeds %d bytes", h.maxResponseBytes)}
		close(inflight.done)
		close(inflight.stored)
		ctx.Error("upstream error", fasthttp.StatusBadGateway)
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("MISS"))
		ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(api.SourceOrigin)))
		h.applyResponseRewrites(&ctx.Response.Header)
		releaseStreamFetch(sf)
		return
	}

	// Body ownership: when the response will be stored, copy into an
	// exact-size slice so the hot tier does not pin fasthttp's doubling
	// slack for the object's lifetime. When it will never be stored
	// (non-cacheable miss), steal the buffer via SwapBody: the body is
	// transient (written to this client and released singleflight
	// followers) and the steal removes a full-body memcpy plus halves
	// peak in-flight body memory. The upstream-fallback result (issue
	// #598) has no raw response; its body is already an exact-size,
	// independently-owned copy produced by upstreamFetchResult, and it
	// travels on the streamFetchResult body field below.
	var body []byte
	switch {
	case sf.resp == nil:
		body = sf.body
	case cacheable:
		body = make([]byte, len(sf.resp.Body()))
		copy(body, sf.resp.Body())
	default:
		body = takeResponseBody(sf.resp)
	}

	// Owned header.Map: followers read res.Header after close(done),
	// concurrently with releaseStreamFetch returning the pooled response
	// to its sync.Pool (see teeStreamToClient for the same fix), and — on
	// the cacheable path — concurrently with buildObject mutating resMap
	// below (obj.Header = resMap: SetEntryRaw, Del, Set stamp the stored
	// object after close(done)). When at least one follower parked on
	// this stream, publish a detached clone and keep the mutable
	// original for storage — the same ownership split collapsedFetch
	// applies with ownedClone. With no followers there is exactly one
	// reader (this caller, before buildObject runs), so the Map is
	// reused as-is and the benchmark's miss-path alloc budget is
	// unchanged. Non-cacheable misses arrive with a zero Map (streamMiss
	// skipped the build), so build one here: 2 allocs, never-stored
	// path only, and the object-build mutations below never happen for
	// it.
	owned := resMap
	if owned.Len() == 0 {
		owned = sf.Header.ToMap()
	} else if inflight.followers.Load() > 0 {
		owned = owned.Clone()
	}
	res := fetchResult{
		StatusCode: sf.StatusCode,
		Header:     fromHeaderMap(owned),
		Body:       body,
		Delay:      sf.Delay,
	}
	publishFillVerdict(inflight, res, res.Header)
	// stored closes at function exit: after the store attempt below on
	// the cacheable path (covering the maxObjectSize early return and
	// the variant-slot-refusal return via this defer), and immediately
	// after the release on the non-cacheable path — nothing will be
	// stored, so the settlement verdict is already final.
	defer close(inflight.stored)

	if !isHEAD {
		ctx.Response.SetBodyRaw(res.Body)
	}

	if cacheable && !isHEAD {
		if h.maxObjectSize > 0 && int64(len(res.Body)) > h.maxObjectSize {
			releaseStreamFetch(sf)
			return
		}
		storeKey := primaryKey
		if vary := effectiveVary(resMap, h.policy); vary != "" {
			storeKey = VariantKey(primaryKey, vary, ri.Header, h.policy)
			if storeKey != primaryKey {
				if !h.reserveVariantSlot(ctx, primaryKey, storeKey) {
					releaseStreamFetch(sf)
					return
				}
			}
		}
		obj := h.storeStreamedObject(ctx, storeKey, ri, res, resMap, fillTTL)
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
	ri RequestInfo, resMap header.Map, fillTTL *time.Duration,
) {
	storeKey := primaryKey
	if vary := effectiveVary(resMap, h.policy); vary != "" {
		storeKey = VariantKey(primaryKey, vary, ri.Header, h.policy)
		if storeKey != primaryKey {
			if !h.reserveVariantSlot(ctx, primaryKey, storeKey) {
				// Variant overflow: the response streams unbuffered, so there is
				// no buffered result to share with singleflight followers —
				// release them at header time to fetch their own response.
				publishStreamUnshareable(inflight)
				h.streamMissNoCache(ctx, sf)
				return
			}
		}
	}

	finalStoreKey := storeKey
	bodyStream := sf.resp.BodyStream()
	buf := streamBufPool.Get().(*bytes.Buffer)
	buf.Reset()

	ctx.Response.SetBodyStreamWriter(func(w *bufio.Writer) {
		h.teeStreamToClient(w, bodyStream, buf, sf, inflight, finalStoreKey, primaryKey, ri, resMap, fillTTL)
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
	ri RequestInfo, resMap header.Map, fillTTL *time.Duration,
) {
	var totalBytes int64
	cacheExceeded := false
	// bodyComplete reports whether the origin body was read to a clean
	// EOF. A write error (the leader's client hung up) breaks the loop
	// mid-body and leaves it false; a non-EOF read error (the origin
	// connection broke mid-body) sets it false explicitly. Either way
	// the buffered copy is a prefix, not a response — see the publish
	// below.
	bodyComplete := false

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
				// The leader's client hung up mid-body: stop streaming
				// and buffering. The partial copy must never reach a
				// sharing surface — the publish below makes it unshareable.
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
			bodyComplete = errors.Is(readErr, io.EOF)
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
	res := fetchResult{
		StatusCode: sf.StatusCode,
		Header:     fromHeaderMap(sf.Header.ToMap()),
		Body:       bodyCopy,
		Delay:      sf.Delay,
	}
	h.settleTeeFill(inflight, res, sf, storeKey, primaryKey, ri, resMap, fillTTL, bodyComplete, cacheExceeded, totalBytes)

	if buf.Cap() <= maxStreamBufRetain {
		buf.Reset()
		streamBufPool.Put(buf)
	}

	releaseStreamFetch(sf)
}

// settleTeeFill publishes the tee fill's verdict to the flight and
// performs the store attempt on the shareable path. A body that did
// not complete cleanly — the leader's client hung up mid-stream, or
// the origin connection broke mid-body — leaves a buffered copy that
// is a prefix, or at best a full body the origin never confirmed:
// publishing it would hand a byte-handoff follower a truncated 200
// (the leader's headers claim Content-Length N, the body carries
// M < N bytes), and storing it would replay that truncation to every
// later request. The leader's own truncation is inherent to streaming
// — its head was already sent — but no follower inherits it and
// nothing is stored: the unshareable sentinel releases every follower
// to its own fetch (ADR-0042) and closes both settlement channels
// (ADR-0066). On the shareable path stored closes at settle exit:
// after the store attempt (skipped on cacheExceeded / maxObjectSize —
// the settlement verdict is then already final).
func (h *Handler) settleTeeFill(
	inflight *inflightStream,
	res fetchResult,
	sf *streamFetchResult,
	storeKey, primaryKey api.Key,
	ri RequestInfo,
	resMap header.Map,
	fillTTL *time.Duration,
	bodyComplete, cacheExceeded bool,
	totalBytes int64,
) {
	if !bodyComplete {
		publishStreamUnshareable(inflight)
		return
	}
	publishFillVerdict(inflight, res, sf.Header)
	defer close(inflight.stored)
	if cacheExceeded || (h.maxObjectSize > 0 && totalBytes > h.maxObjectSize) {
		return
	}
	bgCtx := context.Background()
	obj := h.storeStreamedObject(bgCtx, storeKey, ri, res, resMap, fillTTL)
	if storeKey != primaryKey {
		primaryObj := obj.CloneForReturn(obj.Body)
		primaryObj.Key = primaryKey
		h.storeObject(bgCtx, primaryKey, primaryObj, ri, false, 0)
		h.forwardToOwnerIfRemote(bgCtx, primaryObj)
	}
}

// publishStreamUnshareable resolves singleflight followers for a
// response that is streamed to the leader unbuffered (non-hinted SSE,
// Vary variant overflow) or whose body did not complete cleanly
// (teeStreamToClient: leader client disconnect, origin mid-body
// error): no complete body was ever buffered, so there is no result to
// hand over. Followers are released at header time — not stream end —
// and fetch their own response via fetchAndStore's ErrStreamUnshareable
// branch; a live stream is per-connection by
// design (ADR-0042). Nothing will be stored from such a stream, so
// closing stored here is the settlement verdict (ADR-0066).
func publishStreamUnshareable(inflight *inflightStream) {
	inflight.res = fetchResult{Err: ErrStreamUnshareable}
	close(inflight.done)
	close(inflight.stored)
}

// publishFillVerdict publishes a completed buffered/tee fill's verdict
// to the flight's followers (ADR-0066/ADR-0067). A response carrying
// client-state headers (Set-Cookie / Set-Login) is minted for THIS
// caller's session and its bytes are never handed to another caller:
// the flight publishes the unshareable sentinel, byte-handoff followers
// are released at header time and fetch their own
// (fetchAndStore's ErrStreamUnshareable branch). Only the published
// verdict changes, never the settlement lifecycle — the CALLER closes
// stored after its store attempt, so fill followers re-enter the
// pipeline after the fill settled: under the default client-state gate
// nothing is stored and the follower fetches its own, and under
// allow_set_cookie it serves the stripped stored copy. The leader
// always keeps serving its own response and stores the stripped object
// when the gates allow.
func publishFillVerdict(inflight *inflightStream, res fetchResult, hl headerLookup) {
	if carriesClientState(hl) {
		inflight.res = fetchResult{Err: ErrStreamUnshareable}
	} else {
		inflight.res = res
	}
	close(inflight.done)
}

// publishInflightErr resolves the leader's inflight stream with a
// fetch verdict: the error is published and both settlement channels
// close (done for byte-handoff followers, stored for fill-flight
// followers, ADR-0066). Nothing will be stored after an error
// verdict, so closing stored here is the settlement.
func publishInflightErr(inflight *inflightStream, err error) {
	inflight.res = fetchResult{Err: err}
	close(inflight.done)
	close(inflight.stored)
}

// streamMissNoCache streams the origin response to the client without
// buffering for cache storage. Used when variant cap is exceeded and for
// uncacheable-by-nature streams (SSE). The body copy flushes per chunk so
// events reach the client as they arrive. Nothing is buffered, so callers
// must resolve singleflight followers themselves (publishStreamUnshareable)
// before calling — there is no shareable result to publish.
func (h *Handler) streamMissNoCache(ctx *fasthttp.RequestCtx, sf *streamFetchResult) {
	bodyStream := sf.resp.BodyStream()
	ctx.Response.SetBodyStreamWriter(func(w *bufio.Writer) {
		if writeErr := streamCopyFlush(w, bodyStream); writeErr != nil {
			h.logger.Debug("stream miss: body copy error (no cache)",
				"upstream_pool", h.logUpstreamPool(), "route", h.routeName,
				"error", writeErr)
		}
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
	fillTTL *time.Duration,
) *api.Object {
	// fillTTL: preset by the sync-mode fill cap at header-processing
	// time (ADR-0063) so the stored clock equals the advertised cap;
	// nil (passthrough) computes the TTL in buildObject as before.
	obj := buildObject(key, ri, res, resMap, h.neg, h.defaultTTL, h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.policy, h.stayinAlive, h.poolName, fillTTL, time.Now())
	h.storeObject(ctx, key, obj, ri, false, 0)
	h.forwardToOwnerIfRemote(ctx, obj)
	return obj
}
