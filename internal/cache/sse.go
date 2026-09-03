// sse.go — Server-Sent Events request handling. A request that announces
// SSE intent (Accept: text/event-stream, the WHATWG §9.2.2 client contract)
// is served as a live stream: never cached, never singleflight-collapsed,
// and never buffered, because an event stream is per-connection by design —
// two clients cannot share one origin stream.
//
// Responses that ARE text/event-stream but arrive without the request hint
// (non-hinted) are handled best-effort on the normal miss path: routed to
// the unbuffered streamMissNoCache path instead of the fully-buffered
// fallback. Those streams remain bounded by fetch_timeout because the
// origin connection's read deadline was armed before the response headers
// arrived (see ADR-0042); the Accept hint is the supported configuration.
package cache

import (
	"bufio"
	"bytes"
	"errors"

	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// handleSSE serves a request whose client announced SSE intent. It mirrors
// streamBypass (no caching, no storage) with two differences:
//
//   - The body copy flushes after every chunk, so each event is delivered
//     to the client immediately instead of accumulating in the 4 KiB
//     bufio pipe buffer.
//   - The fetch-semaphore slot is released once the response headers are
//     in hand: an SSE stream buffers nothing on this path, so holding one
//     of the (default 32) fetch slots for the stream's lifetime would let
//     a few dozen concurrent streams starve every other request on the
//     route.
//
// Cache-invalidation semantics for POST/PUT/DELETE are preserved: on a
// 2xx/3xx response the affected keys are purged as soon as the status is
// known (at header time — waiting for an endless body would delay
// invalidation indefinitely).
func (h *Handler) handleSSE(ctx *fasthttp.RequestCtx) {
	if h.fastClient == nil {
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("BYPASS"))
		ctx.Error("upstream error: no fast client configured", fasthttp.StatusBadGateway)
		return
	}

	sf, err := h.doFetchStream(ctx)
	if err != nil {
		if errors.Is(err, ErrFetchShed) {
			h.writeShed503(ctx, "BYPASS")
			return
		}
		ctx.Error("upstream error", fasthttp.StatusBadGateway)
		ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("BYPASS"))
		return
	}

	// Copy origin response headers, skipping bouine attribution headers
	// (same anti-spoofing rule as streamBypass).
	dst := &ctx.Response.Header
	for k, v := range sf.resp.Header.All() {
		if bytes.Equal(k, []byte(header.XCache)) || bytes.Equal(k, []byte(header.XCacheSource)) {
			continue
		}
		dst.AddBytesKV(k, v)
	}
	dst.SetCanonical(header.S2b(header.XCache), header.S2b("BYPASS"))
	ctx.SetStatusCode(sf.StatusCode)

	// Preserve POST/PUT/DELETE invalidation semantics: purge the affected
	// keys as soon as success is known from the status code, instead of
	// after the (possibly endless) body completes.
	if isInvalidatingBytes(ctx.Method()) && sf.StatusCode >= 200 && sf.StatusCode < 400 {
		h.purgeAfterSSEProxy(ctx, sf)
	}

	isHEAD := bytes.Equal(ctx.Method(), []byte("HEAD"))

	if isHEAD {
		releaseStreamFetch(sf)
		return
	}

	if sf.buffered {
		// Test clients and other non-streaming fetchers materialize the
		// body up front; serve it directly (finite by construction).
		ctx.Response.SetBodyRaw(takeResponseBody(sf.resp))
		releaseStreamFetch(sf)
		return
	}

	// The fetch is no longer a bounded "fetch" — it is a long-lived stream
	// that buffers nothing. Release the fetch slot before streaming so
	// concurrent SSE streams cannot exhaust the route's fetch concurrency.
	h.releaseFetchSlotEarly(sf)

	// Stream the origin body to the client, flushing after every chunk so
	// each event crosses the pipe as soon as it arrives.
	bodyStream := sf.resp.BodyStream()
	ctx.Response.SetBodyStreamWriter(func(w *bufio.Writer) {
		err := streamCopyFlush(w, bodyStream)
		if err != nil {
			h.logger.Debug("sse stream: body copy error", "error", err)
		}
		releaseStreamFetch(sf)
	})
}

// purgeAfterSSEProxy invalidates cached entries for an invalidating-method
// request served through the SSE stream path. It mirrors the key purge of
// invalidateAfterProxyFast (RFC 9111 §4.4 Location/Content-Location
// eviction) minus the POST-response storage, which never applies to
// hinted requests: an event stream cannot be stored, and a finite response
// on a client-announced stream URL has no stable representation to cache.
func (h *Handler) purgeAfterSSEProxy(ctx *fasthttp.RequestCtx, sf *streamFetchResult) {
	getRI := requestInfoFromCtx(ctx)
	getRI.Method = "GET"
	key := BuildKey(getRI, h.policy)
	_, _ = h.Purge(ctx, key)

	for _, hdr := range []string{header.ContentLocation, header.Location} {
		if loc := string(sf.resp.Header.Peek(hdr)); loc != "" {
			if locKey := h.buildLocationKey(ctx, loc); !locKey.IsZero() {
				_, _ = h.Purge(ctx, locKey)
			}
		}
	}
}
