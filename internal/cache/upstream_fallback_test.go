package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/pkg/header"
)

// TestHandler_UpstreamFallbackWhenFastClientNil pins issue #598: a cache
// handler built with only Upstream (the cached-static-route wiring — the
// staticfile handler is the origin) must serve a cold MISS and a no-cache
// BYPASS through the fallback fetch instead of 502 "no fast client
// configured".
func TestHandler_UpstreamFallbackWhenFastClientNil(t *testing.T) {
	t.Parallel()
	var calls int
	upstream := func(ctx *fasthttp.RequestCtx) {
		calls++
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("static-body"))
	}
	store := newTestStore()
	h := NewHandler(HandlerConfig{
		Upstream: upstream,
		Store:    store,
	})

	rr1 := testCtx("GET", "http://example.com/file")
	h.ServeRequest(rr1)
	require.Equal(t, 200, respCode(rr1))
	require.Equal(t, "MISS", respHeader(rr1, header.XCache))
	require.Equal(t, "static-body", respBody(rr1))
	require.Equal(t, 1, calls, "miss must reach the upstream handler")

	// Warm request: served from cache without touching the upstream.
	rr2 := testCtx("GET", "http://example.com/file")
	h.ServeRequest(rr2)
	require.Equal(t, 200, respCode(rr2))
	require.Equal(t, "HIT", respHeader(rr2, header.XCache))
	require.Equal(t, "static-body", respBody(rr2))
	require.Equal(t, 1, calls)

	// no-cache request: a bare no-cache without validators dispatches as
	// a Miss (see evalNoCache), which re-contacts the origin through the
	// upstream handler; a validator-less stored object means X-Cache is
	// MISS, not BYPASS.
	rr3 := testCtx("GET", "http://example.com/file")
	rr3.Request.Header.Set(header.CacheControl, "no-cache")
	h.ServeRequest(rr3)
	require.Equal(t, 200, respCode(rr3))
	require.Equal(t, "MISS", respHeader(rr3, header.XCache))
	require.Equal(t, "static-body", respBody(rr3))
	require.Equal(t, 2, calls)

	// no-store request: the BYPASS path re-contacts the origin through
	// the upstream handler.
	rr4 := testCtx("GET", "http://example.com/file")
	rr4.Request.Header.Set(header.CacheControl, "no-store")
	h.ServeRequest(rr4)
	require.Equal(t, 200, respCode(rr4))
	require.Equal(t, "BYPASS", respHeader(rr4, header.XCache))
	require.Equal(t, "static-body", respBody(rr4))
	require.Equal(t, 3, calls)
}

// TestHandler_UpstreamFallbackRevalidate covers the revalidation path
// through the upstream fallback: a stale object with an ETag must be
// refreshed via a conditional request handled by the upstream.
func TestHandler_UpstreamFallbackRevalidate(t *testing.T) {
	t.Parallel()
	upstream := func(ctx *fasthttp.RequestCtx) {
		if string(ctx.Request.Header.Peek(header.IfNoneMatch)) == `"v1"` {
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			ctx.SetStatusCode(304)
			return
		}
		ctx.Response.Header.Set(header.CacheControl, "max-age=1")
		ctx.Response.Header.Set(header.ETag, `"v1"`)
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("static-body"))
	}
	store := newTestStore()
	h := NewHandler(HandlerConfig{
		Upstream: upstream,
		Store:    store,
	})

	rr1 := testCtx("GET", "http://example.com/file")
	h.ServeRequest(rr1)
	require.Equal(t, "MISS", respHeader(rr1, header.XCache))

	time.Sleep(1100 * time.Millisecond)

	rr2 := testCtx("GET", "http://example.com/file")
	h.ServeRequest(rr2)
	require.Equal(t, "REVALIDATED", respHeader(rr2, header.XCache))
	require.Equal(t, "static-body", respBody(rr2))
}

// TestHandler_UpstreamFallbackNoUpstream502 keeps the 502 contract for a
// handler with neither FastClient nor Upstream (misconfiguration): the
// client must still see the Bad Gateway, not a panic.
func TestHandler_UpstreamFallbackNoUpstream502(t *testing.T) {
	t.Parallel()
	store := newTestStore()
	h := NewHandler(HandlerConfig{Store: store})

	rr := testCtx("GET", "http://example.com/file")
	h.ServeRequest(rr)
	require.Equal(t, fasthttp.StatusBadGateway, respCode(rr))
	require.Equal(t, "MISS", respHeader(rr, header.XCache))

	rr2 := testCtx("GET", "http://example.com/file")
	rr2.Request.Header.Set(header.CacheControl, "no-store")
	h.ServeRequest(rr2)
	require.Equal(t, fasthttp.StatusBadGateway, respCode(rr2))
	require.Equal(t, "BYPASS", respHeader(rr2, header.XCache))
}
