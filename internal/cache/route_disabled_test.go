package cache

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

func testStore(t *testing.T) *storage.HotStore {
	t.Helper()
	return storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
}

// testDisabledHandler builds a Disabled handler (ADR-0074) sharing the caller's store.
func testDisabledHandler(t *testing.T, store *storage.HotStore, upstream fasthttp.RequestHandler) *Handler {
	t.Helper()
	return NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store:      store,
		Disabled:   true,
	})
}

// A disabled route never serves from or writes the store, even with a
// stored entry under the key (filled by an enabled sibling sharing it).
func TestRouteDisabled_NeverStoresNeverServes(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	var originCalls atomic.Int32
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60, public")
		ctx.SetStatusCode(200)
		_, _ = fmt.Fprintf(ctx, "origin-%d", originCalls.Add(1))
	}

	// Pre-populate the store via an enabled handler sharing it.
	enabled := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store:      store,
	})
	fill := testCtx("GET", "/user-data")
	enabled.ServeRequest(fill)
	require.Equal(t, "MISS", respHeader(fill, header.XCache))
	require.Equal(t, int64(1), store.Stats().HotEntries, "precondition: the enabled handler stores the entry")

	disabled := testDisabledHandler(t, store, upstream)
	for range 5 {
		ctx := testCtx("GET", "/user-data")
		disabled.ServeRequest(ctx)
		require.Equal(t, 200, respCode(ctx))
		assert.Equal(t, "BYPASS", respHeader(ctx, header.XCache))
		assert.NotEqual(t, "origin-1", respBody(ctx), "no lookup: the stored entry is never served")
		assert.Equal(t, "origin", respSource(ctx))
	}

	assert.Equal(t, int32(6), originCalls.Load())
	assert.Equal(t, int64(1), store.Stats().HotEntries, "the cacheable response is never stored")
}

// The zero cacheKey UserValue (access-log convention) is kept on a disabled route.
func TestRouteDisabled_ZeroCacheKeyUserValue(t *testing.T) {
	t.Parallel()
	h := testDisabledHandler(t, testStore(t), func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(200)
	})
	ctx := testCtx("GET", "http://example.com/x")
	h.ServeRequest(ctx)
	require.Equal(t, "BYPASS", respHeader(ctx, header.XCache))
	key, ok := ctx.UserValue("cacheKey").(api.Key)
	require.True(t, ok)
	assert.Equal(t, api.Key{}, key)
}

// Origin freshness headers pass through untouched: the bypass path synthesizes nothing.
func TestRouteDisabled_OriginFreshnessHeadersPassThrough(t *testing.T) {
	t.Parallel()
	h := testDisabledHandler(t, testStore(t), func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60, public")
		ctx.Response.Header.Set(header.CDNCacheControl, "max-age=30")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	})
	ctx := testCtx("GET", "http://example.com/x")
	h.ServeRequest(ctx)
	require.Equal(t, "BYPASS", respHeader(ctx, header.XCache))
	assert.Equal(t, "max-age=60, public", respHeader(ctx, header.CacheControl))
	assert.Equal(t, "max-age=30", respHeader(ctx, header.CDNCacheControl))
}

// The route's policies still apply on a disabled route: request/response
// header_set/header_remove, strip_prefix, forwarded.
func TestRouteDisabled_PoliciesStillApplied(t *testing.T) {
	t.Parallel()
	var gotURI, gotXCustom, gotXRemove, gotXFF string
	upstream := func(ctx *fasthttp.RequestCtx) {
		gotURI = string(ctx.RequestURI())
		gotXCustom = string(ctx.Request.Header.Peek("X-Custom"))
		gotXRemove = string(ctx.Request.Header.Peek("X-To-Remove"))
		gotXFF = string(ctx.Request.Header.Peek(header.XForwardedFor))
		ctx.Response.Header.Set("X-Origin-Strip", "gone")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}
	h := NewHandler(HandlerConfig{
		Upstream:             upstream,
		FastClient:           &testFastClient{handler: upstream},
		Store:                testStore(t),
		StripPrefix:          "/api",
		RequestHeaderSet:     map[string]string{"X-Custom": "set-by-route"},
		RequestHeaderRemove:  []string{"X-To-Remove"},
		ResponseHeaderSet:    map[string]string{"X-Client": "set-by-route"},
		ResponseHeaderRemove: []string{"X-Origin-Strip"},
		Forwarded:            ForwardedPolicy{ClientIP: true},
		Disabled:             true,
	})

	ctx := testCtx("GET", "/api/items?q=1")
	ctx.Request.Header.Set("X-To-Remove", "must-not-reach-origin")
	h.ServeRequest(ctx)

	require.Equal(t, "BYPASS", respHeader(ctx, header.XCache))
	assert.Equal(t, "/items?q=1", gotURI)
	assert.Equal(t, "set-by-route", gotXCustom)
	assert.Empty(t, gotXRemove)
	assert.NotEmpty(t, gotXFF)
	assert.Equal(t, "set-by-route", respHeader(ctx, "X-Client"))
	assert.Empty(t, respHeader(ctx, "X-Origin-Strip"))
}

// SSE intent keeps the serveCacheBypass branch: handleSSE, streamed live, never stored.
func TestRouteDisabled_SSEStreamsLive(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	var originCalls atomic.Int32
	h := testDisabledHandler(t, store, func(ctx *fasthttp.RequestCtx) {
		originCalls.Add(1)
		ctx.SetContentType("text/event-stream")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("data: one\n\n")
	})
	ctx := testCtxWithHeader("GET", "http://example.com/stream", header.Accept, "text/event-stream")
	h.ServeRequest(ctx)
	require.Equal(t, "BYPASS", respHeader(ctx, header.XCache))
	assert.Equal(t, "data: one\n\n", respBody(ctx))
	assert.Equal(t, int32(1), originCalls.Load())
	assert.Equal(t, int64(0), store.Stats().HotEntries)
}

// Unsafe methods keep the invalidating dispatch (RFC 9111 §4.4): a POST on
// a disabled route still purges same-URI entries.
func TestRouteDisabled_UnsafeMethodStillInvalidates(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60, public")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}
	enabled := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store:      store,
	})
	fill := testCtx("GET", "http://example.com/thing")
	enabled.ServeRequest(fill)
	require.Equal(t, int64(1), store.Stats().HotEntries)

	disabled := testDisabledHandler(t, store, upstream)
	post := testCtx("POST", "http://example.com/thing")
	post.Request.SetBody([]byte("mutation"))
	disabled.ServeRequest(post)
	assert.Equal(t, "MISS", respHeader(post, header.XCache))
	assert.Equal(t, 200, respCode(post))
	assert.Equal(t, int64(0), store.Stats().HotEntries, "the entry is invalidated")
}

// Parity with today's per-request gate: a disabled route and a no-store
// request on an enabled route produce the same BYPASS response.
func TestRouteDisabled_ParityWithPerRequestBypass(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60, public")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("same"))
	}
	enabled := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store:      store,
	})
	disabled := testDisabledHandler(t, store, upstream)

	gated := testCtx("GET", "http://example.com/p")
	gated.Request.Header.Set(header.CacheControl, "no-store")
	enabled.ServeRequest(gated)

	off := testCtx("GET", "http://example.com/p")
	disabled.ServeRequest(off)

	assert.Equal(t, respHeader(gated, header.XCache), respHeader(off, header.XCache))
	assert.Equal(t, respBody(gated), respBody(off))
	assert.Equal(t, respCode(gated), respCode(off))
}

// The RFC 7234 §3 POST-response store must not fire on a disabled route.
func TestRouteDisabled_PostResponseNeverStored(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	var originCalls atomic.Int32
	upstream := func(ctx *fasthttp.RequestCtx) {
		originCalls.Add(1)
		ctx.Response.Header.Set(header.CacheControl, "max-age=60, public")
		ctx.Response.Header.Set(header.ContentLocation, "/thing")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("created"))
	}
	disabled := testDisabledHandler(t, store, upstream)
	post := testCtx("POST", "/thing")
	post.Request.SetBody([]byte("mutation"))
	disabled.ServeRequest(post)
	assert.Equal(t, "MISS", respHeader(post, header.XCache))
	assert.Equal(t, 200, respCode(post))
	assert.Equal(t, int64(0), store.Stats().HotEntries, "fresh POST + Content-Location still not stored")

	// A later GET still bypasses: nothing was stored to serve it.
	get := testCtx("GET", "/thing")
	disabled.ServeRequest(get)
	assert.Equal(t, "BYPASS", respHeader(get, header.XCache))
	assert.Equal(t, int32(2), originCalls.Load())
}

// only-if-cached keeps the bypass path's 504.
func TestRouteDisabled_OnlyIfCached504(t *testing.T) {
	t.Parallel()
	h := testDisabledHandler(t, testStore(t), func(ctx *fasthttp.RequestCtx) {
		t.Error("the origin must not be reached")
	})
	ctx := testCtx("GET", "http://example.com/x")
	ctx.Request.Header.Set(header.CacheControl, "only-if-cached")
	h.ServeRequest(ctx)
	assert.Equal(t, fasthttp.StatusGatewayTimeout, respCode(ctx))
}
