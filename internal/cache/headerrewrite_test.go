package cache

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// serveCtx builds a plain RequestCtx for handler tests.
func serveCtx(t *testing.T, uri string) *fasthttp.RequestCtx {
	t.Helper()
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test" + uri)
	ctx.Request.Header.SetHost("test")
	return ctx
}

// headerRewriteHandler builds a cache.Handler wired to an in-process
// origin with the given rewrite directives. The origin echoes the
// rewritten request header into X-Origin-Saw, always sets
// X-Origin-Marker, and sends a cacheable response so both rewrite
// directions are observable across hit and miss.
func headerRewriteHandler(t *testing.T, cfgFn func(*HandlerConfig)) *Handler {
	t.Helper()
	origin := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.Response.Header.Set("X-Origin-Marker", "present")
		if v := string(ctx.Request.Header.Peek("X-Rewrite-Me")); v != "" {
			ctx.Response.Header.Set("X-Origin-Saw", v)
		}
		ctx.SetStatusCode(200)
		ctx.SetBodyString("ok")
	}
	cfg := HandlerConfig{
		FastClient: &testFastClient{handler: origin},
		Store:      newTestStore(),
		Logger:     slog.Default(),
	}
	cfgFn(&cfg)
	return NewHandler(cfg)
}

func TestHeaderRewrite_RequestSetOnOriginFetch(t *testing.T) {
	t.Parallel()
	h := headerRewriteHandler(t, func(cfg *HandlerConfig) {
		cfg.RequestHeaderSet = map[string]string{"X-Rewrite-Me": "rewritten"}
	})
	ctx := serveCtx(t, "/one")
	h.ServeRequest(ctx)

	// The origin saw the rewritten header on the miss.
	require.Equal(t, "rewritten", string(ctx.Response.Header.Peek("X-Origin-Saw")))
	require.Equal(t, "present", string(ctx.Response.Header.Peek("X-Origin-Marker")))
	assert.Equal(t, "MISS", string(ctx.Response.Header.Peek(header.XCache)))
}

func TestHeaderRewrite_RequestRemoveOnOriginFetch(t *testing.T) {
	t.Parallel()
	h := headerRewriteHandler(t, func(cfg *HandlerConfig) {
		cfg.RequestHeaderRemove = []string{"X-Rewrite-Me"}
	})
	ctx := serveCtx(t, "/one")
	ctx.Request.Header.Set("X-Rewrite-Me", "must-not-reach-origin")
	h.ServeRequest(ctx)

	// The origin never saw the removed header.
	require.Equal(t, "", string(ctx.Response.Header.Peek("X-Origin-Saw")))
}

func TestHeaderRewrite_RequestSetOverridesClient(t *testing.T) {
	t.Parallel()
	h := headerRewriteHandler(t, func(cfg *HandlerConfig) {
		cfg.RequestHeaderSet = map[string]string{"X-Rewrite-Me": "authoritative"}
	})
	ctx := serveCtx(t, "/two")
	ctx.Request.Header.Set("X-Rewrite-Me", "client-value")
	h.ServeRequest(ctx)
	require.Equal(t, "authoritative", string(ctx.Response.Header.Peek("X-Origin-Saw")))
}

func TestHeaderRewrite_ResponseSetOnHitAndMiss(t *testing.T) {
	t.Parallel()
	h := headerRewriteHandler(t, func(cfg *HandlerConfig) {
		cfg.ResponseHeaderSet = map[string]string{"X-Content-Type-Options": "nosniff"}
	})

	// Miss: response carries the set header.
	miss := serveCtx(t, "/cached")
	h.ServeRequest(miss)
	require.Equal(t, "nosniff", string(miss.Response.Header.Peek("X-Content-Type-Options")))

	// Populate the cache, then hit.
	hit := serveCtx(t, "/cached")
	h.ServeRequest(hit)
	require.Equal(t, "HIT", string(hit.Response.Header.Peek(header.XCache)))
	require.Equal(t, "nosniff", string(hit.Response.Header.Peek("X-Content-Type-Options")))
}

func TestHeaderRewrite_ResponseRemoveOnHitAndMiss(t *testing.T) {
	t.Parallel()
	h := headerRewriteHandler(t, func(cfg *HandlerConfig) {
		cfg.ResponseHeaderRemove = []string{"X-Origin-Marker"}
	})

	miss := serveCtx(t, "/cached")
	h.ServeRequest(miss)
	require.Equal(t, "", string(miss.Response.Header.Peek("X-Origin-Marker")), "removed on miss")

	hit := serveCtx(t, "/cached")
	h.ServeRequest(hit)
	require.Equal(t, "HIT", string(hit.Response.Header.Peek(header.XCache)))
	require.Equal(t, "", string(hit.Response.Header.Peek("X-Origin-Marker")), "removed on hit")
}

func TestHeaderRewrite_NilDirectivesAreNoOps(t *testing.T) {
	t.Parallel()
	h := headerRewriteHandler(t, func(cfg *HandlerConfig) {})
	ctx := serveCtx(t, "/plain")
	h.ServeRequest(ctx)
	require.Equal(t, "present", string(ctx.Response.Header.Peek("X-Origin-Marker")))
	require.Equal(t, 200, ctx.Response.StatusCode())
}
