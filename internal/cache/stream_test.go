package cache

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

func TestStreamBypass_ResponseBodyStreamed(t *testing.T) {
	t.Parallel()
	body := []byte("streamed bypass body")
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "no-store")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write(body)
	}
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  4 << 20,
			NumShards: 2,
		}),
	})

	ctx := testCtx("GET", "http://example.com/bypass")
	serveRequest(h, ctx)

	require.Equal(t, 200, respCode(ctx))
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))
	require.Equal(t, string(body), respBody(ctx))
}

func TestStreamMiss_CacheableResponseBodyStreamed(t *testing.T) {
	t.Parallel()
	body := []byte("streamed miss body")
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write(body)
	}
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  4 << 20,
			NumShards: 2,
		}),
	})

	ctx := testCtx("GET", "http://example.com/stream-miss")
	serveRequest(h, ctx)

	require.Equal(t, 200, respCode(ctx))
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))
	require.Equal(t, string(body), respBody(ctx))

	// Second request should be a cache hit.
	ctx2 := testCtx("GET", "http://example.com/stream-miss")
	serveRequest(h, ctx2)
	require.Equal(t, "HIT", respHeader(ctx2, header.XCache))
	require.Equal(t, string(body), respBody(ctx2))
}

func TestStreamMiss_NonCacheableStreamedNotStored(t *testing.T) {
	t.Parallel()
	body := []byte("non-cacheable streamed body")
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "no-store")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write(body)
	}
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  4 << 20,
			NumShards: 2,
		}),
	})

	ctx := testCtx("GET", "http://example.com/non-cacheable")
	serveRequest(h, ctx)

	require.Equal(t, 200, respCode(ctx))
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))
	require.Equal(t, string(body), respBody(ctx))

	// Second request should also be a MISS (not cached).
	ctx2 := testCtx("GET", "http://example.com/non-cacheable")
	serveRequest(h, ctx2)
	require.Equal(t, "MISS", respHeader(ctx2, header.XCache))
}

func TestStreamMiss_VaryStoredCorrectly(t *testing.T) {
	t.Parallel()
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.Response.Header.Set(header.Vary, "X-Test-Variant")
		_, _ = ctx.Write([]byte("variant-" + string(ctx.Request.Header.Peek("X-Test-Variant"))))
	}
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  4 << 20,
			NumShards: 2,
		}),
	})

	// First variant.
	ctxA := testCtx("GET", "http://example.com/vary-stream")
	ctxA.Request.Header.Set("X-Test-Variant", "a")
	serveRequest(h, ctxA)
	require.Equal(t, "MISS", respHeader(ctxA, header.XCache))
	require.Equal(t, "variant-a", respBody(ctxA))

	// Second variant.
	ctxB := testCtx("GET", "http://example.com/vary-stream")
	ctxB.Request.Header.Set("X-Test-Variant", "b")
	serveRequest(h, ctxB)
	require.Equal(t, "MISS", respHeader(ctxB, header.XCache))
	require.Equal(t, "variant-b", respBody(ctxB))

	// First variant should be a hit.
	ctxA2 := testCtx("GET", "http://example.com/vary-stream")
	ctxA2.Request.Header.Set("X-Test-Variant", "a")
	serveRequest(h, ctxA2)
	require.Equal(t, "HIT", respHeader(ctxA2, header.XCache))
	require.Equal(t, "variant-a", respBody(ctxA2))
}

func TestStreamMiss_MaxResponseBytesExceeded(t *testing.T) {
	t.Parallel()
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write(make([]byte, 10<<20))
	}
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  1 << 30,
			NumShards: 2,
		}),
		MaxResponseBytes: 1 << 20, // 1 MiB
	})

	ctx := testCtx("GET", "http://example.com/too-large")
	serveRequest(h, ctx)

	require.Equal(t, 502, respCode(ctx))
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))
}

func TestStreamMiss_InflightDedup(t *testing.T) {
	t.Parallel()
	body := []byte("dedup body")
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write(body)
	}
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  4 << 20,
			NumShards: 2,
		}),
	})

	// Two concurrent requests for the same key — the follower should
	// get the buffered result from the leader.
	ctx1 := testCtx("GET", "http://example.com/dedup")
	ctx2 := testCtx("GET", "http://example.com/dedup")

	serveRequest(h, ctx1)
	serveRequest(h, ctx2)

	require.Equal(t, 200, respCode(ctx1))
	require.Equal(t, string(body), respBody(ctx1))
	require.Equal(t, 200, respCode(ctx2))
	require.Equal(t, string(body), respBody(ctx2))
}
