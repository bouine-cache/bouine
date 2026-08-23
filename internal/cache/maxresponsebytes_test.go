package cache

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
	"github.com/valyala/fasthttp"
)

func newMaxResponseBytesHandler(t *testing.T, upstream fasthttp.RequestHandler, maxBytes int64) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	return NewHandler(HandlerConfig{
		Upstream:         upstream,
		FastClient:       &testFastClient{handler: upstream},
		Store:            store,
		MaxResponseBytes: maxBytes,
	})
}

func TestMaxResponseBytes_OverLimitReturns502(t *testing.T) {
	t.Parallel()
	calls := 0
	body := strings.Repeat("x", 2048)
	upstream := func(ctx *fasthttp.RequestCtx) {
		calls++
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte(body))
	}
	h := newMaxResponseBytesHandler(t, upstream, 1024)

	ctx1 := testCtx("GET", "http://example.com/too-big")
	serveRequest(h, ctx1)

	require.Equal(t, fasthttp.StatusBadGateway, respCode(ctx1))
	assert.Equal(t, 1, calls)

	ctx2 := testCtx("GET", "http://example.com/too-big")
	serveRequest(h, ctx2)
	assert.NotEqual(t, "HIT", respHeader(ctx2, header.XCache))
	assert.Equal(t, 2, calls)
}

func TestMaxResponseBytes_UnderLimitSucceeds(t *testing.T) {
	t.Parallel()
	calls := 0
	body := strings.Repeat("y", 512)
	upstream := func(ctx *fasthttp.RequestCtx) {
		calls++
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte(body))
	}
	h := newMaxResponseBytesHandler(t, upstream, 1024)

	ctx1 := testCtx("GET", "http://example.com/ok")
	serveRequest(h, ctx1)
	if respCode(ctx1) != 200 || respBody(ctx1) != body {
		t.Fatalf("response under limit should pass through: status=%d body=%q", respCode(ctx1), respBody(ctx1))
	}

	ctx2 := testCtx("GET", "http://example.com/ok")
	serveRequest(h, ctx2)
	assert.Equal(t, "HIT", respHeader(ctx2, header.XCache))
	assert.Equal(t, 1, calls)
}

func TestMaxResponseBytes_ExactBoundarySucceeds(t *testing.T) {
	t.Parallel()
	body := strings.Repeat("a", 512)
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte(body))
	}
	h := newMaxResponseBytesHandler(t, upstream, 512)

	ctx := testCtx("GET", "http://example.com/exact")
	serveRequest(h, ctx)
	if respCode(ctx) != 200 || respBody(ctx) != body {
		t.Fatalf("response at exact boundary should pass through: status=%d body=%q", respCode(ctx), respBody(ctx))
	}
}

func TestMaxResponseBytes_DefaultAppliedWhenZero(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream: func(ctx *fasthttp.RequestCtx) {
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			ctx.SetStatusCode(200)
			_, _ = ctx.Write([]byte("small"))
		},
		Store:            store,
		MaxResponseBytes: 0,
	})
	const wantDefault = 4 << 20
	require.Equal(t, int64(wantDefault), h.maxResponseBytes)
}

func TestMaxResponseBytes_InvalidateAndProxyOverLimit(t *testing.T) {
	t.Parallel()
	calls := 0
	body := strings.Repeat("z", 2048)
	upstream := func(ctx *fasthttp.RequestCtx) {
		calls++
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte(body))
	}
	h := newMaxResponseBytesHandler(t, upstream, 1024)

	ctx := testCtx("POST", "http://example.com/post")
	serveRequest(h, ctx)

	require.Equal(t, fasthttp.StatusBadGateway, respCode(ctx))
}
