package cache

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
)

func newSetCookieHandler(t *testing.T, upstream fasthttp.RequestHandler, allow bool) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	return NewHandler(HandlerConfig{
		Upstream:       upstream,
		FastClient:     &testFastClient{handler: upstream},
		Store:          store,
		AllowSetCookie: allow,
	})
}

func originWithSetCookie(body, cc, cookie string) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		if cc != "" {
			ctx.Response.Header.Set(header.CacheControl, cc)
		}
		if cookie != "" {
			ctx.Response.Header.Set(header.SetCookie, cookie)
		}
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte(body))
	}
}

func TestSetCookie_DefaultBlocksCaching(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := func(ctx *fasthttp.RequestCtx) {
		calls++
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.SetCookie, "session=abc123; Path=/")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}
	h := newSetCookieHandler(t, upstream, false)

	url := "http://example.com/login"
	ctx1 := testCtx("GET", url)
	serveRequest(h, ctx1)

	require.Equal(t, 200, respCode(ctx1))
	assert.Equal(t, "session=abc123; Path=/", respHeader(ctx1, header.SetCookie))

	ctx2 := testCtx("GET", url)
	serveRequest(h, ctx2)

	assert.Equal(t, 2, calls)
	assert.NotEqual(t, "HIT", respHeader(ctx2, header.XCache))
}

func TestSetCookie_AllowTrueStoresWithoutCookie(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := func(ctx *fasthttp.RequestCtx) {
		calls++
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.SetCookie, "session=xyz789; Path=/; HttpOnly")
		ctx.Response.Header.Set(header.ETag, `"v1"`)
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("public-body"))
	}
	h := newSetCookieHandler(t, upstream, true)

	url := "http://example.com/page"

	ctx1 := testCtx("GET", url)
	serveRequest(h, ctx1)
	require.Equal(t, 200, respCode(ctx1))
	assert.NotEqual(t, "", respHeader(ctx1, header.SetCookie))

	ctx2 := testCtx("GET", url)
	serveRequest(h, ctx2)
	assert.Equal(t, "HIT", respHeader(ctx2, header.XCache))
	assert.Equal(t, "", respHeader(ctx2, header.SetCookie))
	assert.Equal(t, 1, calls)
}

func TestSetCookie_NoStoreStillBlocks(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := func(ctx *fasthttp.RequestCtx) {
		calls++
		ctx.Response.Header.Set(header.CacheControl, "no-store")
		ctx.Response.Header.Set(header.SetCookie, "session=nope")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("private"))
	}
	h := newSetCookieHandler(t, upstream, true)

	url := "http://example.com/auth"
	for range 3 {
		ctx := testCtx("GET", url)
		serveRequest(h, ctx)
	}
	assert.Equal(t, 3, calls)
}

func TestSetCookie_NoSetCookieHeaderUnaffected(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := func(ctx *fasthttp.RequestCtx) {
		calls++
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("public"))
	}
	h := newSetCookieHandler(t, upstream, false)

	url := "http://example.com/public"
	ctx1 := testCtx("GET", url)
	serveRequest(h, ctx1)
	ctx2 := testCtx("GET", url)
	serveRequest(h, ctx2)

	assert.Equal(t, "HIT", respHeader(ctx2, header.XCache))
	assert.Equal(t, 1, calls)
}

func TestSetCookie_DefaultBlocksEvenWithExplicitFreshness(t *testing.T) {
	t.Parallel()
	upstream := originWithSetCookie("body", "max-age=3600", "token=secret123")
	h := newSetCookieHandler(t, upstream, false)

	url := "http://example.com/important"
	ctx := testCtx("GET", url)
	serveRequest(h, ctx)

	key := BuildKey(requestInfoFromURL("GET", url), nil)
	obj, _, _ := h.store.Get(context.Background(), key)
	require.Nil(t, obj)
}
