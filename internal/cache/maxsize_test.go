package cache

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
)

func newMaxSizeHandler(t *testing.T, upstream fasthttp.RequestHandler, maxSize int64) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	return NewHandler(HandlerConfig{
		Upstream:      upstream,
		FastClient:    &testFastClient{handler: upstream},
		Store:         store,
		MaxObjectSize: maxSize,
	})
}

func TestMaxObjectSize_SmallResponseCached(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := func(ctx *fasthttp.RequestCtx) {
		calls++
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("small"))
	}
	h := newMaxSizeHandler(t, upstream, 1024)

	url := "http://example.com/small"
	ctx1 := testCtx("GET", url)
	serveRequest(h, ctx1)
	ctx2 := testCtx("GET", url)
	serveRequest(h, ctx2)

	assert.Equal(t, "HIT", respHeader(ctx2, header.XCache))
	assert.Equal(t, 1, calls)
}

func TestMaxObjectSize_LargeResponseSkipped(t *testing.T) {
	t.Parallel()
	calls := 0
	body := strings.Repeat("x", 2048)
	upstream := func(ctx *fasthttp.RequestCtx) {
		calls++
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte(body))
	}
	h := newMaxSizeHandler(t, upstream, 1024)

	url := "http://example.com/large"
	ctx1 := testCtx("GET", url)
	serveRequest(h, ctx1)
	if respCode(ctx1) != 200 || respBody(ctx1) != body {
		t.Fatalf("first response wrong: status=%d body=%q", respCode(ctx1), respBody(ctx1))
	}

	ctx2 := testCtx("GET", url)
	serveRequest(h, ctx2)

	assert.Equal(t, 2, calls)
	assert.NotEqual(t, "HIT", respHeader(ctx2, header.XCache))
}

func TestMaxObjectSize_ZeroDisabled(t *testing.T) {
	t.Parallel()
	calls := 0
	body := strings.Repeat("x", 4096)
	upstream := func(ctx *fasthttp.RequestCtx) {
		calls++
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte(body))
	}
	h := newMaxSizeHandler(t, upstream, 0)

	url := "http://example.com/nolimit"
	ctx1 := testCtx("GET", url)
	serveRequest(h, ctx1)
	ctx2 := testCtx("GET", url)
	serveRequest(h, ctx2)

	assert.Equal(t, "HIT", respHeader(ctx2, header.XCache))
	assert.Equal(t, 1, calls)
}

func TestMaxObjectSize_ExactBoundaryCached(t *testing.T) {
	t.Parallel()
	body := strings.Repeat("a", 512)
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte(body))
	}
	h := newMaxSizeHandler(t, upstream, 512)

	url := "http://example.com/exact"
	ctx := testCtx("GET", url)
	serveRequest(h, ctx)

	key := BuildKey(requestInfoFromURL("GET", url), nil)
	obj, _, _ := h.store.Get(context.Background(), key)
	require.NotNil(t, obj)
}
