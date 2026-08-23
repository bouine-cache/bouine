package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
)

func newDefaultTTLHandler(t *testing.T, upstream fasthttp.RequestHandler, def time.Duration) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	return NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store:      store,
		DefaultTTL: def,
	})
}

func TestIsCacheableWithDefault_NoFreshness(t *testing.T) {
	t.Parallel()
	req := header.Map{}
	resp := header.Map{}

	require.False(t, IsCacheable(200, req, resp))
	assert.True(t, IsCacheableWithDefault(200, req, resp, 0, 5*time.Second))
	assert.False(t, IsCacheableWithDefault(200, req, resp, 0, 0))
}

func TestIsCacheableWithDefault_HonoursBlocks(t *testing.T) {
	t.Parallel()
	const def = 5 * time.Second
	cases := []struct {
		name   string
		status int
		req    header.Map
		resp   header.Map
		want   bool
	}{
		{"no-store", 200, header.Map{}, headerMap(header.CacheControl, "no-store"), false},
		{"private", 200, header.Map{}, headerMap(header.CacheControl, "private"), false},
		{"set-cookie", 200, header.Map{}, headerMap(header.SetCookie, "sid=abc"), false},
		{"vary-star", 200, header.Map{}, headerMap(header.Vary, "*"), false},
		{"pragma-no-cache", 200, header.Map{}, headerMap(header.Pragma, "no-cache"), false},
		{"authorization", 200, headerMap(header.Authorization, "Bearer x"), header.Map{}, false},
		{"5xx-excluded", 500, header.Map{}, header.Map{}, false},
		{"plain-200-ok", 200, header.Map{}, header.Map{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := IsCacheableWithDefault(tc.status, tc.req, tc.resp, 0, def)
			if got != tc.want {
				t.Errorf("IsCacheableWithDefault(%d) = %v, want %v", tc.status, got, tc.want)
			}
		})
	}
}

func TestDefaultTTL_CachesHeaderlessResponse(t *testing.T) {
	t.Parallel()
	var hits int
	upstream := func(ctx *fasthttp.RequestCtx) {
		hits++
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}
	h := newDefaultTTLHandler(t, upstream, 5*time.Second)

	ctx1 := testCtx("GET", "http://example.com/r")
	serveRequest(h, ctx1)
	require.Equal(t, "MISS", respHeader(ctx1, header.XCache))

	ctx2 := testCtx("GET", "http://example.com/r")
	serveRequest(h, ctx2)
	require.Equal(t, "HIT", respHeader(ctx2, header.XCache))
	assert.Equal(t, 1, hits)
}

func TestDefaultTTL_DisabledKeepsMISS(t *testing.T) {
	t.Parallel()
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}
	h := newDefaultTTLHandler(t, upstream, 0)

	for range 2 {
		ctx := testCtx("GET", "http://example.com/r")
		serveRequest(h, ctx)
		require.Equal(t, "MISS", respHeader(ctx, header.XCache))
	}
}

func TestDefaultTTL_NoStoreStillBypasses(t *testing.T) {
	t.Parallel()
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "no-store")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}
	h := newDefaultTTLHandler(t, upstream, 5*time.Second)

	for range 2 {
		ctx := testCtx("GET", "http://example.com/r")
		serveRequest(h, ctx)
		require.Equal(t, "MISS", respHeader(ctx, header.XCache))
	}
}
