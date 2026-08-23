package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestHandler_XCacheSource_MissThenHit(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))

	ctx1 := testCtx("GET", "http://example.com/foo")
	serveRequest(h, ctx1)
	require.Equal(t, string(api.SourceOrigin), respHeader(ctx1, header.XCacheSource))

	ctx2 := testCtx("GET", "http://example.com/foo")
	serveRequest(h, ctx2)
	require.Equal(t, string(api.SourceHot), respHeader(ctx2, header.XCacheSource))
}

func TestHandler_XCacheSource_Bypass(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))

	ctx := testCtxWithHeader("GET", "http://example.com/bypass", header.CacheControl, "no-store")
	serveRequest(h, ctx)
	require.Equal(t, "BYPASS", respHeader(ctx, header.XCache))
	require.Equal(t, "", respHeader(ctx, header.XCacheSource))
}

func TestHandler_XCacheSource_OnlyIfCached_504(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))

	ctx := testCtxWithHeader("GET", "http://example.com/missing", header.CacheControl, "only-if-cached")
	serveRequest(h, ctx)

	require.Equal(t, 504, respCode(ctx))
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))
	require.Equal(t, "", respHeader(ctx, header.XCacheSource))
}

func TestHandler_XCacheSource_InvalidateAndProxy_Origin(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))

	ctx := testCtx("POST", "http://example.com/res")
	serveRequest(h, ctx)
	require.Equal(t, string(api.SourceOrigin), respHeader(ctx, header.XCacheSource))
}

func TestHandler_XCacheSource_FetchAndStore_Error_Origin(t *testing.T) {
	t.Parallel()
	bigUpstream := func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(200)
		_, _ = ctx.Write(make([]byte, 10<<20))
	}
	h := NewHandler(HandlerConfig{
		Upstream:   bigUpstream,
		FastClient: &testFastClient{handler: bigUpstream},
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  1 << 20,
			NumShards: 2,
		}),
	})

	ctx := testCtx("GET", "http://example.com/fail")
	serveRequest(h, ctx)

	require.Equal(t, 502, respCode(ctx))
	require.Equal(t, string(api.SourceOrigin), respHeader(ctx, header.XCacheSource))
}

func TestHandler_XCacheSource_Conditional304_Hot(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))

	ctx := testCtx("GET", "http://example.com/304")
	serveRequest(h, ctx)

	ctx2 := testCtxWithHeader("GET", "http://example.com/304", header.IfNoneMatch, `"v1"`)
	serveRequest(h, ctx2)

	require.Equal(t, 304, respCode(ctx2))
	require.Equal(t, "HIT", respHeader(ctx2, header.XCache))
	require.Equal(t, string(api.SourceHot), respHeader(ctx2, header.XCacheSource))
}

func TestHandler_XCacheSource_PeerHit(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  1 << 20,
		NumShards: 2,
	})
	h := NewHandler(HandlerConfig{
		Upstream: origin200("body"),
		Store:    store,
		OwnerFn: func(key api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "peer:1"}, false
		},
		PeerFetch: func(_ context.Context, _ api.PeerInfo, key api.Key) (*api.Object, error) {
			return &api.Object{
				Key:        key,
				StatusCode: 200,
				Header:     headerMap(header.CacheControl, "max-age=60"),
				Body:       []byte("peer-body"),
				BodySize:   9,
				TTL:        time.Minute,
				StoredAt:   time.Now(),
			}, nil
		},
	})

	ctx := testCtx("GET", "http://example.com/peer")
	serveRequest(h, ctx)

	require.Equal(t, "HIT", respHeader(ctx, header.XCache))
	require.Equal(t, string(api.SourcePeer), respHeader(ctx, header.XCacheSource))
}

func TestHandler_XCacheSource_Range_Hot(t *testing.T) {
	t.Parallel()
	h := testHandler(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.ContentType, "text/plain")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("0123456789"))
	})

	ctx := testCtx("GET", "http://example.com/range")
	serveRequest(h, ctx)

	ctx2 := testCtxWithHeader("GET", "http://example.com/range", header.Range, "bytes=0-4")
	serveRequest(h, ctx2)

	require.Equal(t, 206, respCode(ctx2))
	require.Equal(t, "HIT", respHeader(ctx2, header.XCache))
	require.Equal(t, string(api.SourceHot), respHeader(ctx2, header.XCacheSource))
}

func TestHandler_XCacheSource_InvalidateAndProxy_SpoofPrevention(t *testing.T) {
	t.Parallel()
	spoofUpstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.XCacheSource, "hot")
		ctx.Response.Header.Set(header.XCache, "HIT")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("ok"))
	}
	h := testHandler(t, spoofUpstream)

	ctx := testCtx("POST", "http://example.com/spoof")
	serveRequest(h, ctx)

	require.Equal(t, string(api.SourceOrigin), respHeader(ctx, header.XCacheSource))
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))
}

func TestHandler_XCacheSource_Bypass_SpoofPrevention(t *testing.T) {
	t.Parallel()
	spoofUpstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.XCacheSource, "hot")
		ctx.Response.Header.Set(header.XCache, "HIT")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("ok"))
	}
	h := testHandler(t, spoofUpstream)

	ctx := testCtxWithHeader("GET", "http://example.com/bypass-spoof", header.CacheControl, "no-store")
	serveRequest(h, ctx)

	require.Equal(t, "BYPASS", respHeader(ctx, header.XCache))
	require.Equal(t, "", respHeader(ctx, header.XCacheSource))
}

func TestHandler_Conditional304_ETagCanonical(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))

	ctx := testCtx("GET", "http://example.com/etag304")
	serveRequest(h, ctx)

	ctx2 := testCtxWithHeader("GET", "http://example.com/etag304", header.IfNoneMatch, `"v1"`)
	serveRequest(h, ctx2)

	require.Equal(t, 304, respCode(ctx2))
	require.Equal(t, `"v1"`, respHeader(ctx2, header.ETag))
}
