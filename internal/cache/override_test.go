package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
)

func newOverrideHandler(t *testing.T, upstream fasthttp.RequestHandler, override time.Duration) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	return NewHandler(HandlerConfig{
		Upstream:    upstream,
		FastClient:  &testFastClient{handler: upstream},
		Store:       store,
		OverrideTTL: override,
	})
}

func TestOverrideTTL_WinsOverMaxAge(t *testing.T) {
	t.Parallel()
	const routeOverride = 2 * time.Hour

	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=30")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}
	h := newOverrideHandler(t, upstream, routeOverride)

	ctx := testCtx("GET", "http://example.com/r")
	serveRequest(h, ctx)
	require.Equal(t, 200, respCode(ctx))

	key := BuildKey(requestInfoFromCtx(ctx), nil)
	obj, _, err := h.store.Get(context.Background(), key)
	if err != nil || obj == nil {
		t.Fatalf("object not stored: %v", err)
	}
	assert.Equal(t, routeOverride, obj.TTL)
}

func TestOverrideTTL_ForwardsOriginalCacheControlHeader(t *testing.T) {
	t.Parallel()
	const upstreamCC = "max-age=30, must-revalidate"

	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, upstreamCC)
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}
	h := newOverrideHandler(t, upstream, 90*time.Minute)

	ctx := testCtx("GET", "http://example.com/fwd")
	serveRequest(h, ctx)
	assert.Equal(t, upstreamCC, respHeader(ctx, header.CacheControl))

	ctx2 := testCtx("GET", "http://example.com/fwd")
	serveRequest(h, ctx2)
	assert.Equal(t, upstreamCC, respHeader(ctx2, header.CacheControl))
}

func TestOverrideTTL_ZeroDisabled(t *testing.T) {
	t.Parallel()
	const originMaxAge = 45 * time.Second

	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=45")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}
	h := newOverrideHandler(t, upstream, 0)

	ctx := testCtx("GET", "http://example.com/nodis")
	serveRequest(h, ctx)

	key := BuildKey(requestInfoFromCtx(ctx), nil)
	obj, _, err := h.store.Get(context.Background(), key)
	if err != nil || obj == nil {
		t.Fatalf("object not stored: %v", err)
	}
	assert.Equal(t, originMaxAge, obj.TTL)
}

func TestOverrideTTL_HitBeforeExpiry(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := func(ctx *fasthttp.RequestCtx) {
		calls++
		ctx.Response.Header.Set(header.CacheControl, "max-age=1")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}
	h := newOverrideHandler(t, upstream, 24*time.Hour)

	url := "http://example.com/longttl"
	ctx1 := testCtx("GET", url)
	serveRequest(h, ctx1)

	ctx2 := testCtx("GET", url)
	serveRequest(h, ctx2)
	assert.Equal(t, "HIT", respHeader(ctx2, header.XCache))
	assert.Equal(t, 1, calls)
}

func TestOverrideTTL_NoStoreNotCached(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := func(ctx *fasthttp.RequestCtx) {
		calls++
		ctx.Response.Header.Set(header.CacheControl, "no-store")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("private-body"))
	}
	h := newOverrideHandler(t, upstream, time.Hour)

	url := "http://example.com/nostore"
	for range 3 {
		ctx := testCtx("GET", url)
		serveRequest(h, ctx)
	}
	assert.Equal(t, 3, calls)
}

func TestOverrideTTL_ShortensUpstreamTTL(t *testing.T) {
	t.Parallel()
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}
	h := newOverrideHandler(t, upstream, 5*time.Second)

	ctx := testCtx("GET", "http://example.com/short")
	serveRequest(h, ctx)

	key := BuildKey(requestInfoFromCtx(ctx), nil)
	obj, _, _ := h.store.Get(context.Background(), key)
	require.NotNil(t, obj)
	assert.Equal(t, 5*time.Second, obj.TTL)
	assert.Equal(t, "max-age=3600", obj.Header.Get(header.CacheControl))
}

func TestOverrideTTL_WithJitter(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}
	h := NewHandler(HandlerConfig{
		Upstream:      upstream,
		FastClient:    &testFastClient{handler: upstream},
		Store:         store,
		OverrideTTL:   time.Hour,
		JitterPercent: 10,
	})

	ctx := testCtx("GET", "http://example.com/jitter")
	serveRequest(h, ctx)

	key := BuildKey(requestInfoFromCtx(ctx), nil)
	obj, _, _ := h.store.Get(context.Background(), key)
	require.NotNil(t, obj)
	const (
		low  = 54 * time.Minute
		high = 66 * time.Minute
	)
	if obj.TTL < low || obj.TTL > high {
		t.Errorf("jittered TTL = %v, want [%v, %v]", obj.TTL, low, high)
	}
}

func TestOverrideTTL_PreservedAfterConditionalRevalidation(t *testing.T) {
	t.Parallel()
	const etag = `"abc"`
	phase := 0

	upstream := func(ctx *fasthttp.RequestCtx) {
		switch phase {
		case 0:
			ctx.Response.Header.Set(header.CacheControl, "max-age=1, must-revalidate")
			ctx.Response.Header.Set(header.ETag, etag)
			ctx.SetStatusCode(200)
			_, _ = ctx.Write([]byte("body"))
		case 1:
			if string(ctx.Request.Header.Peek(header.IfNoneMatch)) == etag {
				ctx.Response.Header.Set(header.CacheControl, "max-age=1, must-revalidate")
				ctx.Response.Header.Set(header.ETag, etag)
				ctx.SetStatusCode(304)
			} else {
				t.Error("304 phase: expected If-None-Match")
				ctx.SetStatusCode(500)
			}
		}
	}

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	const override = 90 * time.Minute
	h := NewHandler(HandlerConfig{
		Upstream:    upstream,
		FastClient:  &testFastClient{handler: upstream},
		Store:       store,
		OverrideTTL: override,
	})

	url := "http://example.com/reval"

	ctx := testCtx("GET", url)
	serveRequest(h, ctx)

	key := BuildKey(requestInfoFromURL("GET", url), nil)
	obj, _, _ := h.store.Get(context.Background(), key)
	require.NotNil(t, obj)
	expired := obj.CloneForRefresh()
	expired.StoredAt = time.Now().Add(-(override + time.Second))
	_ = h.store.Put(context.Background(), key, expired)

	phase = 1
	ctx2 := testCtx("GET", url)
	serveRequest(h, ctx2)

	after, _, _ := h.store.Get(context.Background(), key)
	require.NotNil(t, after)
	assert.Equal(t, override, after.TTL)
	assert.Equal(t, "max-age=1, must-revalidate", after.Header.Get(header.CacheControl))
}
