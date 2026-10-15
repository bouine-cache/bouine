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
	return newOverrideHandlerNeg(t, upstream, override, nil)
}

// newOverrideHandlerNeg builds an override handler with a negative-caching
// policy for the error-ladder tests (ADR-0059).
func newOverrideHandlerNeg(t *testing.T, upstream fasthttp.RequestHandler, override time.Duration, neg *StatusTTL) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	return NewHandler(HandlerConfig{
		Upstream:    upstream,
		FastClient:  &testFastClient{handler: upstream},
		Store:       store,
		OverrideTTL: override,
		Negative:    neg,
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

// TestOverrideTTL_NotAppliedToErrorStatusWithExplicitFreshness pins the
// ADR-0059 gate: an error response with origin explicit freshness keeps
// that freshness — the route override must not pin a chassis-style
// negative-cached 404 (public, max-age=N) fresh for the override duration.
func TestOverrideTTL_NotAppliedToErrorStatusWithExplicitFreshness(t *testing.T) {
	t.Parallel()
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=600")
		ctx.SetStatusCode(404)
		_, _ = ctx.Write([]byte("not found"))
	}
	h := newOverrideHandler(t, upstream, 30*time.Minute)

	ctx := testCtx("GET", "http://example.com/err-explicit")
	serveRequest(h, ctx)
	require.Equal(t, 404, respCode(ctx))

	key := BuildKey(requestInfoFromCtx(ctx), nil)
	obj, _, err := h.store.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, 600*time.Second, obj.TTL)
}

// TestOverrideTTL_NotAppliedToErrorStatusKeepsNegativeTTL pins the
// negative_ttl rung of the ladder: an uncovered-by-freshness 404 on an
// override route stores for the policy TTL, not the override.
func TestOverrideTTL_NotAppliedToErrorStatusKeepsNegativeTTL(t *testing.T) {
	t.Parallel()
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(404)
		_, _ = ctx.Write([]byte("not found"))
	}
	neg := mustStatusTTL(t, map[string]time.Duration{"404": 15 * time.Second})
	h := newOverrideHandlerNeg(t, upstream, 30*time.Minute, neg)

	ctx := testCtx("GET", "http://example.com/err-neg")
	serveRequest(h, ctx)
	require.Equal(t, 404, respCode(ctx))

	key := BuildKey(requestInfoFromCtx(ctx), nil)
	obj, _, err := h.store.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, 15*time.Second, obj.TTL)
}

// TestOverrideTTL_NotAppliedToErrorStatusHeuristicFallback pins the
// heuristic rung: a 404 + Last-Modified with no explicit freshness and
// no negative_ttl coverage stores for HeuristicTTL (10% of Date −
// Last-Modified), which for a recent Last-Modified is far below the
// override. The uncapped long-Last-Modified case is a documented tradeoff
// (ADR-0059), not pinned here.
func TestOverrideTTL_NotAppliedToErrorStatusHeuristicFallback(t *testing.T) {
	t.Parallel()
	upstream := func(ctx *fasthttp.RequestCtx) {
		// Format(httpTimeFormat) appends a literal "GMT" — convert to UTC
		// first so the parsed Last-Modified is really an hour in the past.
		ctx.Response.Header.Set(header.LastModified, time.Now().UTC().Add(-time.Hour).Format(httpTimeFormat))
		ctx.SetStatusCode(404)
		_, _ = ctx.Write([]byte("not found"))
	}
	h := newOverrideHandler(t, upstream, 30*time.Minute)

	ctx := testCtx("GET", "http://example.com/err-heuristic")
	serveRequest(h, ctx)
	require.Equal(t, 404, respCode(ctx))

	key := BuildKey(requestInfoFromCtx(ctx), nil)
	obj, _, err := h.store.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, obj)
	// HeuristicTTL = 10% of (now − Last-Modified) = ~6m; the sub-second
	// gap between the upstream handler and fill time adds microseconds.
	assert.Greater(t, obj.TTL, 5*time.Minute)
	assert.Less(t, obj.TTL, 7*time.Minute)
}

// TestOverrideTTL_AppliedToRedirectStatus pins the < 400 boundary of the
// gate against a future "simplification" to 2xx-only: redirects are
// content responses and keep the ADR-0011 override.
func TestOverrideTTL_AppliedToRedirectStatus(t *testing.T) {
	t.Parallel()
	const override = 30 * time.Minute
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.Response.Header.Set(header.Location, "/new-home")
		ctx.SetStatusCode(301)
	}
	h := newOverrideHandler(t, upstream, override)

	ctx := testCtx("GET", "http://example.com/redirect")
	serveRequest(h, ctx)
	require.Equal(t, 301, respCode(ctx))

	key := BuildKey(requestInfoFromCtx(ctx), nil)
	obj, _, err := h.store.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, override, obj.TTL)
}

// TestOverrideTTL_5xxNotApplied covers the 5xx half of the gate: a 503
// with origin explicit freshness keeps it (short-lived retry windows,
// not a 30m-pinned outage).
func TestOverrideTTL_5xxNotApplied(t *testing.T) {
	t.Parallel()
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=10")
		ctx.SetStatusCode(503)
		_, _ = ctx.Write([]byte("unavailable"))
	}
	h := newOverrideHandler(t, upstream, 30*time.Minute)

	ctx := testCtx("GET", "http://example.com/err-503")
	serveRequest(h, ctx)
	require.Equal(t, 503, respCode(ctx))

	key := BuildKey(requestInfoFromCtx(ctx), nil)
	obj, _, err := h.store.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, 10*time.Second, obj.TTL)
}

// TestOverrideTTL_PreservedAfterConditionalRevalidation_ErrorStatus is
// the error mirror of PreservedAfterConditionalRevalidation: an error
// object revalidated via 304 re-arms its TTL from the merged freshness
// (max-age=1), never from the route override.
func TestOverrideTTL_PreservedAfterConditionalRevalidation_ErrorStatus(t *testing.T) {
	t.Parallel()
	const etag = `"err"`
	phase := 0

	upstream := func(ctx *fasthttp.RequestCtx) {
		switch phase {
		case 0:
			ctx.Response.Header.Set(header.CacheControl, "max-age=1, must-revalidate")
			ctx.Response.Header.Set(header.ETag, etag)
			ctx.SetStatusCode(404)
			_, _ = ctx.Write([]byte("not found"))
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

	url := "http://example.com/reval-err"

	ctx := testCtx("GET", url)
	serveRequest(h, ctx)
	require.Equal(t, 404, respCode(ctx))

	key := BuildKey(requestInfoFromURL("GET", url), nil)
	obj, _, _ := h.store.Get(context.Background(), key)
	require.NotNil(t, obj)
	require.Equal(t, time.Second, obj.TTL, "seeded 404 must keep origin max-age=1, not the override")
	expired := obj.CloneForRefresh()
	expired.StoredAt = time.Now().Add(-2 * time.Second)
	_ = h.store.Put(context.Background(), key, expired)

	phase = 1
	ctx2 := testCtx("GET", url)
	serveRequest(h, ctx2)

	after, _, _ := h.store.Get(context.Background(), key)
	require.NotNil(t, after)
	assert.Equal(t, time.Second, after.TTL, "refreshed 404 must re-arm from merged max-age, never the override")
	assert.Equal(t, 404, after.StatusCode)
	assert.Equal(t, "max-age=1, must-revalidate", after.Header.Get(header.CacheControl))
}
