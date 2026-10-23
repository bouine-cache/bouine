package cache

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// downstreamcc_test.go pins the ADR-0063 behavior matrix per serve class
// × mode: exact synthesized CDN-Cache-Control bytes, Cache-Control never
// mutated, Age/X-Cache unchanged, origin CDC-family lines replaced under
// sync, and the I8 stale-304 background-revalidation trigger.

// newSyncHandler builds a downstream_freshness: sync handler over a
// synthetic upstream.
// benignOrigin answers every fetch with a fresh, ETag-bearing response:
// tests that serve pre-stored objects and trigger background
// revalidations (StaleHit + SWR) need a non-nil origin for the detached
// revalidation goroutine to fetch against — a nil upstream would panic
// out-of-band and crash the test binary.
func benignOrigin(ctx *fasthttp.RequestCtx) {
	if string(ctx.Request.Header.Peek(header.IfNoneMatch)) != "" {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(304)
		return
	}
	ctx.Response.Header.Set(header.CacheControl, "max-age=60")
	ctx.Response.Header.Set(header.ETag, `"v2"`)
	ctx.SetStatusCode(200)
	_, _ = ctx.Write([]byte("fresh"))
}

func newSyncHandler(t *testing.T, upstream fasthttp.RequestHandler, cfg HandlerConfig) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	cfg.Upstream = upstream
	cfg.FastClient = &testFastClient{handler: upstream}
	cfg.Store = store
	cfg.DownstreamFreshness = downstreamSync
	return NewHandler(cfg)
}

func TestSyncCDC_FreshHitServesWallRemaining(t *testing.T) {
	t.Parallel()
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=120")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}
	h := newSyncHandler(t, upstream, HandlerConfig{})
	defer h.Close(context.Background())

	ctx := testCtx("GET", "http://sync.example.com/fresh")
	serveRequest(h, ctx)
	require.Equal(t, 200, respCode(ctx))
	// The fill carries the min() rule: origin and bouine agree (120s), so
	// the cap equals the final TTL (allowing for second truncation).
	cdc := respHeader(ctx, header.CDNCacheControl)
	rem, err := parseCDCMaxAge(cdc)
	require.NoError(t, err)
	assert.InDelta(t, 120, rem, 2)

	// A hit within the TTL carries TTL − elapsed (floor 0), recomputed
	// from the stored clock, and Cache-Control is untouched.
	ctx2 := testCtx("GET", "http://sync.example.com/fresh")
	serveRequest(h, ctx2)
	require.Equal(t, "HIT", respHeader(ctx2, header.XCache))
	assert.Equal(t, "max-age=120", respHeader(ctx2, header.CacheControl))
	cdc2 := respHeader(ctx2, header.CDNCacheControl)
	require.NotEmpty(t, cdc2)
	rem2, err := parseCDCMaxAge(cdc2)
	require.NoError(t, err)
	assert.InDelta(t, 120, rem2, 3)
	assert.Contains(t, cdc2, "public")
	assert.NotContains(t, cdc2, "stale-while-revalidate")
	assert.NotContains(t, cdc2, "no-cache")
}

func TestSyncCDC_FreshHitMustRevalidateCarried(t *testing.T) {
	t.Parallel()
	key := BuildKey(requestInfoFromURL("GET", "http://sync.example.com/must"), nil)
	h := newSyncHandler(t, benignOrigin, HandlerConfig{})
	defer h.Close(context.Background())
	obj := &api.Object{
		Key: key, StatusCode: 200,
		Header: headerMap(header.CacheControl, "max-age=120, must-revalidate"),
		Body:   []byte("body"), BodySize: 4,
		StoredAt: time.Now(), TTL: 120 * time.Second,
		RespMustRevalidate: true,
	}
	_ = h.store.Put(context.Background(), key, obj)

	ctx := testCtx("GET", "http://sync.example.com/must")
	serveRequest(h, ctx)
	cdc := respHeader(ctx, header.CDNCacheControl)
	assert.Contains(t, cdc, "public")
	assert.Contains(t, cdc, "must-revalidate")
	rem, err := parseCDCMaxAge(cdc)
	require.NoError(t, err)
	assert.InDelta(t, 120, rem, 2)
}

func TestSyncCDC_RevalidatedNoCacheObjectNeverNumeric(t *testing.T) {
	t.Parallel()
	// Stored no-cache object (storable: ETag-bearing, heuristic status):
	// every use revalidates, so the serve must be REVALIDATED and carry
	// public, no-cache — never a numeric clock (I2, RFC 9111 §5.2.2.4).
	h := newSyncHandler(t, func(ctx *fasthttp.RequestCtx) {
		if string(ctx.Request.Header.Peek(header.IfNoneMatch)) != "" {
			ctx.SetStatusCode(304)
			return
		}
		ctx.Response.Header.Set(header.CacheControl, "no-cache, max-age=60")
		ctx.Response.Header.Set(header.ETag, `"v1"`)
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}, HandlerConfig{})
	defer h.Close(context.Background())

	ctx := testCtx("GET", "http://sync.example.com/nc")
	serveRequest(h, ctx)
	require.Equal(t, 200, respCode(ctx))
	// The storable no-cache fill carries public, no-cache (I2 fill
	// carve-out), never a numeric clock.
	assert.Equal(t, "public, no-cache", respHeader(ctx, header.CDNCacheControl))

	// Every subsequent use revalidates; the REVALIDATED serve keeps
	// public, no-cache.
	ctx2 := testCtx("GET", "http://sync.example.com/nc")
	serveRequest(h, ctx2)
	assert.Equal(t, "REVALIDATED", respHeader(ctx2, header.XCache))
	assert.Equal(t, "public, no-cache", respHeader(ctx2, header.CDNCacheControl))
}

func TestSyncCDC_StaleServeMirrorsSWRWindow(t *testing.T) {
	t.Parallel()
	key := BuildKey(requestInfoFromURL("GET", "http://sync.example.com/swr"), nil)
	h := newSyncHandler(t, benignOrigin, HandlerConfig{})
	defer h.Close(context.Background())
	// Stale by 100s within a 600s SWR window: the mirror is
	// max-age=0, swr=<window remaining>.
	obj := &api.Object{
		Key: key, StatusCode: 200,
		Header: headerMap(header.CacheControl, "max-age=60"),
		Body:   []byte("body"), BodySize: 4,
		StoredAt:             time.Now().Add(-100 * time.Second),
		TTL:                  60 * time.Second,
		StaleWhileRevalidate: 600 * time.Second,
	}
	_ = h.store.Put(context.Background(), key, obj)

	ctx := testCtx("GET", "http://sync.example.com/swr")
	serveRequest(h, ctx)
	require.Equal(t, "STALE", respHeader(ctx, header.XCache))
	cdc := respHeader(ctx, header.CDNCacheControl)
	rem, err := parseCDCSWR(cdc)
	require.NoError(t, err)
	assert.InDelta(t, 560, rem, 3)
	assert.Contains(t, cdc, "public, max-age=0")
	// Cache-Control untouched; Warning present as on any stale serve.
	assert.Equal(t, "max-age=60", respHeader(ctx, header.CacheControl))
	assert.Contains(t, respHeader(ctx, header.Warning), "110")
}

func TestSyncCDC_StaleSoftPurgeMirrorsFromPurgeTime(t *testing.T) {
	t.Parallel()
	key := BuildKey(requestInfoFromURL("GET", "http://sync.example.com/purge"), nil)
	h := newSyncHandler(t, benignOrigin, HandlerConfig{})
	defer h.Close(context.Background())
	obj := &api.Object{
		Key: key, StatusCode: 200,
		Header: headerMap(header.CacheControl, "max-age=3600"),
		Body:   []byte("body"), BodySize: 4,
		StoredAt:             time.Now().Add(-90 * time.Minute),
		TTL:                  3600 * time.Second,
		StaleWhileRevalidate: 600 * time.Second,
	}
	_ = h.store.Put(context.Background(), key, obj)
	_, err := h.SoftPurge(context.Background(), key)
	require.NoError(t, err)

	ctx := testCtx("GET", "http://sync.example.com/purge")
	serveRequest(h, ctx)
	require.Equal(t, "STALE", respHeader(ctx, header.XCache))
	cdc := respHeader(ctx, header.CDNCacheControl)
	rem, perr := parseCDCSWR(cdc)
	require.NoError(t, perr)
	// The soft purge resets StoredAt to the purge time and TTL to 0, so
	// the mirror window is measured from the purge (D2: soft purge rides
	// the SWR machinery).
	assert.InDelta(t, 600, rem, 3)
}

func TestSyncCDC_StaleGraceAndMaxStaleClampStrict(t *testing.T) {
	t.Parallel()
	key := BuildKey(requestInfoFromURL("GET", "http://sync.example.com/grace"), nil)
	h := newSyncHandler(t, func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(503)
	}, HandlerConfig{StayinAlive: true})
	defer h.Close(context.Background())
	// Stale past both the SWR and SIE windows: stayin-alive serves it on
	// origin failure; the synthesis must clamp strict (no swr — outage
	// posture never becomes downstream entry policy).
	obj := &api.Object{
		Key: key, StatusCode: 200,
		Header: headerMap(header.CacheControl, "max-age=60", header.ETag, `"v1"`),
		Body:   []byte("body"), BodySize: 4,
		StoredAt:             time.Now().Add(-30 * time.Minute),
		TTL:                  60 * time.Second,
		StaleWhileRevalidate: 60 * time.Second,
		StaleIfError:         60 * time.Second,
	}
	_ = h.store.Put(context.Background(), key, obj)

	ctx := testCtx("GET", "http://sync.example.com/grace")
	serveRequest(h, ctx)
	require.Equal(t, "STALE", respHeader(ctx, header.XCache))
	assert.Equal(t, "public, max-age=0", respHeader(ctx, header.CDNCacheControl))

	// Client-authorized staleness beyond the object's own window
	// (max-stale) clamps strict as well.
	ctx2 := testCtxWithHeader("GET", "http://sync.example.com/grace", header.CacheControl, "max-stale=3600")
	serveRequest(h, ctx2)
	require.Equal(t, "STALE", respHeader(ctx2, header.XCache))
	assert.Equal(t, "public, max-age=0", respHeader(ctx2, header.CDNCacheControl))
}

func TestSyncCDC_OriginCDCFamilyReplaced(t *testing.T) {
	t.Parallel()
	// The origin's own RFC 9213-family and Surrogate-Control lines are
	// removed under sync (I3): the synthesis is authoritative, and a
	// downstream cache must never see both the origin's line and
	// bouine's.
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=120")
		ctx.Response.Header.Set(header.CDNCacheControl, "max-age=9999")
		ctx.Response.Header.Set(header.CloudflareCDNCacheControl, "max-age=8888")
		ctx.Response.Header.Set(header.SurrogateControl, "max-age=7777")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}
	h := newSyncHandler(t, upstream, HandlerConfig{})
	defer h.Close(context.Background())

	ctx := testCtx("GET", "http://sync.example.com/replace")
	serveRequest(h, ctx)
	// The only CDN-Cache-Control line is the synthesized one: the
	// synthesized form always leads with public; the origin's raw
	// line ("max-age=9999", no public) must not survive. The numeric
	// value is legitimately derived from the origin's CDC (RFC 9213:
	// the targeted field replaces Cache-Control for shared caches), so
	// the discriminator is the synthesized shape, not the digits.
	require.Equal(t, "public, max-age=9999", respHeader(ctx, header.CDNCacheControl))
	assert.Empty(t, respHeader(ctx, header.CloudflareCDNCacheControl))
	assert.Empty(t, respHeader(ctx, header.SurrogateControl))
	// And the hit serve stays free of the replaced lines too.
	ctx2 := testCtx("GET", "http://sync.example.com/replace")
	serveRequest(h, ctx2)
	assert.Empty(t, respHeader(ctx2, header.CloudflareCDNCacheControl))
	assert.Empty(t, respHeader(ctx2, header.SurrogateControl))
	assert.Contains(t, respHeader(ctx2, header.CDNCacheControl), "public")
}

func TestSyncCDC_FillMinRule(t *testing.T) {
	t.Parallel()
	t.Run("override longer than origin caps at origin", func(t *testing.T) {
		t.Parallel()
		h := newSyncHandler(t, func(ctx *fasthttp.RequestCtx) {
			ctx.Response.Header.Set(header.CacheControl, "max-age=30")
			ctx.SetStatusCode(200)
			_, _ = ctx.Write([]byte("body"))
		}, HandlerConfig{OverrideTTL: 2 * time.Hour})
		defer h.Close(context.Background())
		ctx := testCtx("GET", "http://sync.example.com/decouple")
		serveRequest(h, ctx)
		rem, err := parseCDCMaxAge(respHeader(ctx, header.CDNCacheControl))
		require.NoError(t, err)
		assert.InDelta(t, 30, rem, 2)
		// ADR-0011's decoupling case preserved: the object is stored with
		// bouine's longer TTL.
		key := BuildKey(requestInfoFromCtx(ctx), nil)
		obj, _, _ := h.store.Get(context.Background(), key)
		require.NotNil(t, obj)
		assert.Equal(t, 2*time.Hour, obj.TTL)
	})
	t.Run("override tighter than origin caps at override", func(t *testing.T) {
		t.Parallel()
		h := newSyncHandler(t, func(ctx *fasthttp.RequestCtx) {
			ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
			ctx.SetStatusCode(200)
			_, _ = ctx.Write([]byte("body"))
		}, HandlerConfig{OverrideTTL: 60 * time.Second})
		defer h.Close(context.Background())
		ctx := testCtx("GET", "http://sync.example.com/tight")
		serveRequest(h, ctx)
		rem, err := parseCDCMaxAge(respHeader(ctx, header.CDNCacheControl))
		require.NoError(t, err)
		assert.InDelta(t, 60, rem, 2)
	})
	t.Run("qualitative directives carried", func(t *testing.T) {
		t.Parallel()
		h := newSyncHandler(t, func(ctx *fasthttp.RequestCtx) {
			ctx.Response.Header.Set(header.CacheControl, "max-age=60, must-revalidate, proxy-revalidate")
			ctx.SetStatusCode(200)
			_, _ = ctx.Write([]byte("body"))
		}, HandlerConfig{})
		defer h.Close(context.Background())
		ctx := testCtx("GET", "http://sync.example.com/qual")
		serveRequest(h, ctx)
		assert.Contains(t, respHeader(ctx, header.CDNCacheControl), "must-revalidate")
		assert.Contains(t, respHeader(ctx, header.CDNCacheControl), "proxy-revalidate")
	})
}

func TestSyncCDC_UncacheableToSharedNeverSynthesizes(t *testing.T) {
	t.Parallel()
	h := newSyncHandler(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "private, max-age=120")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}, HandlerConfig{})
	defer h.Close(context.Background())
	ctx := testCtx("GET", "http://sync.example.com/private")
	serveRequest(h, ctx)
	assert.Empty(t, respHeader(ctx, header.CDNCacheControl))
	assert.Equal(t, "private, max-age=120", respHeader(ctx, header.CacheControl))
}

func TestSyncCDC_304CarriesCapAndWarning(t *testing.T) {
	t.Parallel()
	// A conditional against a stale-within-SWR object: the 304 carries
	// the same mirror cap as the 200 serve plus Warning: 110 (I4).
	key := BuildKey(requestInfoFromURL("GET", "http://sync.example.com/cond"), nil)
	h := newSyncHandler(t, benignOrigin, HandlerConfig{})
	defer h.Close(context.Background())
	obj := &api.Object{
		Key: key, StatusCode: 200,
		Header: headerMap(header.CacheControl, "max-age=60", header.ETag, `"v1"`),
		Body:   []byte("body"), BodySize: 4,
		StoredAt:             time.Now().Add(-100 * time.Second),
		TTL:                  60 * time.Second,
		StaleWhileRevalidate: 600 * time.Second,
		ETag:                 `"v1"`,
	}
	_ = h.store.Put(context.Background(), key, obj)

	ctx := testCtxWithHeader("GET", "http://sync.example.com/cond", header.IfNoneMatch, `"v1"`)
	serveRequest(h, ctx)
	require.Equal(t, 304, respCode(ctx))
	rem, err := parseCDCSWR(respHeader(ctx, header.CDNCacheControl))
	require.NoError(t, err)
	assert.InDelta(t, 560, rem, 3)
	assert.Contains(t, respHeader(ctx, header.Warning), "110")
}

func TestSyncCDC_206CarriesCap(t *testing.T) {
	t.Parallel()
	key := BuildKey(requestInfoFromURL("GET", "http://sync.example.com/range"), nil)
	h := newSyncHandler(t, benignOrigin, HandlerConfig{})
	defer h.Close(context.Background())
	obj := &api.Object{
		Key: key, StatusCode: 200,
		Header: headerMap(header.CacheControl, "max-age=120", header.ContentType, "text/plain"),
		Body:   []byte("Hello, World!"), BodySize: 13,
		StoredAt: time.Now(), TTL: 120 * time.Second,
	}
	_ = h.store.Put(context.Background(), key, obj)

	ctx := testCtxWithHeader("GET", "http://sync.example.com/range", header.Range, "bytes=0-4")
	serveRequest(h, ctx)
	require.Equal(t, 206, respCode(ctx))
	rem, err := parseCDCMaxAge(respHeader(ctx, header.CDNCacheControl))
	require.NoError(t, err)
	assert.InDelta(t, 120, rem, 2)
}

func TestSyncCDC_PassthroughUnchanged(t *testing.T) {
	t.Parallel()
	// Default mode: byte-for-byte passthrough, no synthesized CDC, on
	// every serve class (ADR-0011; cache-tests conformance untouched).
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=120")
		ctx.Response.Header.Set(header.CDNCacheControl, "max-age=9999")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store:      store,
	})
	defer h.Close(context.Background())

	ctx := testCtx("GET", "http://plain.example.com/pt")
	serveRequest(h, ctx)
	assert.Equal(t, "max-age=9999", respHeader(ctx, header.CDNCacheControl))
	ctx2 := testCtx("GET", "http://plain.example.com/pt")
	serveRequest(h, ctx2)
	assert.Equal(t, "HIT", respHeader(ctx2, header.XCache))
	assert.Equal(t, "max-age=9999", respHeader(ctx2, header.CDNCacheControl))
}

// TestSyncCDC_I8_Stale304TriggersBgRevalidate pins the ADR-0063 I8 fix:
// a conditional request that matches a stale-within-SWR object's
// validator previously answered 304 and returned before the 200-path
// trigger, freezing content for the full SWR window (both modes). The
// 304 path must fire the same trigger under the same triple gate.
func TestSyncCDC_I8_Stale304TriggersBgRevalidate(t *testing.T) {
	for _, mode := range []string{"passthrough", "sync"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				reval := make(chan struct{}, 1)
				origin := func(ctx *fasthttp.RequestCtx) {
					if string(ctx.Request.Header.Peek(header.IfNoneMatch)) != "" {
						select {
						case reval <- struct{}{}:
						default:
						}
						ctx.Response.Header.Set(header.CacheControl, "max-age=60")
						ctx.SetStatusCode(304)
						return
					}
					ctx.Response.Header.Set(header.CacheControl, "max-age=60")
					ctx.Response.Header.Set(header.ETag, `"v1"`)
					ctx.SetStatusCode(200)
					_, _ = ctx.Write([]byte("body"))
				}
				h := newSyncHandler(t, origin, HandlerConfig{})
				if mode == "passthrough" {
					h.syncCDC = false
				}
				defer h.store.Close(context.Background())
				defer h.Close(context.Background())

				key := BuildKey(requestInfoFromURL("GET", "http://i8.example.com/x"), nil)
				obj := &api.Object{
					Key: key, StatusCode: 200,
					Header: headerMap(header.CacheControl, "max-age=60", header.ETag, `"v1"`),
					Body:   []byte("body"), BodySize: 4,
					StoredAt:             time.Now().Add(-70 * time.Second),
					TTL:                  60 * time.Second,
					StaleWhileRevalidate: 60 * time.Second,
					ETag:                 `"v1"`,
				}
				_ = h.store.Put(context.Background(), key, obj)

				ctx := testCtxWithHeader("GET", "http://i8.example.com/x", header.IfNoneMatch, `"v1"`)
				h.ServeRequest(ctx)
				require.Equal(t, 304, respCode(ctx))
				if mode == "sync" {
					// The stale-sourced 304 carries the mirror cap.
					rem, err := parseCDCSWR(respHeader(ctx, header.CDNCacheControl))
					require.NoError(t, err)
					assert.InDelta(t, 50, rem, 3)
				}

				synctest.Sleep(200 * time.Millisecond)
				select {
				case <-reval:
				default:
					t.Fatal("stale conditional 304 did not trigger background revalidation")
				}

				// The refreshed object replaced the stale one.
				updated, _, _ := h.store.Get(context.Background(), key)
				require.NotNil(t, updated)
				assert.Equal(t, 60*time.Second, updated.TTL)
			})
		})
	}
}

// TestSyncCDC_I8_Fresh304TriggersNothing: a conditional 304 against a
// fresh object must not spawn a background revalidation.
func TestSyncCDC_I8_Fresh304TriggersNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reval := make(chan struct{}, 1)
		origin := func(ctx *fasthttp.RequestCtx) {
			select {
			case reval <- struct{}{}:
			default:
			}
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			ctx.Response.Header.Set(header.ETag, `"v1"`)
			ctx.SetStatusCode(200)
			_, _ = ctx.Write([]byte("body"))
		}
		h := newSyncHandler(t, origin, HandlerConfig{})
		defer h.store.Close(context.Background())
		defer h.Close(context.Background())

		key := BuildKey(requestInfoFromURL("GET", "http://i8f.example.com/x"), nil)
		obj := &api.Object{
			Key: key, StatusCode: 200,
			Header: headerMap(header.CacheControl, "max-age=60", header.ETag, `"v1"`),
			Body:   []byte("body"), BodySize: 4,
			StoredAt: time.Now(), TTL: 60 * time.Second,
			ETag: `"v1"`,
		}
		_ = h.store.Put(context.Background(), key, obj)

		ctx := testCtxWithHeader("GET", "http://i8f.example.com/x", header.IfNoneMatch, `"v1"`)
		h.ServeRequest(ctx)
		require.Equal(t, 304, respCode(ctx))
		synctest.Sleep(200 * time.Millisecond)
		select {
		case <-reval:
			t.Fatal("fresh conditional 304 must not trigger background revalidation")
		default:
		}
	})
}

// TestSyncCDC_FastPathHit: the h1 fast path appends the synthesized CDC
// on hits (zero-alloc) and skips the origin's CDC-family lines from the
// static head.
func TestSyncCDC_FastPathHit(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)
	fp.syncCDC = true

	key := buildKeyFromRaw(&api.RawRequest{Method: "GET", Path: "/", Host: "fp.example.com", Scheme: "http"}, nil)
	obj := &api.Object{
		Key: key, StatusCode: 200,
		Header: headerMap(
			header.ContentType, "text/html",
			header.CacheControl, "max-age=120",
			header.CDNCacheControl, "max-age=9999",
		),
		Body: []byte("Hello, World!"), BodySize: 13,
		StoredAt: time.Now(), TTL: 600 * time.Second,
	}
	require.NoError(t, store.Put(context.Background(), key, obj))

	req := &api.RawRequest{Method: "GET", Path: "/", Host: "fp.example.com", Scheme: "http", HTTPVersion: "HTTP/1.1"}
	now := time.Now()
	resp, ok := fp.TryHit(req, now)
	require.True(t, ok)
	defer fp.Release(resp)

	head := string(resp.HeaderBuf)
	assert.Contains(t, head, "X-Cache: HIT")
	assert.Contains(t, head, "Cdn-Cache-Control: public, max-age=59")
	assert.NotContains(t, head, "9999")
	assert.NotContains(t, head, "max-age=9999")
	assert.Contains(t, head, "Cache-Control: max-age=120")
}

func TestSyncCDC_PeerWardIsolation(t *testing.T) {
	t.Parallel()
	// The synthesized CDC never enters the stored object's headers, so
	// the peer-fetch wire codec (which serializes the stored object)
	// cannot carry it peer-ward, and a peer's CDC-family lines cannot
	// shrink the receiving node's TTL: the codec path never parses
	// Cache-Control into buildObject (I5; verified against the
	// owner-first binary-codec transport).
	key := BuildKey(requestInfoFromURL("GET", "http://peer.example.com/x"), nil)
	h := newSyncHandler(t, benignOrigin, HandlerConfig{})
	defer h.Close(context.Background())
	obj := &api.Object{
		Key: key, StatusCode: 200,
		Header: headerMap(header.CacheControl, "max-age=120", header.CDNCacheControl, "max-age=5"),
		Body:   []byte("body"), BodySize: 4,
		StoredAt: time.Now(), TTL: 120 * time.Second,
	}
	_ = h.store.Put(context.Background(), key, obj)

	got, _, err := h.store.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, 120*time.Second, got.TTL, "peer-sourced CDC line must not shrink the TTL")
	assert.NotContains(t, string(got.Header.Get(header.CDNCacheControl)), "public")
}

// TestSyncCDC_PeerResponseCDCIgnored pins the P4 contract end-to-end:
// a peer-served object carrying CDC-family headers (as origin CDC lines
// stored by a peer in passthrough mode) is served, never re-stored, and
// never parsed into a local TTL — the peer transport is the binary
// object codec, not an HTTP fill.
func TestSyncCDC_PeerResponseCDCIgnored(t *testing.T) {
	t.Parallel()
	key := BuildKey(requestInfoFromURL("GET", "http://peer2.example.com/x"), nil)
	h := newSyncHandler(t, benignOrigin, HandlerConfig{})
	defer h.Close(context.Background())
	peerObj := &api.Object{
		Key: key, StatusCode: 200,
		Header: headerMap(header.CacheControl, "max-age=3600", header.CDNCacheControl, "max-age=5"),
		Body:   []byte("peerbody"), BodySize: 8,
		StoredAt: time.Now(), TTL: 3600 * time.Second,
	}
	// A peer object served to this handler's clients (handleCacheMiss's
	// owner-first branch) is stored by no local path.
	ctx := testCtx("GET", "http://peer2.example.com/x")
	h.serveObject(ctx, peerObj, time.Now(), cacheHit, api.SourcePeer)
	assert.Equal(t, 200, respCode(ctx))
	// Under sync the peer serve is capped from the peer object's own
	// clock (the content's actual clock) — never from a CDC line.
	rem, err := parseCDCMaxAge(respHeader(ctx, header.CDNCacheControl))
	require.NoError(t, err)
	assert.InDelta(t, 3600, rem, 3)
}

// parseCDCMaxAge extracts the max-age value from a synthesized CDC
// directive list.
func parseCDCMaxAge(cdc string) (int, error) {
	var v int
	if _, err := fmt.Sscanf(cdc, "public, max-age=%d", &v); err != nil {
		return 0, fmt.Errorf("parse max-age from %q: %w", cdc, err)
	}
	return v, nil
}

// parseCDCSWR extracts the stale-while-revalidate value from a mirror
// line ("public, max-age=0, stale-while-revalidate=N").
func parseCDCSWR(cdc string) (int, error) {
	var v int
	if _, err := fmt.Sscanf(cdc, "public, max-age=0, stale-while-revalidate=%d", &v); err != nil {
		return 0, fmt.Errorf("parse swr from %q: %w", cdc, err)
	}
	return v, nil
}
