package cache

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/origin"
	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
)

// stayinE2EHarness wires handler + hot store + real origin pool exactly
// like the engine builder does (buildStore arms MayReap from the pools
// map): a Handler with stayin_alive on, a HotStore whose reaper gate is
// the real decider shape, and a PoolFastClient whose passive health
// ejects the only target on connection errors. This closes the loop
// between the three layers the piecemeal unit tests cover separately
// (issue #936): fill -> outage -> ejection -> reaper pass -> stale serve
// still works; recovery -> reaper pass -> entry collected.
type stayinE2EHarness struct {
	h     *Handler
	store *storage.HotStore
	pool  *origin.Pool
	ln    net.Listener
	srv   *fasthttp.Server
	fails *atomic.Bool
}

func newStayinE2EHarness(t *testing.T) *stayinE2EHarness {
	t.Helper()
	fails := &atomic.Bool{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	handler := func(ctx *fasthttp.RequestCtx) {
		if fails.Load() {
			ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
			_, _ = ctx.WriteString("down")
			return
		}
		ctx.Response.Header.Set("Cache-Control", "max-age=1, stale-while-revalidate=3600, stale-if-error=3600")
		ctx.SetStatusCode(fasthttp.StatusOK)
		_, _ = ctx.WriteString("e2e-body")
	}
	srv := &fasthttp.Server{Handler: handler}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Shutdown() })
	t.Cleanup(func() { _ = ln.Close() })

	pool, err := origin.NewPool(origin.PoolConfig{
		Name:           "e2e",
		Targets:        []string{ln.Addr().String()},
		Consecutive5xx: 1,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(context.Background()) })

	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:       1 << 20,
		NumShards:      2,
		ReaperInterval: -1, // background reaper off; tests drive reapExpired directly
		MayReap: func(obj *api.Object) bool {
			if !obj.KeepGrace {
				return true
			}
			return pool.HasHealthyTarget()
		},
	})
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	h := NewHandler(HandlerConfig{
		Upstream:    pool.FastHandler(0),
		FastClient:  pool.FastClient(),
		Store:       store,
		StayinAlive: true,
		PoolName:    "e2e",
	})
	return &stayinE2EHarness{h: h, store: store, pool: pool, ln: ln, srv: srv, fails: fails}
}

// TestStayinAlive_E2E_OutageBeyondHorizon pins the full ADR-0051
// contract end to end (issue #936): fill while healthy, flip the origin
// to 503, eject the only target via the real passive-health path, run a
// reaper pass past TTL+SWR+SIE, and the request must still get the stale
// body with X-Cache: STALE; after MarkHealthy + another pass the expired
// graced entry is collected and the next request misses.
func TestStayinAlive_E2E_OutageBeyondHorizon(t *testing.T) {
	t.Parallel()
	hh := newStayinE2EHarness(t)
	url := "/e2e"

	// 1. Fill while healthy.
	rr := testCtx("GET", url)
	hh.h.ServeRequest(rr)
	require.Equal(t, 200, respCode(rr))
	require.Equal(t, "e2e-body", respBody(rr))
	time.Sleep(50 * time.Millisecond) // let the tee body-stream writer finish and store

	key := hh.h.buildKey(rr)
	obj, _, err := hh.store.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, obj)
	require.True(t, obj.KeepGrace, "stayin_alive fill must stamp KeepGrace")
	require.Equal(t, "e2e", obj.Pool)

	// Age the entry past TTL+SWR+SIE with no windows configured.
	stale := obj.CloneForRefresh()
	stale.StoredAt = time.Now().Add(-(stale.TTL + stale.StaleWhileRevalidate + stale.StaleIfError) - time.Minute)
	require.NoError(t, hh.store.Put(context.Background(), key, stale))

	// 2. Origin down: 503s. The stale object serves (stayin_alive), and
	// the 503 fetch ejects the only target — connection errors count
	// immediately (no success window), so closing the listener is the
	// reliable ejection trigger with Consecutive5xx: 1.
	hh.fails.Store(true)
	_ = hh.ln.Close() // subsequent dials fail -> recordPassiveError -> eject
	_ = hh.srv.Shutdown()
	time.Sleep(20 * time.Millisecond) // let the in-flight listener state settle
	rr = testCtx("GET", url)
	hh.h.ServeRequest(rr)
	time.Sleep(20 * time.Millisecond) // dial failure is recorded synchronously, but keep the assertion deterministic
	require.Equal(t, 200, respCode(rr), "stayin_alive must serve stale on error")
	require.Equal(t, "STALE", respHeader(rr, "X-Cache"))
	require.False(t, hh.pool.HasHealthyTarget(), "target must be ejected after the failed fetch")

	// 3. Reaper pass past the horizon: the graced entry is HELD.
	hh.store.ReapForTest(time.Now())
	got, _, _ := hh.store.Get(context.Background(), key)
	require.NotNil(t, got, "expired graced entry must survive the reaper while the pool is unhealthy")
	require.Positive(t, hh.store.Stats().ReaperGraceHolds, "grace hold must be counted")

	// 4. A later request still serves stale (the entry is still there).
	rr = testCtx("GET", url)
	hh.h.ServeRequest(rr)
	require.Equal(t, 200, respCode(rr))
	require.Equal(t, "STALE", respHeader(rr, "X-Cache"))
	require.Equal(t, "e2e-body", respBody(rr))

	// 5. Recovery: rebind the origin on the same port, restore the
	// target, reaper pass -> entry collected.
	hh.fails.Store(false)
	ln2, err := net.Listen("tcp", hh.ln.Addr().String())
	require.NoError(t, err)
	srv2 := &fasthttp.Server{Handler: func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("Cache-Control", "max-age=1, stale-while-revalidate=3600, stale-if-error=3600")
		ctx.SetStatusCode(fasthttp.StatusOK)
		_, _ = ctx.WriteString("e2e-body")
	}}
	go func() { _ = srv2.Serve(ln2) }()
	t.Cleanup(func() { _ = srv2.Shutdown() })
	hh.pool.MarkHealthy(hh.ln.Addr().String())
	require.True(t, hh.pool.HasHealthyTarget())
	hh.store.ReapForTest(time.Now())
	got, _, _ = hh.store.Get(context.Background(), key)
	assert.Nil(t, got, "expired graced entry must be reaped once the pool is healthy again")

	// 6. Next request refills from origin (miss path).
	rr = testCtx("GET", url)
	hh.h.ServeRequest(rr)
	require.Equal(t, 200, respCode(rr))
	require.Equal(t, "e2e-body", respBody(rr))
}

// TestStayinAlive_E2E_NoHealthSignalReaped pins the negative contract
// (issue #936, pairs with #933's warning): with a pool that has NO
// health signal, HasHealthyTarget stays true, so even a graced entry is
// reaped on the normal schedule once expired.
func TestStayinAlive_E2E_NoHealthSignalReaped(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	srv := &fasthttp.Server{Handler: func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("Cache-Control", "max-age=1, stale-while-revalidate=3600, stale-if-error=3600")
		ctx.SetStatusCode(fasthttp.StatusOK)
		_, _ = ctx.WriteString("nh-body")
	}}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Shutdown() })

	// No Consecutive5xx: passive health disabled, target never ejects.
	pool, err := origin.NewPool(origin.PoolConfig{
		Name:    "nohealth",
		Targets: []string{ln.Addr().String()},
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(context.Background()) })

	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:       1 << 20,
		NumShards:      2,
		ReaperInterval: -1,
		MayReap: func(obj *api.Object) bool {
			if !obj.KeepGrace {
				return true
			}
			return pool.HasHealthyTarget()
		},
	})
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	h := NewHandler(HandlerConfig{
		Upstream:    pool.FastHandler(0),
		FastClient:  pool.FastClient(),
		Store:       store,
		StayinAlive: true,
		PoolName:    "nohealth",
	})

	url := "/nosignal"
	rr := testCtx("GET", url)
	h.ServeRequest(rr)
	require.Equal(t, 200, respCode(rr))
	time.Sleep(50 * time.Millisecond) // let the tee body-stream writer finish and store

	key := h.buildKey(rr)
	obj, _, err := store.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, obj)
	require.True(t, obj.KeepGrace)

	stale := obj.CloneForRefresh()
	stale.StoredAt = time.Now().Add(-(stale.TTL + stale.StaleWhileRevalidate + stale.StaleIfError) - time.Minute)
	require.NoError(t, store.Put(context.Background(), key, stale))

	require.True(t, pool.HasHealthyTarget(), "pool without health signal never ejects")
	store.ReapForTest(time.Now())
	got, _, _ := store.Get(context.Background(), key)
	assert.Nil(t, got, "without a health signal the expired graced entry is reaped on schedule")
	assert.Zero(t, store.Stats().ReaperGraceHolds)
}
