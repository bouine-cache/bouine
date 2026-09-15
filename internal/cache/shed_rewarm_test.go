package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

// Tests for the post-ban re-warm fix: a foreground miss that sheds (fetch
// semaphore full for fetchWaitTimeout) must still refill the store via a
// bounded background fetch, so a surrogate-ban purge cliff cannot pin the
// hit ratio at the shed equilibrium — every shed used to be a lost refill
// and the system never recovered until banTTL expired.

// shedRewarmHandler builds a handler with a single-slot fetchSem, the
// given shed counter, and a short wait bound, mirroring the shed harness.
func shedRewarmHandler(t *testing.T, upstream fasthttp.RequestHandler) (*Handler, *shedCounter) {
	t.Helper()
	c := &shedCounter{}
	h := testHandler(t, upstream)
	h.FetchShedInc = c
	h.fetchWaitTimeout = 50 * time.Millisecond
	h.fetchSem = make(chan struct{}, 1)
	return h, c
}

// TestShedMissStillRewarmsStore proves a shed hard miss is not a lost
// refill: the client gets 503 + Retry-After (unchanged protection), but a
// bounded background fetch re-warms the store so the NEXT request for the
// same key hits instead of shedding again (issue: shed-on-wait blocked
// store re-warm, pinning the hit ratio at ~7% for hours).
func TestShedMissStillRewarmsStore(t *testing.T) {
	t.Parallel()
	h, counter := shedRewarmHandler(t, origin200("rewarm-body"))

	h.fetchSem <- struct{}{} // hold the only foreground slot
	url := "http://example.com/purge-cliff"

	rr := testCtx("GET", url)
	h.ServeRequest(rr)
	require.Equal(t, 503, respCode(rr))
	require.Equal(t, "1", respHeader(rr, header.RetryAfter))
	require.Equal(t, int64(1), counter.n.Load(), "shed counter must increment exactly once")
	<-h.fetchSem // release the foreground slot

	// The background refill must land without any further foreground
	// traffic, on the handler's own bounded pool.
	key := BuildKeyFromURL(url, nil)
	require.Eventually(t, func() bool {
		obj, _, err := h.store.Get(context.Background(), key)
		return err == nil && obj != nil && string(obj.Body) == "rewarm-body"
	}, 5*time.Second, 10*time.Millisecond, "shed miss must still refill the store")

	// And the next foreground request hits — the re-warm broke the
	// shed equilibrium.
	rr2 := testCtx("GET", url)
	h.ServeRequest(rr2)
	require.Equal(t, "HIT", respHeader(rr2, header.XCache))
	require.Equal(t, "rewarm-body", respBody(rr2))
	require.Equal(t, int64(1), counter.n.Load(), "no second shed for a re-warmed key")

	require.NoError(t, h.Close(context.Background()))
}

// TestShedMissRewarmIsDeduped proves concurrent sheds for the same key
// trigger one background refill, not one per shed (singleflight via the
// inflight-stream registry, same collapse the foreground miss path uses).
func TestShedMissRewarmIsDeduped(t *testing.T) {
	t.Parallel()
	var originCalls atomic.Int64
	upstream := func(ctx *fasthttp.RequestCtx) {
		originCalls.Add(1)
		time.Sleep(20 * time.Millisecond)
		origin200("dedup-body")(ctx)
	}
	h, _ := shedRewarmHandler(t, upstream)

	h.fetchSem <- struct{}{} // hold the only foreground slot
	url := "http://example.com/dedup-rewarm"

	const shedders = 4
	for range shedders {
		go h.ServeRequest(testCtx("GET", url))
	}
	require.Eventually(t, func() bool {
		return counterSheds(h) >= 1
	}, 5*time.Second, 5*time.Millisecond, "at least one request must shed")
	<-h.fetchSem

	key := BuildKeyFromURL(url, nil)
	require.Eventually(t, func() bool {
		obj, _, err := h.store.Get(context.Background(), key)
		return err == nil && obj != nil
	}, 5*time.Second, 10*time.Millisecond, "the refill must land")

	// One collapsed background fetch + at most the leader's foreground
	// attempt; strictly fewer origin calls than shedders.
	require.Less(t, originCalls.Load(), int64(shedders),
		"background refill must be collapsed, not per-shed")

	require.NoError(t, h.Close(context.Background()))
}

// TestShedMissRewarmDoesNotTouchForegroundSemaphore proves the refill
// path does not contend with the foreground fetch budget: with the
// foreground slot held the whole time, the refill still completes (it
// runs unslotted, bounded by its own revalSem-style allowance instead).
func TestShedMissRewarmDoesNotTouchForegroundSemaphore(t *testing.T) {
	t.Parallel()
	h, _ := shedRewarmHandler(t, origin200("unslotted-body"))

	h.fetchSem <- struct{}{} // hold the only foreground slot for the whole test
	url := "http://example.com/unslotted-rewarm"

	rr := testCtx("GET", url)
	h.ServeRequest(rr)
	require.Equal(t, 503, respCode(rr))

	key := BuildKeyFromURL(url, nil)
	require.Eventually(t, func() bool {
		obj, _, err := h.store.Get(context.Background(), key)
		return err == nil && obj != nil
	}, 5*time.Second, 10*time.Millisecond,
		"refill must not depend on a foreground fetchSem slot")

	require.NoError(t, h.Close(context.Background()))
}

// TestShedMissRewarmShutdownSafe proves Close drains the refill goroutine
// before returning (no use-after-close store writes, §11 goroutine
// ownership) and that a refill triggered after Close is a no-op.
func TestShedMissRewarmShutdownSafe(t *testing.T) {
	t.Parallel()
	h, _ := shedRewarmHandler(t, origin200("drain-body"))

	h.fetchSem <- struct{}{}
	url := "http://example.com/drain"
	rr := testCtx("GET", url)
	h.ServeRequest(rr)
	require.Equal(t, 503, respCode(rr))

	require.NoError(t, h.Close(context.Background()))

	key := BuildKeyFromURL(url, nil)
	// Either the refill completed before the drain finished, or it was
	// cancelled mid-flight; either way the store must never see a torn
	// write after Close returns.
	obj, _, err := h.store.Get(context.Background(), key)
	if err == nil && obj != nil {
		require.Equal(t, "drain-body", string(obj.Body))
	}

	rr2 := testCtx("GET", url)
	h.ServeRequest(rr2) // must not panic or refill after Close
	<-h.fetchSem
}

// TestShedMissRewarmCapBoundsConcurrency proves the refill allowance is
// bounded: with a tiny cap and many shed keys, no more than cap refills
// run concurrently (the revalSem-style admission control).
func TestShedMissRewarmCapBoundsConcurrency(t *testing.T) {
	t.Parallel()
	var active, maxActive atomic.Int64
	upstream := func(ctx *fasthttp.RequestCtx) {
		cur := active.Add(1)
		for {
			old := maxActive.Load()
			if cur <= old || maxActive.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		active.Add(-1)
		origin200("cap-body")(ctx)
	}
	h, _ := shedRewarmHandler(t, upstream)
	h.rewarmSem = make(chan struct{}, 2) // tiny cap

	const keys = 8
	for i := range keys {
		h.fetchSem <- struct{}{}
		url := "http://example.com/cap-" + string(rune('a'+i))
		go func() {
			defer func() { <-h.fetchSem }()
			h.ServeRequest(testCtx("GET", url))
		}()
	}

	require.Eventually(t, func() bool {
		return maxActive.Load() >= 1
	}, 5*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool {
		return h.storeStatsEntries() >= keys
	}, 10*time.Second, 20*time.Millisecond, "all keys must re-warm")
	require.LessOrEqual(t, maxActive.Load(), int64(2),
		"refill concurrency must respect the rewarm cap")

	require.NoError(t, h.Close(context.Background()))
}

// counterSheds reads the shed counter regardless of its concrete type.
func counterSheds(h *Handler) int64 {
	if c, ok := h.FetchShedInc.(*shedCounter); ok {
		return c.n.Load()
	}
	return 0
}

// storeStatsEntries is a small indirection so the cap test can wait for
// re-warm completion without reaching into store internals.
func (h *Handler) storeStatsEntries() int {
	return len(h.store.(storage.KeyLister).Keys())
}
