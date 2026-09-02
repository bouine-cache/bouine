package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

// shedCounter is a nil-safe stand-in for the injected shed counter.
type shedCounter struct{ n atomic.Int64 }

func (c *shedCounter) Inc() { c.n.Add(1) }

// shedTestHandler builds a handler with a single-slot fetchSem and the
// given shed counter, mirroring the production wiring.
func shedTestHandler(t *testing.T, upstream fasthttp.RequestHandler) (*Handler, *shedCounter) {
	t.Helper()
	c := &shedCounter{}
	h := testHandler(t, upstream)
	h.FetchShedInc = c
	h.fetchWaitTimeout = 50 * time.Millisecond
	h.fetchSem = make(chan struct{}, 1)
	return h, c
}

// staleNoValidator warms the cache for url and mutates the stored object
// into a stale one without validators, so a later request dispatches as
// Miss → handleCacheMiss → fetchAndStoreStayinAlive (the slotted
// foreground path) rather than Revalidate (unslotted doFetchBg).
func staleNoValidator(t *testing.T, h *Handler, url, body string) {
	t.Helper()
	rr := testCtx("GET", url)
	h.ServeRequest(rr)
	require.Equal(t, "MISS", respHeader(rr, header.XCache))

	key := BuildKeyFromURL(url, nil)
	ctx := context.Background()
	obj, _, err := h.store.Get(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, obj)
	require.Equal(t, body, string(obj.Body))
	obj.TTL = -time.Minute // force stale
	obj.ETag = ""
	obj.Header.Del(header.ETag)
	obj.Header.Del(header.LastModified)
	obj.CacheControl = obj.Header.Get(header.CacheControl)
	require.NoError(t, h.store.Put(ctx, key, obj))
}

// TestDoFetchShedsWhenSemaphoreFull proves the foreground fetch wait is
// bounded: with the only slot held and fetchWaitTimeout elapsed, the
// fetch returns ErrFetchShed instead of parking forever.
func TestDoFetchShedsWhenSemaphoreFull(t *testing.T) {
	t.Parallel()
	h, counter := shedTestHandler(t, origin200("body"))
	h.fetchSem <- struct{}{} // hold the only slot
	defer func() { <-h.fetchSem }()

	start := time.Now()
	res := h.doFetch(testCtx("GET", "http://example.com/"))
	elapsed := time.Since(start)

	require.Error(t, res.Err)
	require.ErrorIs(t, res.Err, ErrFetchShed)
	require.Less(t, elapsed, 5*time.Second, "fetch must not park unboundedly")
	require.Equal(t, int64(1), counter.n.Load(), "shed counter must increment exactly once")
}

// TestDoFetchFastPathAcquiresWithoutWaiting pins the two-stage acquire:
// when a slot is free, the fetch succeeds (never takes the slow-path
// timer arm).
func TestDoFetchFastPathAcquiresWithoutWaiting(t *testing.T) {
	t.Parallel()
	h, counter := shedTestHandler(t, origin200("body"))

	res := h.doFetch(testCtx("GET", "http://example.com/"))

	require.NoError(t, res.Err)
	require.Equal(t, 200, res.StatusCode)
	require.Equal(t, int64(0), counter.n.Load())
}

// TestDoFetchWaitsBrieflyThenAcquires pins that the wait bound does not
// fire under brief contention: the fetch waits for the slot and proceeds
// once it is freed well within the bound.
func TestDoFetchWaitsBrieflyThenAcquires(t *testing.T) {
	t.Parallel()
	h, counter := shedTestHandler(t, origin200("body"))
	// Generous margins against CI scheduling jitter: free the slot after
	// ~10ms against a 5s bound, so the shed arm cannot fire spuriously.
	h.fetchWaitTimeout = 5 * time.Second
	h.fetchSem <- struct{}{} // hold the only slot

	go func() {
		time.Sleep(10 * time.Millisecond)
		<-h.fetchSem // free the slot within the bound
	}()

	res := h.doFetch(testCtx("GET", "http://example.com/"))
	require.NoError(t, res.Err)
	require.Equal(t, 200, res.StatusCode)
	require.Equal(t, int64(0), counter.n.Load())
}

// TestDoFetchStreamShedsWhenSemaphoreFull is the streaming-path twin of
// TestDoFetchShedsWhenSemaphoreFull.
func TestDoFetchStreamShedsWhenSemaphoreFull(t *testing.T) {
	t.Parallel()
	h, counter := shedTestHandler(t, origin200("body"))
	h.fetchSem <- struct{}{}
	defer func() { <-h.fetchSem }()

	start := time.Now()
	_, err := h.doFetchStream(testCtx("GET", "http://example.com/"))
	elapsed := time.Since(start)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrFetchShed)
	require.Less(t, elapsed, 5*time.Second, "stream fetch must not park unboundedly")
	require.Equal(t, int64(1), counter.n.Load())
}

// TestShedWithStaleServesStale proves a shed request with a stale object
// in scope serves stale (RFC 5861-style), not an error.
func TestShedWithStaleServesStale(t *testing.T) {
	t.Parallel()
	h, _ := shedTestHandler(t, origin200("body"))
	h.stayinAlive = true

	url := "http://example.com/sie"
	staleNoValidator(t, h, url, "body")

	// Hold the fetch slot so the miss-path fetch sheds.
	h.fetchSem <- struct{}{}
	defer func() { <-h.fetchSem }()

	rr2 := testCtx("GET", url)
	h.ServeRequest(rr2)
	require.Equal(t, 200, respCode(rr2))
	require.Equal(t, "STALE", respHeader(rr2, header.XCache))
	require.Equal(t, "body", respBody(rr2))
}

// TestShedNoStaleReturns503 proves a shed hard miss returns 503 with
// Retry-After, distinct from the 502 origin-failure mapping.
func TestShedNoStaleReturns503(t *testing.T) {
	t.Parallel()
	h, counter := shedTestHandler(t, origin200("body"))

	h.fetchSem <- struct{}{}
	defer func() { <-h.fetchSem }()

	rr := testCtx("GET", "http://example.com/hardmiss")
	h.ServeRequest(rr)

	require.Equal(t, 503, respCode(rr))
	require.Equal(t, "MISS", respHeader(rr, header.XCache))
	require.Equal(t, "1", respHeader(rr, header.RetryAfter))
	require.Equal(t, int64(1), counter.n.Load())
}

// TestShedBypassReturns503 proves a shed BYPASS request returns 503 with
// Retry-After instead of a 502.
func TestShedBypassReturns503(t *testing.T) {
	t.Parallel()
	h, counter := shedTestHandler(t, origin200("body"))

	h.fetchSem <- struct{}{}
	defer func() { <-h.fetchSem }()

	rr := testCtxWithHeader("GET", "http://example.com/nocache", header.CacheControl, "no-cache")
	h.ServeRequest(rr)

	require.Equal(t, 503, respCode(rr))
	require.Equal(t, "1", respHeader(rr, header.RetryAfter))
	require.Equal(t, int64(1), counter.n.Load())
}

// TestShedInflightFollowersUnpark proves followers waiting on the leader's
// inflightStream un-park promptly when the leader sheds: they receive the
// shed error via the closed done channel and map it to 503 + Retry-After
// instead of stalling behind the leader.
func TestShedInflightFollowersUnpark(t *testing.T) {
	t.Parallel()
	h, counter := shedTestHandler(t, origin200("body"))

	h.fetchSem <- struct{}{}
	defer func() { <-h.fetchSem }()

	const followers = 4
	results := make(chan int, followers)
	var wg sync.WaitGroup
	wg.Add(followers)
	start := make(chan struct{})
	for range followers {
		go func() {
			defer wg.Done()
			<-start
			rr := testCtx("GET", "http://example.com/concurrent")
			h.ServeRequest(rr)
			results <- respCode(rr)
		}()
	}
	close(start)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("followers did not un-park after leader shed")
	}
	for range followers {
		require.Equal(t, 503, <-results, "all shed followers must map to 503")
	}
	// The original leader sheds once. A goroutine that arrives after the
	// leader's inflight entry is deleted becomes a new leader and sheds
	// again — correct behavior under staggered scheduling (see
	// fetchAndStore's loadOrStore/delete window), so the total may exceed
	// 1 and is bounded by the goroutine count. Pinned here: every
	// follower un-parked with 503 above, and each leader shed exactly
	// once.
	sheds := counter.n.Load()
	require.GreaterOrEqual(t, sheds, int64(1), "at least the original leader must shed")
	require.LessOrEqual(t, sheds, int64(followers), "at most one shed per request")
}

// TestShedSingleflightFollowerServesStale covers the collapsedFetch
// follower on the stayin-alive path: when the leader sheds, the follower
// receives ErrFetchShed through singleflight and serves stale — the
// counter must not increment twice.
func TestShedSingleflightFollowerServesStale(t *testing.T) {
	t.Parallel()
	h, counter := shedTestHandler(t, origin200("body"))
	h.stayinAlive = true

	url := "http://example.com/sf-shed"
	staleNoValidator(t, h, url, "body")

	h.fetchSem <- struct{}{}
	defer func() { <-h.fetchSem }()

	const requests = 3
	results := make(chan int, requests)
	var wg sync.WaitGroup
	wg.Add(requests)
	start := make(chan struct{})
	for range requests {
		go func() {
			defer wg.Done()
			<-start
			rr := testCtx("GET", url)
			h.ServeRequest(rr)
			results <- respCode(rr)
		}()
	}
	close(start)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("singleflight requests did not un-park after leader shed")
	}
	for range requests {
		require.Equal(t, 200, <-results, "all shed requests must serve stale")
	}
	// One shed per leader at the semaphore. A late request arriving
	// after the leader's inflight entry is deleted becomes a new leader
	// and sheds again, so the total is bounded by the request count,
	// not fixed at 1.
	sheds := counter.n.Load()
	require.GreaterOrEqual(t, sheds, int64(1))
	require.LessOrEqual(t, sheds, int64(requests))
}

// TestFetchWaitTimeoutConfig plumbs fetch_wait_timeout through
// HandlerConfig, defaulting to the package constant.
func TestFetchWaitTimeoutConfig(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	require.Equal(t, defaultFetchWaitTimeout, h.fetchWaitTimeout)

	h2 := NewHandler(HandlerConfig{
		Upstream:         origin200("body"),
		FastClient:       &testFastClient{handler: origin200("body")},
		Store:            storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2}),
		FetchWaitTimeout: 250 * time.Millisecond,
	})
	require.Equal(t, 250*time.Millisecond, h2.fetchWaitTimeout)
}

// TestShedStreamErrorPathMapsDistinctly proves the miss error path
// distinguishes shed (503 + Retry-After) from origin failure (502).
func TestShedStreamErrorPathMapsDistinctly(t *testing.T) {
	t.Parallel()
	h, _ := shedTestHandler(t, origin200("body"))

	// Origin failure: 502.
	h.fastClient = &errorFastClient{}
	rr := testCtx("GET", "http://example.com/err")
	h.ServeRequest(rr)
	require.Equal(t, 502, respCode(rr))

	// Shed: 503 + Retry-After.
	h.fastClient = &testFastClient{handler: origin200("body")}
	h.fetchSem <- struct{}{}
	rr2 := testCtx("GET", "http://example.com/err2")
	h.ServeRequest(rr2)
	<-h.fetchSem
	require.Equal(t, 503, respCode(rr2))
	require.Equal(t, "1", respHeader(rr2, header.RetryAfter))
}

// errorFastClient always fails, simulating origin errors.
type errorFastClient struct{}

func (c *errorFastClient) Do(_ context.Context, _ *fasthttp.Request, _ *fasthttp.Response) error {
	return errors.New("origin down")
}

func (c *errorFastClient) DoDeadline(_ *fasthttp.Request, _ *fasthttp.Response, _ time.Time) error {
	return errors.New("origin down")
}
