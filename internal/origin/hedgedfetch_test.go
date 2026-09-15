package origin

import (
	"context"
	"io"
	"log/slog"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"

	"github.com/valyala/fasthttp"
)

// newPoolWithHedge builds a pool with hedge_timeout set against a single
// upstream target.
func poolWithHedge(t *testing.T, addr string, hedge time.Duration) *Pool {
	t.Helper()
	p, err := NewPool(PoolConfig{
		Name:         "test",
		Targets:      []string{addr},
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		HedgeTimeout: hedge,
	})
	require.NoError(t, err)
	return p
}

func hedgeRequest(t *testing.T) *fasthttp.Request {
	t.Helper()
	req := fasthttp.AcquireRequest()
	req.Header.SetMethod("GET")
	req.SetRequestURI("/slow")
	req.Header.SetHost("test")
	return req
}

func TestPoolFastClient_HedgeDisabledByDefault(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, newEchoHandler())
	defer srv.Close()

	p := poolWithHedge(t, srv.Addr, 0)
	fc := p.FastClient()
	require.Equal(t, time.Duration(0), fc.hedgeTimeout, "zero hedge_timeout must leave the pool unhedged")
}

func TestPoolFastClient_HedgeFastResponseSingleAttempt(t *testing.T) {
	t.Parallel()
	// The handler sleeps longer than the hedge delay: both attempts fire,
	// so the counter reads 2 even though the primary is healthy.
	var calls atomic.Int32
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		time.Sleep(50 * time.Millisecond)
		calls.Add(1)
		ctx.SetStatusCode(200)
	})
	defer srv.Close()

	p := poolWithHedge(t, srv.Addr, 10*time.Millisecond)
	fc := p.FastClient()

	req := hedgeRequest(t)
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	require.NoError(t, fc.Do(context.Background(), req, resp))
	require.Equal(t, 200, resp.StatusCode())
	// The hedge fires ~10ms in but the primary completes at ~50ms; the
	// caller returns as soon as the first response lands, and the other
	// attempt may still be in flight. Give the loser time to reach the
	// handler before counting.
	require.Eventually(t, func() bool {
		return calls.Load() == 2
	}, 2*time.Second, 10*time.Millisecond, "slow primary must fire the hedge")
}

func TestPoolFastClient_HedgeWinnerCopiedToCaller(t *testing.T) {
	t.Parallel()
	// First attempt is slow; the hedge wins with a distinct body.
	var calls atomic.Int32
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		if calls.Add(1) == 1 {
			time.Sleep(1500 * time.Millisecond)
			ctx.SetStatusCode(200)
			ctx.SetBodyString("slow")
			return
		}
		ctx.SetStatusCode(200)
		ctx.SetBodyString("fast")
	})
	defer srv.Close()

	p := poolWithHedge(t, srv.Addr, 30*time.Millisecond)
	fc := p.FastClient()

	req := hedgeRequest(t)
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	start := time.Now()
	require.NoError(t, fc.Do(context.Background(), req, resp))
	elapsed := time.Since(start)
	require.Equal(t, 200, resp.StatusCode())
	require.Equal(t, "fast", string(resp.Body()), "the hedge attempt must win")
	// The hedge attempt (fired ~30ms in, fast) must win; the caller
	// must not wait for the 1500ms slow loser. Even under -race, the
	// fast attempt lands far before 1s.
	require.Less(t, elapsed, time.Second, "the caller must not wait for the slow loser")
}

func TestPoolFastClient_HedgeNonIdempotentSingleAttempt(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		time.Sleep(50 * time.Millisecond)
		calls.Add(1)
		ctx.SetStatusCode(200)
	})
	defer srv.Close()

	p := poolWithHedge(t, srv.Addr, 10*time.Millisecond)
	fc := p.FastClient()

	req := hedgeRequest(t)
	req.Header.SetMethod("POST")
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	require.NoError(t, fc.Do(context.Background(), req, resp))
	require.Equal(t, 200, resp.StatusCode())
	// The POST must not fire a duplicate: count after the hedge delay
	// would have elapsed and the response settled.
	time.Sleep(80 * time.Millisecond)
	require.EqualValues(t, 1, calls.Load(), "POST must not be hedged")
}

func TestPoolFastClient_HedgeDoDeadlinePath(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		if calls.Add(1) == 1 {
			time.Sleep(200 * time.Millisecond)
		}
		ctx.SetStatusCode(200)
	})
	defer srv.Close()

	p := poolWithHedge(t, srv.Addr, 30*time.Millisecond)
	fc := p.FastClient()

	req := hedgeRequest(t)
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	deadline := time.Now().Add(5 * time.Second)
	require.NoError(t, fc.DoDeadline(req, resp, deadline))
	require.Equal(t, 200, resp.StatusCode())
	require.Eventually(t, func() bool {
		return calls.Load() == 2
	}, 2*time.Second, 10*time.Millisecond, "DoDeadline must fire the hedge")
}

func TestPoolFastClient_HedgeSSENotHedged(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		time.Sleep(50 * time.Millisecond)
		calls.Add(1)
		ctx.SetStatusCode(200)
	})
	defer srv.Close()

	p := poolWithHedge(t, srv.Addr, 10*time.Millisecond)
	fc := p.FastClient()

	req := hedgeRequest(t)
	req.Header.Set("Accept", "text/event-stream")
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	require.NoError(t, fc.Do(context.Background(), req, resp))
	time.Sleep(80 * time.Millisecond)
	require.EqualValues(t, 1, calls.Load(), "SSE-intent requests must not be hedged")
}

func TestHedgingAllowed(t *testing.T) {
	t.Parallel()

	sseReq := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(sseReq)
	sseReq.Header.SetMethod("GET")
	sseReq.Header.Set("Accept", "text/event-stream")

	postReq := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(postReq)
	postReq.Header.SetMethod("POST")

	getReq := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(getReq)
	getReq.Header.SetMethod("GET")

	assert.True(t, hedgingAllowed(getReq, nil))
	assert.False(t, hedgingAllowed(postReq, nil), "POST is not idempotent")
	assert.False(t, hedgingAllowed(sseReq, &fasthttp.Client{}), "SSE streams must not be doubled")
	assert.True(t, hedgingAllowed(sseReq, nil), "no stream client: SSE intent is routed through the normal client")
}

func TestDoHedged_PrimaryErrorHedgeSucceeds(t *testing.T) {
	t.Parallel()
	// The primary must stay in flight past the hedge delay for the
	// duplicate to fire — an instantly-failing primary returns its
	// error before the hedge is ever fired (by design, no wasted
	// duplicate).
	var calls atomic.Int32
	fire := func(_ context.Context) error {
		if calls.Add(1) == 1 {
			time.Sleep(100 * time.Millisecond)
			return assert.AnError
		}
		return nil
	}
	err := doHedged(context.Background(), 20*time.Millisecond, fire)
	require.NoError(t, err, "the second attempt's success must rescue the caller")
	require.EqualValues(t, 2, calls.Load())
}

func TestDoHedged_PrimaryErrorBeforeHedge(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	fire := func(_ context.Context) error {
		calls.Add(1)
		return assert.AnError
	}
	err := doHedged(context.Background(), time.Hour, fire)
	require.ErrorIs(t, err, assert.AnError)
	require.EqualValues(t, 1, calls.Load(), "an error before the hedge delay must not fire the duplicate")
}

func TestDoHedged_NoGoroutineLeak(t *testing.T) {
	t.Parallel()
	before := countGoroutines()
	for range 50 {
		var calls atomic.Int32
		fire := func(_ context.Context) error {
			if calls.Add(1) == 1 {
				time.Sleep(30 * time.Millisecond)
			}
			return nil
		}
		require.NoError(t, doHedged(context.Background(), 5*time.Millisecond, fire))
	}
	// The loser attempts are aborted by the cancel but their goroutines
	// may still be inside a sleep or a pool release for a few
	// milliseconds; poll for the count to drop back near baseline.
	// Parallel tests run concurrently in this package, so allow a
	// generous margin above the baseline instead of a tight one.
	require.Eventually(t, func() bool {
		return countGoroutines() <= before+20
	}, 10*time.Second, 100*time.Millisecond, "hedged attempts must not leak goroutines")
}

func countGoroutines() int {
	return runtime.NumGoroutine()
}
