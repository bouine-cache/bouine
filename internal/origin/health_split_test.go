package origin

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestActiveHealth_AccumulatesDespitePassiveTraffic is the core bug
// from issue #290: active probe failures must accumulate even while
// passive traffic sends 2xx to a healthy target in the same pool.
// Before the fix, the passive else-branch zeroed the shared counter
// on every 2xx, wiping active probe failures.
func TestActiveHealth_AccumulatesDespitePassiveTraffic(t *testing.T) {
	t.Parallel()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	defer good.Close()

	p := pool(t, bad.Listener.Addr().String(), good.Listener.Addr().String())
	// Passive health enabled with a high threshold so passive ejection
	// doesn't interfere — we're testing active health accumulation.
	h := p.Handler(100, nil)

	hc := NewActiveHealthChecker(p, ActiveHealthConfig{
		Path:               "/",
		Interval:           10 * time.Millisecond,
		Timeout:            1 * time.Second,
		UnhealthyThreshold: 3,
		ExpectedCodes:      []int{200},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Run active health checks in the background.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = hc.Run(ctx)
	}()

	// Simulate steady passive traffic to the good target (2xx responses).
	// Before the fix, this zeroes the shared errors counter on every
	// request, preventing active health from ever accumulating failures.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				rr := httptest.NewRecorder()
				h.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))
			}
		}
	}()

	wg.Wait()

	// The bad target should be ejected by active health despite the
	// passive traffic keeping the good target's counter at zero.
	healthy := p.Healthy()
	require.Len(t, healthy, 1)
	require.Equal(t, good.Listener.Addr().String(), healthy[0])
}

// TestPassiveHealth_ErrorHandlerEjects verifies that connection errors
// (refused, timeout) increment the passive error counter and eject
// the target. Before the fix, ErrorHandler only logged — it did not
// increment any counter, so a dead origin was never passively ejected.
func TestPassiveHealth_ErrorHandlerEjects(t *testing.T) {
	t.Parallel()
	// Start a server then immediately close it so connections are refused.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	addr := srv.Listener.Addr().String()
	srv.Close() // Kill the server — connections will now be refused.

	p := pool(t, addr)
	h := p.Handler(3, nil)

	// Send requests — each will hit ErrorHandler (connection refused).
	for range 5 {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))
		require.Equal(t, http.StatusBadGateway, rr.Code)
	}

	require.Len(t, p.Healthy(), 0)
}

// TestPassiveHealth_DisabledDoesNotZeroCounters verifies that when
// passive health is disabled (consecutive5xx == 0), the ModifyResponse
// and ErrorHandler paths do not touch passiveErrors. Before the fix,
// the else-branch in ModifyResponse zeroed the shared counter on every
// non-5xx response regardless of whether passive was enabled.
func TestPassiveHealth_DisabledDoesNotZeroCounters(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()

	p := pool(t, srv.Listener.Addr().String())
	// Passive disabled (consecutive5xx == 0).
	h := p.Handler(0, nil)

	// Send a few requests — ModifyResponse else-branch fires but
	// must NOT touch passiveErrors.
	for range 3 {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))
	}

	// Manually set passiveErrors — the 2xx traffic must not have zeroed it.
	p.targets[0].passiveErrors.Store(42)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))

	got := p.targets[0].passiveErrors.Load()
	require.Equal(t, int64(42), got)
}

// TestMarkHealthy_CAS verifies that MarkHealthy uses CompareAndSwap so
// the log and the flag always agree, and resets both counters.
func TestMarkHealthy_CAS(t *testing.T) {
	t.Parallel()
	bad := fivexxServer(t)
	defer bad.Close()

	p := pool(t, bad.Listener.Addr().String())
	h := p.Handler(1, nil)

	// Eject the target.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))
	require.Len(t, p.Healthy(), 0)

	// Manually set both counters to verify they're reset.
	p.targets[0].passiveErrors.Store(5)
	p.targets[0].probeErrors.Store(3)
	p.targets[0].successes.Store(2)

	p.MarkHealthy(bad.Listener.Addr().String())
	require.Len(t, p.Healthy(), 1)

	got := p.targets[0].passiveErrors.Load()
	require.Equal(t, int64(0), got)
	got = p.targets[0].probeErrors.Load()
	require.Equal(t, int64(0), got)
	got = p.targets[0].successes.Load()
	require.Equal(t, int64(0), got)
}

// TestConcurrent_ActiveAndPassive verifies that active and passive
// health paths do not interfere when run concurrently. The passive
// counter and the probe counter must be independent.
func TestConcurrent_ActiveAndPassive(t *testing.T) {
	t.Parallel()
	bad := fivexxServer(t)
	defer bad.Close()

	p := pool(t, bad.Listener.Addr().String())
	h := p.Handler(2, nil)

	hc := NewActiveHealthChecker(p, ActiveHealthConfig{
		Path:               "/",
		Interval:           5 * time.Millisecond,
		Timeout:            1 * time.Second,
		UnhealthyThreshold: 5,
		ExpectedCodes:      []int{200},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	// Passive: send 5xx traffic.
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				rr := httptest.NewRecorder()
				h.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))
			}
		}
	}()

	// Active: run health checks.
	go func() {
		defer wg.Done()
		_ = hc.Run(ctx)
	}()

	wg.Wait()

	// Target should be ejected (by whichever path hits threshold first).
	require.Len(t, p.Healthy(), 0)
}

// TestTargetStatus_SplitCounters verifies that TargetStatus exposes
// passiveErrors and probeErrors separately, and ConsecutiveErrors is
// their sum for backward compatibility.
func TestTargetStatus_SplitCounters(t *testing.T) {
	t.Parallel()
	srv := echoServer(t)
	defer srv.Close()

	p := pool(t, srv.Listener.Addr().String())
	p.targets[0].passiveErrors.Store(7)
	p.targets[0].probeErrors.Store(3)

	statuses := p.Targets()
	require.Len(t, statuses, 1)
	s := statuses[0]
	require.Equal(t, int64(7), s.PassiveErrors)
	require.Equal(t, int64(3), s.ProbeErrors)
	require.Equal(t, int64(10), s.ConsecutiveErrors)
}
