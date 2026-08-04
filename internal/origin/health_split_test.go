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
	if len(healthy) != 1 {
		t.Fatalf("expected exactly 1 healthy target (good), got %v", healthy)
	}
	if healthy[0] != good.Listener.Addr().String() {
		t.Fatalf("expected good target to remain healthy, got %s", healthy[0])
	}
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
		if rr.Code != http.StatusBadGateway {
			t.Fatalf("expected 502, got %d", rr.Code)
		}
	}

	if len(p.Healthy()) != 0 {
		t.Fatalf("expected 0 healthy targets after connection errors, got %v", p.Healthy())
	}
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

	if got := p.targets[0].passiveErrors.Load(); got != 42 {
		t.Fatalf("passiveErrors = %d, want 42 (passive disabled must not zero)", got)
	}
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
	if len(p.Healthy()) != 0 {
		t.Fatal("should be ejected")
	}

	// Manually set both counters to verify they're reset.
	p.targets[0].passiveErrors.Store(5)
	p.targets[0].probeErrors.Store(3)
	p.targets[0].successes.Store(2)

	p.MarkHealthy(bad.Listener.Addr().String())
	if len(p.Healthy()) != 1 {
		t.Fatal("should be healthy after MarkHealthy")
	}

	if got := p.targets[0].passiveErrors.Load(); got != 0 {
		t.Fatalf("passiveErrors = %d, want 0", got)
	}
	if got := p.targets[0].probeErrors.Load(); got != 0 {
		t.Fatalf("probeErrors = %d, want 0", got)
	}
	if got := p.targets[0].successes.Load(); got != 0 {
		t.Fatalf("successes = %d, want 0", got)
	}
}

// TestRecordProbeSuccess_ResetsSuccessesOnCASFailure verifies that
// successes is reset to 0 even when the CAS to restore the target
// fails (because MarkHealthy or another goroutine already set it
// healthy). Without the reset, the next ejection cycle would restore
// after a single probe success instead of threshold successes.
func TestRecordProbeSuccess_ResetsSuccessesOnCASFailure(t *testing.T) {
	t.Parallel()
	srv := echoServer(t)
	defer srv.Close()

	p := pool(t, srv.Listener.Addr().String())
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Eject the target.
	p.targets[0].healthy.Store(false)
	p.targets[0].probeErrors.Store(5)

	// Simulate MarkHealthy racing with recordProbeSuccess: MarkHealthy
	// sets healthy=true first, so the CAS in recordProbeSuccess fails.
	p.MarkHealthy(srv.Listener.Addr().String())
	if len(p.Healthy()) != 1 {
		t.Fatal("should be healthy after MarkHealthy")
	}

	// Now call recordProbeSuccess. The CAS will fail (already healthy),
	// but successes must still be reset.
	p.targets[0].successes.Store(99)
	p.targets[0].recordProbeSuccess(3, logger, "test")
	if got := p.targets[0].successes.Load(); got != 0 {
		t.Fatalf("successes = %d, want 0 (must reset even on CAS failure)", got)
	}
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
	if len(p.Healthy()) != 0 {
		t.Fatalf("expected 0 healthy targets, got %v", p.Healthy())
	}
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
	if len(statuses) != 1 {
		t.Fatalf("expected 1 target, got %d", len(statuses))
	}
	s := statuses[0]
	if s.PassiveErrors != 7 {
		t.Fatalf("PassiveErrors = %d, want 7", s.PassiveErrors)
	}
	if s.ProbeErrors != 3 {
		t.Fatalf("ProbeErrors = %d, want 3", s.ProbeErrors)
	}
	if s.ConsecutiveErrors != 10 {
		t.Fatalf("ConsecutiveErrors = %d, want 10", s.ConsecutiveErrors)
	}
}

// TestActiveRestore_ResetsPassiveErrors verifies that a successful
// active health restore resets passiveErrors to zero. Without this, a
// restored target carries stale passive error debt and gets re-ejected
// on the first passive 5xx response.
func TestActiveRestore_ResetsPassiveErrors(t *testing.T) {
	t.Parallel()
	srv := echoServer(t)
	defer srv.Close()

	p := pool(t, srv.Listener.Addr().String())

	// Simulate a target that was passively ejected: healthy=false,
	// passiveErrors at threshold.
	p.targets[0].healthy.Store(false)
	p.targets[0].passiveErrors.Store(3)
	p.targets[0].probeErrors.Store(0)

	hc := NewActiveHealthChecker(p, ActiveHealthConfig{
		Path:               "/",
		Interval:           10 * time.Millisecond,
		Timeout:            1 * time.Second,
		HealthyThreshold:   2,
		UnhealthyThreshold: 3,
		ExpectedCodes:      []int{200},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = hc.Run(ctx)

	// Target should be restored.
	if len(p.Healthy()) != 1 {
		t.Fatalf("expected 1 healthy target after restore, got %v", p.Healthy())
	}

	// passiveErrors must be reset — otherwise the first 5xx response
	// would increment to 4 >= threshold and re-eject immediately.
	if got := p.targets[0].passiveErrors.Load(); got != 0 {
		t.Fatalf("passiveErrors = %d after restore, want 0 (stale passive debt)", got)
	}
}
