// Package poll provides test-only helpers that wait for asynchronous
// conditions to become true without resorting to fixed time.Sleep calls.
//
// Use [Eventually] for poll-until-true loops (goroutine counts, port
// readiness, cluster convergence, cache population, etc.). Prefer real
// synchronization primitives (channels, sync.WaitGroup) when a precise
// signal is available; reach for this helper only when the only observable
// signal is a queryable predicate.
package poll

import (
	"testing"
	"time"
)

// Eventually polls condition every interval until it returns true or the
// timeout elapses. On timeout it fails the test via t.Fatalf.
//
// Eventually is the deterministic replacement for ad-hoc
// "for i := 0; i < N; i++ { if cond(); break; time.Sleep(...) }" loops:
// the timeout bounds the worst case while the interval keeps the polling
// cheap, and the failure mode is an explicit, descriptive test failure
// rather than a silent flake.
func Eventually(t testing.TB, timeout, interval time.Duration, condition func() bool) {
	t.Helper()
	if interval <= 0 {
		interval = 10 * time.Millisecond
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		<-time.After(interval)
	}
	if condition() {
		return
	}
	t.Fatalf("poll.Eventually: condition not satisfied within %s", timeout)
}

// WaitFor is a shortcut for Eventually with a sensible default interval.
func WaitFor(t testing.TB, timeout time.Duration, condition func() bool) {
	Eventually(t, timeout, 10*time.Millisecond, condition)
}
