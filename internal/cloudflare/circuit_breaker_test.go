package cloudflare_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	cf "github.com/bouine-cache/bouine/internal/cloudflare"
)

func TestCircuitBreaker_StartsClosed(t *testing.T) {
	t.Parallel()
	cb := cf.NewCircuitBreaker(cf.CircuitConfig{})
	require.Equal(t, cf.CircuitClosed, cb.State())
	require.True(t, cb.Allow())
}

func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	t.Parallel()
	cb := cf.NewCircuitBreaker(cf.CircuitConfig{
		FailureThreshold: 3,
		OpenTimeout:      10 * time.Second,
	})

	// 3 failures should open the circuit.
	cb.RecordFailure()
	require.Equal(t, cf.CircuitClosed, cb.State())
	cb.RecordFailure()
	require.Equal(t, cf.CircuitClosed, cb.State())
	cb.RecordFailure()
	require.Equal(t, cf.CircuitOpen, cb.State())

	// Allow should return false when open.
	require.False(t, cb.Allow())
}

func TestCircuitBreaker_SuccessResetsFailures(t *testing.T) {
	t.Parallel()
	cb := cfCircuitBreaker(t, 5)

	cb.RecordFailure()
	cb.RecordFailure()
	require.Equal(t, 2, cb.Failures())

	cb.RecordSuccess()
	require.Equal(t, 0, cb.Failures())
	require.Equal(t, cf.CircuitClosed, cb.State())
}

func TestCircuitBreaker_TransitionsToHalfOpen(t *testing.T) {
	t.Parallel()
	now := time.Now()
	cb := cf.NewCircuitBreaker(cf.CircuitConfig{
		FailureThreshold: 1,
		OpenTimeout:      50 * time.Millisecond,
	})
	cb.SetNowFunc(func() time.Time { return now })

	// Open the circuit.
	cb.RecordFailure()
	require.Equal(t, cf.CircuitOpen, cb.State())

	// Before timeout: Allow returns false.
	require.False(t, cb.Allow())

	// After timeout: Allow should transition to half-open and return true.
	cb.SetNowFunc(func() time.Time { return now.Add(100 * time.Millisecond) })
	require.True(t, cb.Allow())
	require.Equal(t, cf.CircuitHalfOpen, cb.State())
}

func TestCircuitBreaker_HalfOpenSuccessCloses(t *testing.T) {
	t.Parallel()
	now := time.Now()
	cb := cf.NewCircuitBreaker(cf.CircuitConfig{
		FailureThreshold: 1,
		OpenTimeout:      50 * time.Millisecond,
	})
	cb.SetNowFunc(func() time.Time { return now })

	// Open the circuit.
	cb.RecordFailure()

	// Move past the open timeout.
	cb.SetNowFunc(func() time.Time { return now.Add(100 * time.Millisecond) })

	// Allow should transition to half-open.
	require.True(t, cb.Allow())
	require.Equal(t, cf.CircuitHalfOpen, cb.State())

	// Success in half-open should close the circuit.
	cb.RecordSuccess()
	require.Equal(t, cf.CircuitClosed, cb.State())
}

func TestCircuitBreaker_HalfOpenFailureReopens(t *testing.T) {
	t.Parallel()
	now := time.Now()
	cb := cf.NewCircuitBreaker(cf.CircuitConfig{
		FailureThreshold: 1,
		OpenTimeout:      50 * time.Millisecond,
	})
	cb.SetNowFunc(func() time.Time { return now })

	// Open the circuit.
	cb.RecordFailure()

	// Move past the open timeout.
	cb.SetNowFunc(func() time.Time { return now.Add(100 * time.Millisecond) })

	// Allow should transition to half-open.
	require.True(t, cb.Allow())

	// Failure in half-open should re-open the circuit.
	cb.RecordFailure()
	require.Equal(t, cf.CircuitOpen, cb.State())
}

func TestCircuitBreaker_HalfOpenMaxCalls(t *testing.T) {
	t.Parallel()
	now := time.Now()
	cb := cf.NewCircuitBreaker(cf.CircuitConfig{
		FailureThreshold: 1,
		OpenTimeout:      50 * time.Millisecond,
		HalfOpenMaxCalls: 2,
	})
	cb.SetNowFunc(func() time.Time { return now })

	// Open the circuit.
	cb.RecordFailure()

	// Move past the open timeout.
	cb.SetNowFunc(func() time.Time { return now.Add(100 * time.Millisecond) })

	// First two calls in half-open are allowed.
	require.True(t, cb.Allow())
	require.True(t, cb.Allow())
	// Third call should be rejected (max calls exhausted).
	require.False(t, cb.Allow())
}

func TestCircuitBreaker_OnStateChange(t *testing.T) {
	t.Parallel()
	var changes atomic.Int64
	cb := cf.NewCircuitBreaker(cf.CircuitConfig{
		FailureThreshold: 1,
		OpenTimeout:      10 * time.Second,
	})
	cb.OnStateChange(func(from, to cf.CircuitState) {
		changes.Add(1)
	})

	cb.RecordFailure()
	require.Equal(t, int64(1), changes.Load(), "should fire on closed → open")
}

func TestCircuitBreaker_OnReject(t *testing.T) {
	t.Parallel()
	var rejects atomic.Int64
	cb := cf.NewCircuitBreaker(cf.CircuitConfig{
		FailureThreshold: 1,
		OpenTimeout:      10 * time.Second,
	})
	cb.OnReject(func() {
		rejects.Add(1)
	})

	cb.RecordFailure() // open
	cb.Allow()         // rejected
	require.Equal(t, int64(1), rejects.Load())
}

func TestCircuitBreaker_Defaults(t *testing.T) {
	t.Parallel()
	cb := cf.NewCircuitBreaker(cf.CircuitConfig{})
	require.Equal(t, cf.CircuitClosed, cb.State())
	require.True(t, cb.Allow())

	// Should require default threshold (5) failures to open.
	for range 4 {
		cb.RecordFailure()
		require.Equal(t, cf.CircuitClosed, cb.State())
	}
	cb.RecordFailure()
	require.Equal(t, cf.CircuitOpen, cb.State())
}

func TestCircuitBreaker_String(t *testing.T) {
	t.Parallel()
	require.Equal(t, "closed", cf.CircuitClosed.String())
	require.Equal(t, "open", cf.CircuitOpen.String())
	require.Equal(t, "half_open", cf.CircuitHalfOpen.String())
	require.Equal(t, "unknown", cf.CircuitState(99).String())
}

// Helper to create a circuit breaker with a threshold.
func cfCircuitBreaker(t *testing.T, threshold int) *cf.CircuitBreaker {
	t.Helper()
	return cf.NewCircuitBreaker(cf.CircuitConfig{
		FailureThreshold: threshold,
		OpenTimeout:      10 * time.Second,
	})
}
