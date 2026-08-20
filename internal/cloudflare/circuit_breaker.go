package cloudflare

import (
	"sync"
	"time"
)

// CircuitState represents the state of the circuit breaker.
type CircuitState int

const (
	// CircuitClosed means the circuit is closed — calls are allowed.
	CircuitClosed CircuitState = iota
	// CircuitOpen means the circuit is open — calls are rejected.
	CircuitOpen
	// CircuitHalfOpen means the circuit is probing — limited calls allowed.
	CircuitHalfOpen
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

// CircuitConfig configures the circuit breaker behavior.
type CircuitConfig struct {
	// FailureThreshold is the number of consecutive failures before the
	// circuit opens. Default 5 when 0.
	FailureThreshold int
	// OpenTimeout is how long the circuit stays open before transitioning
	// to half-open (probing). Default 30s when 0.
	OpenTimeout time.Duration
	// HalfOpenMaxCalls is the number of probe calls allowed in half-open
	// state before transitioning back to closed (on success) or open (on
	// failure). Default 1 when 0.
	HalfOpenMaxCalls int
}

const (
	defaultFailureThreshold = 5
	defaultOpenTimeout      = 30 * time.Second
	defaultHalfOpenMaxCalls = 1
)

// CircuitBreaker protects against cascading failures by opening the circuit
// after N consecutive failures, failing fast during the open period, and
// probing with a limited number of calls in half-open state.
//
// The circuit breaker is safe for concurrent use.
type CircuitBreaker struct {
	cfg          CircuitConfig
	mu           sync.Mutex
	state        CircuitState
	failures     int
	openedAt     time.Time
	halfOpenUsed int

	// nowFunc returns the current time. Defaults to time.Now.
	// Injected for testing.
	nowFunc func() time.Time

	// Metrics callbacks (optional, nil-safe).
	onStateChange func(from, to CircuitState)
	onReject      func()
}

// NewCircuitBreaker creates a CircuitBreaker with the given config.
func NewCircuitBreaker(cfg CircuitConfig) *CircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = defaultFailureThreshold
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = defaultOpenTimeout
	}
	if cfg.HalfOpenMaxCalls <= 0 {
		cfg.HalfOpenMaxCalls = defaultHalfOpenMaxCalls
	}
	return &CircuitBreaker{
		cfg:     cfg,
		state:   CircuitClosed,
		nowFunc: time.Now,
	}
}

// SetNowFunc injects a custom time function for testing.
func (cb *CircuitBreaker) SetNowFunc(fn func() time.Time) {
	if fn == nil {
		fn = time.Now
	}
	cb.nowFunc = fn
}

// OnStateChange sets a callback invoked when the circuit transitions between
// states (closed → open, open → half_open, half_open → closed/open).
func (cb *CircuitBreaker) OnStateChange(fn func(from, to CircuitState)) {
	cb.onStateChange = fn
}

// OnReject sets a callback invoked when Allow() returns false (circuit open).
func (cb *CircuitBreaker) OnReject(fn func()) {
	cb.onReject = fn
}

// Allow returns true if a call is permitted. In closed state, always true.
// In open state, false (unless the open timeout has elapsed, in which case
// the circuit transitions to half-open and returns true). In half-open
// state, true only if the probe call budget has not been exhausted.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		if cb.nowFunc().Sub(cb.openedAt) < cb.cfg.OpenTimeout {
			if cb.onReject != nil {
				cb.onReject()
			}
			return false
		}
		// Open timeout elapsed — transition to half-open and handle
		// as a half-open call (counts against the probe budget).
		cb.transitionTo(CircuitHalfOpen)
		if cb.halfOpenUsed < cb.cfg.HalfOpenMaxCalls {
			cb.halfOpenUsed++
			return true
		}
		if cb.onReject != nil {
			cb.onReject()
		}
		return false
	case CircuitHalfOpen:
		if cb.halfOpenUsed < cb.cfg.HalfOpenMaxCalls {
			cb.halfOpenUsed++
			return true
		}
		if cb.onReject != nil {
			cb.onReject()
		}
		return false
	default:
		return true
	}
}

// RecordSuccess records a successful call. In half-open state, transitions
// to closed. In closed state, resets the failure counter.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures = 0
	if cb.state == CircuitHalfOpen {
		cb.transitionTo(CircuitClosed)
	}
}

// RecordFailure records a failed call. In closed state, increments the
// failure counter and opens the circuit if the threshold is reached. In
// half-open state, immediately re-opens the circuit.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		cb.failures++
		if cb.failures >= cb.cfg.FailureThreshold {
			cb.transitionTo(CircuitOpen)
		}
	case CircuitHalfOpen:
		cb.transitionTo(CircuitOpen)
	}
}

// State returns the current circuit state. This is a snapshot — the state
// may change between the call and the next Allow/Record call.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// Failures returns the current consecutive failure count.
func (cb *CircuitBreaker) Failures() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.failures
}

func (cb *CircuitBreaker) transitionTo(newState CircuitState) {
	oldState := cb.state
	cb.state = newState
	switch newState {
	case CircuitOpen:
		cb.openedAt = cb.nowFunc()
		cb.halfOpenUsed = 0
	case CircuitHalfOpen:
		cb.halfOpenUsed = 0
	case CircuitClosed:
		cb.failures = 0
	}
	if cb.onStateChange != nil && oldState != newState {
		cb.onStateChange(oldState, newState)
	}
}
