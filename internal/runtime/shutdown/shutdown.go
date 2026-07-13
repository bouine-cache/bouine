// Package shutdown implements the ordered graceful shutdown sequence.
// Each step has a budget carved from the
// total terminationGracePeriod.
package shutdown

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bouine-cache/bouine/internal/observability"
)

// Sequencer runs an ordered series of shutdown steps, each with a
// time budget. Steps run in the order they are registered.
// It also tracks startup readiness via a ReadinessGate.
type Sequencer struct {
	steps  []step
	logger observability.Logger
	ready  atomic.Bool
	gate   *ReadinessGate
}

type step struct {
	name   string
	budget time.Duration
	fn     func(ctx context.Context) error
}

// ReadinessGate tracks named startup conditions. IsReady returns true
// only when the Sequencer is not shutting down and all registered
// conditions are marked ready.
type ReadinessGate struct {
	mu         sync.RWMutex
	conditions map[string]*atomic.Bool
}

// NewReadinessGate creates an empty ReadinessGate.
func NewReadinessGate() *ReadinessGate {
	return &ReadinessGate{
		conditions: make(map[string]*atomic.Bool),
	}
}

// Register adds a named condition. The condition starts false.
func (g *ReadinessGate) Register(name string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.conditions[name]; !ok {
		g.conditions[name] = &atomic.Bool{}
	}
}

// MarkReady sets a condition to true. If the condition was not
// registered, this is a no-op.
func (g *ReadinessGate) MarkReady(name string) {
	g.mu.RLock()
	c, ok := g.conditions[name]
	g.mu.RUnlock()
	if ok {
		c.Store(true)
	}
}

// AllReady returns true if all registered conditions are true.
func (g *ReadinessGate) AllReady() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, c := range g.conditions {
		if !c.Load() {
			return false
		}
	}
	return true
}

// NewSequencer creates a Sequencer. It starts in the "not-ready"
// state — IsReady returns false until MarkReady is called for all
// registered conditions and Shutdown has not begun.
func NewSequencer(logger observability.Logger) *Sequencer {
	logger = observability.ResolveLogger(logger)
	s := &Sequencer{
		logger: logger,
		gate:   NewReadinessGate(),
	}
	s.ready.Store(true) // not shutting down; gate conditions start false
	return s
}

// Gate returns the ReadinessGate so callers can register conditions
// and mark them ready during startup.
func (s *Sequencer) Gate() *ReadinessGate {
	return s.gate
}

// AddStep registers a shutdown step. Steps execute in registration
// order. The budget is the maximum time the step is allowed to take.
func (s *Sequencer) AddStep(name string, budget time.Duration, fn func(ctx context.Context) error) {
	s.steps = append(s.steps, step{name: name, budget: budget, fn: fn})
}

// IsReady reports whether all startup conditions are met and the
// server is not shutting down. Returns false after Execute is called
// or while any registered condition is still false.
func (s *Sequencer) IsReady() bool {
	if !s.ready.Load() {
		return false
	}
	return s.gate.AllReady()
}

// markShuttingDown flips the ready flag to false. Called by Execute.
func (s *Sequencer) markShuttingDown() {
	s.ready.Store(false)
}

// Execute runs all steps in order, carving each step's budget from
// the provided parent context. Errors are logged but don't stop
// subsequent steps. Pass the daemon's root context so step budgets
// are bounded by both the step limit and the overall shutdown deadline.
func (s *Sequencer) Execute(ctx context.Context) {
	s.markShuttingDown()
	s.logger.Info("shutdown: starting", "steps", len(s.steps))

	for _, st := range s.steps {
		ctx, cancel := context.WithTimeout(ctx, st.budget)
		s.logger.Info("shutdown: step starting", "name", st.name, "budget", st.budget)

		if err := st.fn(ctx); err != nil {
			s.logger.Warn("shutdown: step error", "name", st.name, "error", err)
		} else {
			s.logger.Info("shutdown: step done", "name", st.name)
		}
		cancel()
	}

	s.logger.Info("shutdown: complete")
}
