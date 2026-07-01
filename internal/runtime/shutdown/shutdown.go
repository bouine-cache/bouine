// Package shutdown implements the ordered graceful shutdown sequence.
// Each step has a budget carved from the
// total terminationGracePeriod.
package shutdown

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/thylong/bouine/internal/observability"
)

// Sequencer runs an ordered series of shutdown steps, each with a
// time budget. Steps run in the order they are registered.
type Sequencer struct {
	steps  []step
	logger observability.Logger
	ready  atomic.Bool
}

type step struct {
	name   string
	budget time.Duration
	fn     func(ctx context.Context) error
}

// NewSequencer creates a Sequencer. It starts in the "ready" state.
func NewSequencer(logger observability.Logger) *Sequencer {
	if logger == nil {
		logger = observability.NoopLogger{}
	}
	s := &Sequencer{logger: logger}
	s.ready.Store(true)
	return s
}

// AddStep registers a shutdown step. Steps execute in registration
// order. The budget is the maximum time the step is allowed to take.
func (s *Sequencer) AddStep(name string, budget time.Duration, fn func(ctx context.Context) error) {
	s.steps = append(s.steps, step{name: name, budget: budget, fn: fn})
}

// IsReady reports whether the server is still accepting traffic.
// Returns false after Execute is called.
func (s *Sequencer) IsReady() bool {
	return s.ready.Load()
}

// Execute runs all steps in order, carving each step's budget from
// the provided parent context. Errors are logged but don't stop
// subsequent steps. Pass the daemon's root context so step budgets
// are bounded by both the step limit and the overall shutdown deadline.
func (s *Sequencer) Execute(ctx context.Context) {
	s.ready.Store(false)
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
