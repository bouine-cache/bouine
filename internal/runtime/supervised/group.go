// Package supervised provides a small wrapper around errgroup that
// enforces AGENTS.md §11: every goroutine has an owner, recovers from
// panics, names itself for observability, and joins on shutdown.
//
// Usage:
//
//	g := supervised.NewGroup(ctx, logger)
//	g.Go("listener-h1", func(ctx context.Context) error {
//	    return srv.Serve(l)
//	})
//	if err := g.Wait(); err != nil {
//	    return err
//	}
//
// The first error or panic cancels the derived context, signalling
// every sibling goroutine to drain.
package supervised

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"

	"golang.org/x/sync/errgroup"
)

// Group is a supervised set of goroutines. The zero value is not
// usable; call NewGroup.
//
// Stable.
type Group struct {
	eg     *errgroup.Group
	ctx    context.Context //nolint:containedctx // group lifetime is the context
	logger *slog.Logger
}

// NewGroup returns a Group whose derived context is cancelled when the
// first member returns a non-nil error or panics. A nil logger falls
// back to slog.Default.
//
// Stable.
func NewGroup(ctx context.Context, logger *slog.Logger) *Group {
	if logger == nil {
		logger = slog.Default()
	}
	eg, gctx := errgroup.WithContext(ctx)
	return &Group{eg: eg, ctx: gctx, logger: logger}
}

// Go starts a named goroutine. The name appears in error wrapping and
// panic logs, so it should be short and descriptive ("listener-h1",
// "origin-pool-app", …).
//
// Stable.
func (g *Group) Go(name string, fn func(ctx context.Context) error) {
	g.eg.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				g.logger.Error("supervised goroutine panicked",
					"name", name,
					"panic", r,
					"stack", string(stack))
				err = fmt.Errorf("%s: panic: %v", name, r)
			}
		}()
		if err := fn(g.ctx); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		return nil
	})
}

// Wait blocks until all members have returned. Returns the first
// non-nil error (already wrapped with the goroutine name).
//
// Stable.
func (g *Group) Wait() error { return g.eg.Wait() }

// Context returns the derived context. It is cancelled the moment the
// first member returns a non-nil error or panics.
//
// Stable.
func (g *Group) Context() context.Context { return g.ctx }
