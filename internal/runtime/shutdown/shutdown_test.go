package shutdown

import (
	"testing"

	"github.com/bouine-cache/bouine/internal/observability"
)

func newTestLogger() observability.Logger {
	return observability.NewSampledLogger(nil, 0)
}

func TestReadinessGate_AllConditionsTrue(t *testing.T) {
	g := NewReadinessGate()
	g.Register("store-loaded")
	g.Register("listeners-bound")

	if g.AllReady() {
		t.Fatal("expected not ready with unmarked conditions")
	}

	g.MarkReady("store-loaded")
	if g.AllReady() {
		t.Fatal("expected not ready with one condition false")
	}

	g.MarkReady("listeners-bound")
	if !g.AllReady() {
		t.Fatal("expected ready with all conditions true")
	}
}

func TestReadinessGate_MarkReadyUnregistered(t *testing.T) {
	g := NewReadinessGate()
	g.MarkReady("nonexistent") // should be a no-op
	if !g.AllReady() {
		t.Fatal("expected ready with no registered conditions")
	}
}

func TestReadinessGate_DuplicateRegister(t *testing.T) {
	g := NewReadinessGate()
	g.Register("store-loaded")
	g.Register("store-loaded") // should not reset to false
	g.MarkReady("store-loaded")
	if !g.AllReady() {
		t.Fatal("expected ready after marking the single condition")
	}
}

func TestSequencer_IsReady_GateConditions(t *testing.T) {
	s := NewSequencer(newTestLogger())
	s.Gate().Register("store-loaded")
	s.Gate().Register("listeners-bound")

	if s.IsReady() {
		t.Fatal("expected not ready before conditions are marked")
	}

	s.Gate().MarkReady("store-loaded")
	if s.IsReady() {
		t.Fatal("expected not ready with one condition false")
	}

	s.Gate().MarkReady("listeners-bound")
	if !s.IsReady() {
		t.Fatal("expected ready with all conditions true and not shutting down")
	}
}

func TestSequencer_IsReady_ShutdownFlipsFalse(t *testing.T) {
	s := NewSequencer(newTestLogger())
	// No conditions registered → AllReady returns true.
	if !s.IsReady() {
		t.Fatal("expected ready with no conditions and not shutting down")
	}

	s.markShuttingDown()
	if s.IsReady() {
		t.Fatal("expected not ready after markShuttingDown")
	}
}
