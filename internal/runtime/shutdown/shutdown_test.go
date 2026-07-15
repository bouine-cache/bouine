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

func TestSequencer_Drain_MarksNotReady(t *testing.T) {
	s := NewSequencer(newTestLogger())
	s.Gate().Register("store-loaded")
	s.Gate().MarkReady("store-loaded")

	if !s.IsReady() {
		t.Fatal("expected ready after all conditions met")
	}

	s.Drain()
	if s.IsReady() {
		t.Fatal("expected not ready after Drain")
	}
}

func TestReadinessGate_Conditions(t *testing.T) {
	g := NewReadinessGate()
	g.Register("store-loaded")
	g.Register("listeners-bound")

	conds := g.Conditions()
	if len(conds) != 2 {
		t.Fatalf("expected 2 conditions, got %d", len(conds))
	}
	// Conditions are sorted alphabetically.
	if conds[0].Name != "listeners-bound" || conds[0].Ready {
		t.Fatalf("expected listeners-bound=false first, got %s=%v", conds[0].Name, conds[0].Ready)
	}
	if conds[1].Name != "store-loaded" || conds[1].Ready {
		t.Fatalf("expected store-loaded=false second, got %s=%v", conds[1].Name, conds[1].Ready)
	}

	g.MarkReady("store-loaded")
	conds = g.Conditions()
	if !conds[1].Ready {
		t.Fatal("expected store-loaded=true after MarkReady")
	}
	if conds[0].Ready {
		t.Fatal("expected listeners-bound=false")
	}
}

func TestReadinessGate_ConditionsEmpty(t *testing.T) {
	g := NewReadinessGate()
	conds := g.Conditions()
	if len(conds) != 0 {
		t.Fatalf("expected 0 conditions, got %d", len(conds))
	}
}
