package shutdown

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/observability"
)

func newTestLogger() observability.Logger {
	return observability.NewSampledLogger(nil, 0)
}

func TestReadinessGate_AllConditionsTrue(t *testing.T) {
	g := NewReadinessGate()
	g.Register("store-loaded")
	g.Register("listeners-bound")

	require.False(t, g.AllReady())

	g.MarkReady("store-loaded")
	require.False(t, g.AllReady())

	g.MarkReady("listeners-bound")
	require.True(t, g.AllReady())
}

func TestReadinessGate_MarkReadyUnregistered(t *testing.T) {
	g := NewReadinessGate()
	g.MarkReady("nonexistent") // should be a no-op
	require.True(t, g.AllReady())
}

func TestReadinessGate_DuplicateRegister(t *testing.T) {
	g := NewReadinessGate()
	g.Register("store-loaded")
	g.Register("store-loaded") // should not reset to false
	g.MarkReady("store-loaded")
	require.True(t, g.AllReady())
}

func TestSequencer_IsReady_GateConditions(t *testing.T) {
	s := NewSequencer(newTestLogger())
	s.Gate().Register("store-loaded")
	s.Gate().Register("listeners-bound")

	require.False(t, s.IsReady())

	s.Gate().MarkReady("store-loaded")
	require.False(t, s.IsReady())

	s.Gate().MarkReady("listeners-bound")
	require.True(t, s.IsReady())
}

func TestSequencer_IsReady_ShutdownFlipsFalse(t *testing.T) {
	s := NewSequencer(newTestLogger())
	// No conditions registered → AllReady returns true.
	require.True(t, s.IsReady())

	s.markShuttingDown()
	require.False(t, s.IsReady())
}

func TestSequencer_Drain_MarksNotReady(t *testing.T) {
	s := NewSequencer(newTestLogger())
	s.Gate().Register("store-loaded")
	s.Gate().MarkReady("store-loaded")

	require.True(t, s.IsReady())

	s.Drain()
	require.False(t, s.IsReady())
}

func TestReadinessGate_Conditions(t *testing.T) {
	g := NewReadinessGate()
	g.Register("store-loaded")
	g.Register("listeners-bound")

	conds := g.Conditions()
	require.Len(t, conds, 2)
	// Conditions are sorted alphabetically.
	if conds[0].Name != "listeners-bound" || conds[0].Ready {
		t.Fatalf("expected listeners-bound=false first, got %s=%v", conds[0].Name, conds[0].Ready)
	}
	if conds[1].Name != "store-loaded" || conds[1].Ready {
		t.Fatalf("expected store-loaded=false second, got %s=%v", conds[1].Name, conds[1].Ready)
	}

	g.MarkReady("store-loaded")
	conds = g.Conditions()
	require.True(t, conds[1].Ready)
	require.False(t, conds[0].Ready)
}

func TestReadinessGate_ConditionsEmpty(t *testing.T) {
	g := NewReadinessGate()
	conds := g.Conditions()
	require.Len(t, conds, 0)
}
