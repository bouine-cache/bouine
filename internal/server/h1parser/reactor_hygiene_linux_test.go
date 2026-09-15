//go:build linux

package h1parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSpinBudgetForScalesWithConnections pins the saturation CPU-burn
// fix: the loop's busy-poll budget is the full configured window at
// low connection counts (the keep-alive RTT win) and tapers to zero as
// the loop multiplexes more connections — at saturation every spun
// poll steals a core from fetch goroutines.
func TestSpinBudgetForScalesWithConnections(t *testing.T) {
	t.Parallel()
	full := spinBudgetFor(1)
	require.Equal(t, reactorSpinBudget, full,
		"a near-empty loop must keep the full spin budget (the RTT win)")
	require.Equal(t, reactorSpinBudget, spinBudgetFor(spinBudgetFullConns))
	assert.Equal(t, 0, spinBudgetFor(spinBudgetZeroConns),
		"a saturated loop must never busy-poll")
	assert.Equal(t, 0, spinBudgetFor(spinBudgetZeroConns*4))
	if full > 0 {
		mid := spinBudgetFor((spinBudgetFullConns + spinBudgetZeroConns) / 2)
		assert.Greater(t, full, mid,
			"the budget tapers between the anchors")
		assert.GreaterOrEqual(t, mid, 0)
	}
}
