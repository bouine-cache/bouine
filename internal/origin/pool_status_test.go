package origin

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTargetStatusLastChange pins the last-change contract for
// /v1/origins (issue #813): every health transition records a
// timestamp, and a fresh target reports zero.
func TestTargetStatusLastChangeTransitions(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	p, err := NewPool(PoolConfig{
		Name:           "api",
		Targets:        []string{"127.0.0.1:1"},
		Consecutive5xx: 3,
		Logger:         logger,
	})
	require.NoError(t, err, "NewPool")

	st := p.Targets()[0]
	require.True(t, st.Healthy, "fresh target must be healthy")
	require.Zero(t, st.LastChangeUnixNano, "no transition recorded yet")

	target := p.targets[0]
	for range 3 {
		target.recordPassiveError(3, logger, "api", "5xx", "500")
	}
	st = p.Targets()[0]
	require.False(t, st.Healthy, "passive ejection at threshold")
	require.Greater(t, st.LastChangeUnixNano, int64(0), "ejection stamps last change")
	require.Equal(t, int64(3), st.PassiveErrors)

	p.MarkHealthy("127.0.0.1:1")
	st = p.Targets()[0]
	require.True(t, st.Healthy, "manual restore")
	require.GreaterOrEqual(t, st.LastChangeUnixNano, int64(1), "restore stamps last change")
	require.Equal(t, int64(0), st.PassiveErrors, "restore clears the passive streak")

	for range 3 {
		target.recordProbeError(3, logger, "api")
	}
	st = p.Targets()[0]
	require.False(t, st.Healthy, "probe ejection at threshold")

	for range 2 {
		target.recordProbeSuccess(2, logger, "api")
	}
	st = p.Targets()[0]
	require.True(t, st.Healthy, "probe restore at healthy threshold")
	require.Equal(t, int64(0), st.ProbeSuccesses, "streak resets after restore")
	require.Greater(t, st.LastChangeUnixNano, int64(0))

	assert.Equal(t, "127.0.0.1:1", st.Addr, "status carries the target address")
}
