package warm

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCompactionDuration_NativeHistogramPresent pins the native histogram
// contract for the warm-tier compaction duration metric: it must expose
// the sparse-bucket representation alongside the classic buckets.
func TestCompactionDuration_NativeHistogramPresent(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	m.CompactionDuration.Observe(0.5)

	mfs, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, mf := range mfs {
		if mf.GetName() != "bouine_warm_compaction_duration_seconds" {
			continue
		}
		found = true
		for _, met := range mf.GetMetric() {
			h := met.GetHistogram()
			assert.Equal(t, int32(3), h.GetSchema(),
				"native histogram schema must be present (3 = factor 1.1)")
			assert.Equal(t, uint64(1), h.GetSampleCount(),
				"one observation must land in the native histogram")
		}
	}
	assert.True(t, found, "bouine_warm_compaction_duration_seconds must be gathered")
}
