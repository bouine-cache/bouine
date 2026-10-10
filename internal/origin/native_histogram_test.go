package origin

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRequestDuration_NativeHistogramPresent pins the native histogram
// contract for the origin request duration metric: it must expose the
// sparse-bucket representation alongside the classic buckets.
func TestRequestDuration_NativeHistogramPresent(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	m.RequestDuration.WithLabelValues("pool", "target", "200").Observe(0.05)

	mfs, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, mf := range mfs {
		if mf.GetName() != "bouine_origin_request_duration_seconds" {
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
	assert.True(t, found, "bouine_origin_request_duration_seconds must be gathered")
}
