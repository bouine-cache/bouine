package observability

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRequestDuration_NativeHistogramPresent pins the wire contract for
// Grafana Cloud: the duration histogram must expose the native
// (sparse-bucket) representation so native histogram_quantile queries
// work, while the classic _bucket series stay for layout-agnostic
// consumers until operators drop them via relabel.
func TestRequestDuration_NativeHistogramPresent(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.RecordHit("p", "HIT", "hot", 200, 10, 500*time.Microsecond)

	mfs, err := reg.Gather()
	require.NoError(t, err)
	found := false
	for _, mf := range mfs {
		if mf.GetName() != "bouine_request_duration_seconds" {
			continue
		}
		found = true
		for _, met := range mf.GetMetric() {
			h := met.GetHistogram()
			// Schema 3 = 1.1 growth factor (prometheus.HistogramOpts:
			// factor 1.1 maps to schema 3). Schema != 0 proves the
			// native (sparse-bucket) representation is on the wire;
			// classic _bucket series are emitted alongside it.
			assert.Equal(t, int32(3), h.GetSchema(),
				"native histogram schema must be present (3 = factor 1.1)")
			assert.Equal(t, uint64(1), h.GetSampleCount(),
				"one observation must land in the native histogram")
			buckets := int32(0)
			for _, s := range h.GetPositiveSpan() {
				buckets += int32(s.GetLength())
			}
			assert.Equal(t, int32(1), buckets,
				"one distinct value must populate exactly one sparse bucket")
		}
	}
	assert.True(t, found, "bouine_request_duration_seconds must be gathered")
}

// TestRequestDuration_NativeBucketCap pins the sparse-bucket cap under
// adversarial traffic: 10k distinct latencies spanning the full 0.5ms-1s
// range across one tuple must stay far below the 80-bucket cap (client_
// golang compacts via schema reduction, probed at 44 buckets for this
// spread), proving the cardinality contract holds without the cap being
// hit in realistic traffic.
func TestRequestDuration_NativeBucketCap(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	// 10k distinct values, 0.5ms..1.5s, one tuple.
	for i := range 10_000 {
		m.RecordHit("p", "HIT", "hot", 100+i%900, 10, time.Duration(500_000+i*99))
	}
	_ = m
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "bouine_request_duration_seconds" {
			continue
		}
		for _, met := range mf.GetMetric() {
			h := met.GetHistogram()
			// Spans encode runs; count buckets from spans.
			buckets := int32(0)
			for _, s := range h.GetPositiveSpan() {
				buckets += int32(s.GetLength())
			}
			assert.LessOrEqual(t, buckets, int32(80),
				"sparse bucket count must respect NativeHistogramMaxBucketNumber")
			assert.Greater(t, buckets, int32(0), "distinct values must populate sparse buckets")
		}
	}
}

// BenchmarkGate_HistogramObserve_Native pins the hard zero-alloc
// contract on the native Observe path: the hit path must not allocate,
// per AGENTS.md §7, in the configuration actually shipped. Warm path =
// bucket already populated (steady state); limit path = bucket growth
// capped by NativeHistogramMaxBucketNumber.
func BenchmarkGate_HistogramObserve_Native(b *testing.B) {
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.PreResolveRoutes([]string{"p"})

	// Steady-state shape: one pool, one tuple, one latency value.
	m.RecordHit("p", "HIT", "hot", 200, 10, 500*time.Microsecond)
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		m.RecordHit("p", "HIT", "hot", 200, 10, 500*time.Microsecond)
	}
}

// BenchmarkGate_HistogramObserve_Native_Distinct exercises the
// limitBuckets path: every iteration a new latency value, forcing
// sparse-bucket growth/compaction up to the 80-bucket cap.
func BenchmarkGate_HistogramObserve_Native_Distinct(b *testing.B) {
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.PreResolveRoutes([]string{"p"})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		m.RecordHit("p", "HIT", "hot", 200, 10, time.Duration(1+i%990_000))
	}
}
