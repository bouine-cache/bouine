package warm

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
)

// metricValue returns the value of a single unlabeled counter or gauge
// from the registry, or 0 if the metric is not found.
func metricValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err, "gather")
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		if len(mf.GetMetric()) == 0 {
			return 0
		}
		m := mf.GetMetric()[0]
		if c := m.GetCounter(); c != nil {
			return c.GetValue()
		}
		if g := m.GetGauge(); g != nil {
			return g.GetValue()
		}
		return 0
	}
	return 0
}

// assertMetricExists verifies a metric with the given name is registered.
func assertMetricExists(t *testing.T, reg *prometheus.Registry, name string) {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err, "gather")
	for _, mf := range mfs {
		if mf.GetName() == name {
			return
		}
	}
	t.Errorf("metric %q not found in registry", name)
}

func TestRegisterMetrics_AllRegistered(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	_ = RegisterMetrics(reg)

	want := []string{
		"bouine_warm_disk_bytes",
		"bouine_warm_max_bytes",
		"bouine_warm_over_budget_total",
		"bouine_warm_evictions_total",
		"bouine_warm_compaction_triggered_total",
	}
	for _, name := range want {
		assertMetricExists(t, reg, name)
	}
}

func TestRegisterMetrics_NilRegistryIsSafe(t *testing.T) {
	t.Parallel()
	m := RegisterMetrics(nil)
	// All helpers must be no-ops, not panics.
	m.IncOverBudget()
	m.IncEvictions()
	m.IncCompactionTriggered()
	m.SetDiskBytes(42)
	m.SetMaxBytes(100)
}

func TestMetrics_OverBudgetIncrements(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 512, SegMax: 1 << 20, Metrics: m})
	require.NoError(t, err, "NewStore")
	t.Cleanup(func() { _ = s.Close() })

	// Fill the budget with protected entries so eviction cannot free space.
	smallBody := make([]byte, 100) // 120 bytes per record
	for i := 0; i < 4; i++ {
		_, _, err := s.Put(testkey.From(uint64(i)), smallBody)
		require.NoErrorf(t, err, "Put %d", i)
	}
	for i := 0; i < 4; i++ {
		s.Protect(testkey.From(uint64(i)))
	}

	// This Put must be rejected with ErrOverBudget.
	_, _, err = s.Put(testkey.From(99), smallBody)
	require.True(t, errors.Is(err, ErrOverBudget))

	got := metricValue(t, reg, "bouine_warm_over_budget_total")
	assert.Equal(t, float64(1), got)
}

func TestMetrics_EvictionsIncrements(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	dir := t.TempDir()
	// Small budget so Put triggers eviction.
	s, err := NewStore(Config{Dir: dir, MaxBytes: 360, SegMax: 1 << 20, Metrics: m})
	require.NoError(t, err, "NewStore")
	t.Cleanup(func() { _ = s.Close() })

	smallBody := make([]byte, 100) // 120 bytes per record
	// 3 records × 120 = 360 = budget. The 4th Put must evict to fit.
	for i := 0; i < 3; i++ {
		_, _, err := s.Put(testkey.From(uint64(i)), smallBody)
		require.NoErrorf(t, err, "Put %d", i)
	}
	_, _, err = s.Put(testkey.From(99), smallBody)
	require.NoError(t, err, "Put 99 with eviction")

	got := metricValue(t, reg, "bouine_warm_evictions_total")
	if got < 1 {
		t.Errorf("evictions_total = %v, want >= 1", got)
	}
}

func TestMetrics_CompactionTriggeredIncrements(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 0, SegMax: 1 << 20, Metrics: m})
	require.NoError(t, err, "NewStore")
	t.Cleanup(func() { _ = s.Close() })

	err = s.Compact()
	require.NoError(t, err, "Compact")

	got := metricValue(t, reg, "bouine_warm_compaction_triggered_total")
	assert.Equal(t, float64(1), got)
}

func TestMetrics_DiskBytesMatchesSegmentSizes(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 0, SegMax: 1 << 20, Metrics: m})
	require.NoError(t, err, "NewStore")
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, 100) // 120 bytes per record
	for i := 0; i < 5; i++ {
		_, _, err := s.Put(testkey.From(uint64(i)), body)
		require.NoErrorf(t, err, "Put %d", i)
	}

	// DiskBytes should equal the sum of segment file sizes, which for 5
	// records of 120 bytes each is 600 bytes.
	got := s.DiskBytes()
	want := int64(5 * (HeaderLen + len(body) + FooterLen))
	assert.Equal(t, want, got)

	// Verify the gauge is not yet set (engine polls it). Set it and check.
	m.SetDiskBytes(got)
	regVal := metricValue(t, reg, "bouine_warm_disk_bytes")
	assert.Equal(t, float64(want), regVal)
}

func TestMetrics_MaxBytesGauge(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	dir := t.TempDir()
	const maxBytes = 1 << 20 // 1 MiB
	s, err := NewStore(Config{Dir: dir, MaxBytes: maxBytes, SegMax: 1 << 20, Metrics: m})
	require.NoError(t, err, "NewStore")
	t.Cleanup(func() { _ = s.Close() })

	got := s.MaxBytes()
	assert.Equal(t, int64(maxBytes), got)

	regVal := metricValue(t, reg, "bouine_warm_max_bytes")
	assert.Equal(t, float64(maxBytes), regVal)
}

func TestMetrics_MaxBytesZeroGauge(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 0, SegMax: 1 << 20, Metrics: m})
	require.NoError(t, err, "NewStore")
	t.Cleanup(func() { _ = s.Close() })

	regVal := metricValue(t, reg, "bouine_warm_max_bytes")
	assert.Equal(t, float64(0), regVal)
}

// TestMetrics_NilMetricsSafe verifies that a store constructed without
// metrics does not panic on Put, eviction, or compaction paths.
func TestMetrics_NilMetricsSafe(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 512, SegMax: 1 << 20})
	require.NoError(t, err, "NewStore")
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, 100)
	for i := 0; i < 5; i++ {
		_, _, _ = s.Put(testkey.From(uint64(i)), body) // some may be rejected, must not panic
	}
	err = s.Compact()
	require.NoError(t, err, "Compact with nil metrics")
}
