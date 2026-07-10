package warm

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// metricValue returns the value of a single unlabeled counter or gauge
// from the registry, or 0 if the metric is not found.
func metricValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
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
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
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
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Fill the budget with protected entries so eviction cannot free space.
	smallBody := make([]byte, 100) // 120 bytes per record
	for i := 0; i < 4; i++ {
		if _, _, err := s.Put(uint64(i), smallBody); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	for i := 0; i < 4; i++ {
		s.Protect(uint64(i))
	}

	// This Put must be rejected with ErrOverBudget.
	_, _, err = s.Put(99, smallBody)
	if !errors.Is(err, ErrOverBudget) {
		t.Fatalf("Put over budget: err=%v, want ErrOverBudget", err)
	}

	got := metricValue(t, reg, "bouine_warm_over_budget_total")
	if got != 1 {
		t.Errorf("over_budget_total = %v, want 1", got)
	}
}

func TestMetrics_EvictionsIncrements(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	dir := t.TempDir()
	// Small budget so Put triggers eviction.
	s, err := NewStore(Config{Dir: dir, MaxBytes: 360, SegMax: 1 << 20, Metrics: m})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	smallBody := make([]byte, 100) // 120 bytes per record
	// 3 records × 120 = 360 = budget. The 4th Put must evict to fit.
	for i := 0; i < 3; i++ {
		if _, _, err := s.Put(uint64(i), smallBody); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if _, _, err := s.Put(99, smallBody); err != nil {
		t.Fatalf("Put 99 with eviction: %v", err)
	}

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
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	got := metricValue(t, reg, "bouine_warm_compaction_triggered_total")
	if got != 1 {
		t.Errorf("compaction_triggered_total = %v, want 1", got)
	}
}

func TestMetrics_DiskBytesMatchesSegmentSizes(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 0, SegMax: 1 << 20, Metrics: m})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, 100) // 120 bytes per record
	for i := 0; i < 5; i++ {
		if _, _, err := s.Put(uint64(i), body); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// DiskBytes should equal the sum of segment file sizes, which for 5
	// records of 120 bytes each is 600 bytes.
	got := s.DiskBytes()
	want := int64(5 * (HeaderLen + len(body) + FooterLen))
	if got != want {
		t.Errorf("DiskBytes = %d, want %d", got, want)
	}

	// Verify the gauge is not yet set (engine polls it). Set it and check.
	m.SetDiskBytes(got)
	regVal := metricValue(t, reg, "bouine_warm_disk_bytes")
	if regVal != float64(want) {
		t.Errorf("warm_disk_bytes gauge = %v, want %v", regVal, float64(want))
	}
}

func TestMetrics_MaxBytesGauge(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	dir := t.TempDir()
	const maxBytes = 1 << 20 // 1 MiB
	s, err := NewStore(Config{Dir: dir, MaxBytes: maxBytes, SegMax: 1 << 20, Metrics: m})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if got := s.MaxBytes(); got != maxBytes {
		t.Errorf("MaxBytes = %d, want %d", got, maxBytes)
	}

	regVal := metricValue(t, reg, "bouine_warm_max_bytes")
	if regVal != float64(maxBytes) {
		t.Errorf("warm_max_bytes gauge = %v, want %v", regVal, float64(maxBytes))
	}
}

func TestMetrics_MaxBytesZeroGauge(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 0, SegMax: 1 << 20, Metrics: m})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	regVal := metricValue(t, reg, "bouine_warm_max_bytes")
	if regVal != 0 {
		t.Errorf("warm_max_bytes gauge = %v, want 0 (unlimited)", regVal)
	}
}

// TestMetrics_NilMetricsSafe verifies that a store constructed without
// metrics does not panic on Put, eviction, or compaction paths.
func TestMetrics_NilMetricsSafe(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 512, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, 100)
	for i := 0; i < 5; i++ {
		_, _, _ = s.Put(uint64(i), body) // some may be rejected, must not panic
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact with nil metrics: %v", err)
	}
}
