package warm

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds Prometheus collectors for warm-tier disk pressure.
// Nil pointers are safe to use — Inc/Gauge.Set are no-ops when the
// underlying collector is nil. This lets single-node mode (no warm
// tier) skip registration entirely.
//
// Stable.
type Metrics struct {
	// DiskBytes is the total on-disk size of all warm segment files,
	// including live records, tombstones, and superseded entries.
	// Polluted by the engine's store-metrics goroutine from
	// Store.DiskBytes().
	DiskBytes prometheus.Gauge
	// MaxBytes is the configured warm-tier byte budget. 0 means
	// unlimited (no enforcement). Set once at construction.
	MaxBytes prometheus.Gauge
	// OverBudget counts warm-tier Put rejections due to ErrOverBudget.
	// A sustained rate indicates the warm tier is full and eviction
	// cannot keep up (all remaining entries are protected or the
	// record is larger than the budget).
	OverBudget prometheus.Counter
	// Evictions counts warm-tier SIEVE evictions. Paired with
	// OverBudget, it shows whether the eviction policy is keeping up
	// with writes.
	Evictions prometheus.Counter
	// CompactionTriggered counts warm-tier Compact() calls. The
	// production compactLoop only calls Compact when NeedsCompaction
	// returns true, but the counter itself is not gated on that
	// check — direct callers (tests, admin) increment it regardless.
	CompactionTriggered prometheus.Counter
}

// RegisterMetrics creates and registers the warm-tier metrics on the
// given registry. Pass nil to disable metrics (no warm tier).
func RegisterMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		return &Metrics{}
	}
	m := &Metrics{
		DiskBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bouine",
			Name:      "warm_disk_bytes",
			Help:      "Total on-disk size of all warm-tier segment files, including tombstones and superseded entries.",
		}),
		MaxBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bouine",
			Name:      "warm_max_bytes",
			Help:      "Configured warm-tier byte budget (0 = unlimited). Put is rejected with ErrOverBudget when live bytes exceed this limit and eviction cannot free space.",
		}),
		OverBudget: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "warm_over_budget_total",
			Help:      "Warm-tier Put rejections due to ErrOverBudget (either the record exceeds the total budget, or eviction could not free enough space).",
		}),
		Evictions: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "warm_evictions_total",
			Help:      "Warm-tier entries evicted by SIEVE since boot.",
		}),
		CompactionTriggered: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "warm_compaction_triggered_total",
			Help:      "Warm-tier Compact() calls since boot. The production compactLoop only calls Compact when NeedsCompaction is true, but direct callers (tests, admin) increment it regardless.",
		}),
	}
	reg.MustRegister(m.DiskBytes, m.MaxBytes, m.OverBudget, m.Evictions, m.CompactionTriggered)
	return m
}

// IncOverBudget increments the over-budget rejection counter. Safe to
// call on a nil Metrics.
func (m *Metrics) IncOverBudget() {
	if m == nil || m.OverBudget == nil {
		return
	}
	m.OverBudget.Inc()
}

// IncEvictions increments the warm-tier eviction counter. Safe to call
// on a nil Metrics.
func (m *Metrics) IncEvictions() {
	if m == nil || m.Evictions == nil {
		return
	}
	m.Evictions.Inc()
}

// IncCompactionTriggered increments the compaction-triggered counter.
// Safe to call on a nil Metrics.
func (m *Metrics) IncCompactionTriggered() {
	if m == nil || m.CompactionTriggered == nil {
		return
	}
	m.CompactionTriggered.Inc()
}

// SetDiskBytes sets the current total on-disk segment size gauge. Safe
// to call on a nil Metrics.
func (m *Metrics) SetDiskBytes(bytes int64) {
	if m == nil || m.DiskBytes == nil {
		return
	}
	m.DiskBytes.Set(float64(bytes))
}

// SetMaxBytes sets the configured warm-tier byte budget gauge. Safe to
// call on a nil Metrics.
func (m *Metrics) SetMaxBytes(bytes int64) {
	if m == nil || m.MaxBytes == nil {
		return
	}
	m.MaxBytes.Set(float64(bytes))
}
