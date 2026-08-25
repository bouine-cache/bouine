package warm

import (
	"time"

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
	// OverBudgetBytes is the current number of bytes by which the warm
	// tier exceeds its configured budget. 0 when at or under budget.
	// Complements OverBudget (which counts events, not severity).
	OverBudgetBytes prometheus.Gauge
	// Evictions counts warm-tier SIEVE evictions. Paired with
	// OverBudget, it shows whether the eviction policy is keeping up
	// with writes.
	Evictions prometheus.Counter
	// CompactionTriggered counts warm-tier Compact() calls. The
	// production compactLoop only calls Compact when NeedsCompaction
	// returns true, but the counter itself is not gated on that
	// check — direct callers (tests, admin) increment it regardless.
	CompactionTriggered prometheus.Counter
	// CompactionDuration measures the wall time of a single compaction
	// cycle (Compact or CompactSegment). High values indicate the
	// compaction is blocking request handlers for too long.
	CompactionDuration prometheus.Histogram
	// CompactionBytesReclaimed counts dead bytes reclaimed by
	// compaction (tombstones + superseded entries removed). Measures
	// compaction efficiency when compared against CompactionTriggered.
	CompactionBytesReclaimed prometheus.Counter
	// PromotionSkipped counts hot→warm promotions that were skipped,
	// by reason (low_frequency, warm_disabled, budget_full). Used to
	// validate selective promotion effectiveness.
	PromotionSkipped *prometheus.CounterVec
	// MmapPageFaults counts major page faults on warm-tier mmap'd
	// segment reads. A high rate indicates warm reads are hitting disk
	// rather than the page cache. Linux-only; always 0 on other platforms.
	MmapPageFaults prometheus.Counter
	// MmapResidentBytes is the estimated resident memory from warm-tier
	// mmap'd segment pages. Helps diagnose RSS overruns in hot+warm
	// configurations where mmap pages are not controlled by Go's GOMEMLIMIT.
	// Linux-only; always 0 on other platforms.
	MmapResidentBytes prometheus.Gauge
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
		OverBudgetBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bouine",
			Name:      "warm_over_budget_bytes",
			Help:      "Current bytes over the warm-tier budget (0 when at or under budget). Complements warm_over_budget_total which counts events but not severity.",
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
		CompactionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "bouine",
			Name:      "warm_compaction_duration_seconds",
			Help:      "Wall time of a single warm-tier compaction cycle (Compact or CompactSegment). High values indicate compaction is blocking request handlers.",
			Buckets:   []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		CompactionBytesReclaimed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "warm_compaction_bytes_reclaimed_total",
			Help:      "Dead bytes (tombstones + superseded entries) reclaimed by warm-tier compaction since boot. Measures compaction efficiency.",
		}),
		PromotionSkipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "warm_promotion_skipped_total",
			Help:      "Hot→warm promotions skipped, by reason (low_frequency, warm_disabled, budget_full). Used to validate selective promotion effectiveness.",
		}, []string{"reason"}),
		MmapPageFaults: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "warm_mmap_page_faults_total",
			Help:      "Major page faults on warm-tier mmap'd segment reads. High rate indicates warm reads are hitting disk. Linux-only; always 0 on other platforms.",
		}),
		MmapResidentBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bouine",
			Name:      "warm_mmap_resident_bytes",
			Help:      "Estimated resident memory from warm-tier mmap'd segment pages. Helps diagnose RSS overruns in hot+warm configurations. Linux-only; always 0 on other platforms.",
		}),
	}
	reg.MustRegister(
		m.DiskBytes, m.MaxBytes, m.OverBudget, m.OverBudgetBytes,
		m.Evictions, m.CompactionTriggered,
		m.CompactionDuration, m.CompactionBytesReclaimed,
		m.PromotionSkipped,
		m.MmapPageFaults, m.MmapResidentBytes,
	)
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

// SetOverBudgetBytes sets the current bytes-over-budget gauge. Pass 0
// when the warm tier is at or under budget. Safe to call on a nil Metrics.
func (m *Metrics) SetOverBudgetBytes(bytes int64) {
	if m == nil || m.OverBudgetBytes == nil {
		return
	}
	if bytes < 0 {
		bytes = 0
	}
	m.OverBudgetBytes.Set(float64(bytes))
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

// ObserveCompactionDuration records the wall time of a compaction cycle.
// Safe to call on a nil Metrics.
func (m *Metrics) ObserveCompactionDuration(d time.Duration) {
	if m == nil || m.CompactionDuration == nil {
		return
	}
	m.CompactionDuration.Observe(d.Seconds())
}

// AddCompactionBytesReclaimed adds n bytes reclaimed by a compaction
// cycle. Safe to call on a nil Metrics.
func (m *Metrics) AddCompactionBytesReclaimed(n int64) {
	if m == nil || m.CompactionBytesReclaimed == nil {
		return
	}
	m.CompactionBytesReclaimed.Add(float64(n))
}

// IncPromotionSkipped increments the promotion-skipped counter for the
// given reason (low_frequency, warm_disabled, budget_full). Safe to
// call on a nil Metrics.
func (m *Metrics) IncPromotionSkipped(reason string) {
	if m == nil || m.PromotionSkipped == nil {
		return
	}
	m.PromotionSkipped.WithLabelValues(reason).Inc()
}

// AddPromotionSkipped adds n to the promotion-skipped counter for the
// given reason. Safe to call on a nil Metrics.
func (m *Metrics) AddPromotionSkipped(reason string, n int) {
	if m == nil || m.PromotionSkipped == nil || n <= 0 {
		return
	}
	m.PromotionSkipped.WithLabelValues(reason).Add(float64(n))
}

// IncMmapPageFaults increments the mmap page fault counter by n. Safe
// to call on a nil Metrics.
func (m *Metrics) IncMmapPageFaults(n int64) {
	if m == nil || m.MmapPageFaults == nil {
		return
	}
	m.MmapPageFaults.Add(float64(n))
}

// SetMmapResidentBytes sets the estimated resident bytes from mmap'd
// warm-tier segment pages. Safe to call on a nil Metrics.
func (m *Metrics) SetMmapResidentBytes(bytes int64) {
	if m == nil || m.MmapResidentBytes == nil {
		return
	}
	m.MmapResidentBytes.Set(float64(bytes))
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
