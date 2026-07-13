package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

// StartupMetrics holds Prometheus collectors for the startup path.
// These are gauges and a histogram with no labels (or a single label
// with a bounded set of values), well within the cardinality budget.
type StartupMetrics struct {
	// StartupPhase is the current startup phase:
	// 0=init, 1=loading_warm, 2=loading_wal, 3=recompute_stats,
	// 4=cluster_join, 5=ready.
	StartupPhase *prometheus.GaugeVec
	// StartupConditionReady reports 1 when a named readiness condition
	// is true, 0 when false. Label: condition (bounded to ≤4 values).
	StartupConditionReady *prometheus.GaugeVec
	// StartupDurationSeconds measures total startup time from process
	// start to all readiness conditions met.
	StartupDurationSeconds prometheus.Histogram
}

// NewStartupMetrics creates and registers startup metrics on the given
// registry. Nil registry is treated as a no-op (metrics are created but
// not registered — useful for tests).
func NewStartupMetrics(reg *prometheus.Registry) *StartupMetrics {
	m := &StartupMetrics{
		StartupPhase: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "bouine",
			Name:      "startup_phase",
			Help:      "Current startup phase: 0=init, 1=loading_warm, 2=loading_wal, 3=recompute_stats, 4=cluster_join, 5=ready.",
		}, []string{"phase"}),
		StartupConditionReady: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "bouine",
			Name:      "startup_condition_ready",
			Help:      "Readiness condition status: 1=ready, 0=not-ready. Label 'condition' is bounded to startup gate names.",
		}, []string{"condition"}),
		StartupDurationSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "bouine",
			Name:      "startup_duration_seconds",
			Help:      "Total startup duration from process start to all readiness conditions met.",
			Buckets:   prometheus.ExponentialBuckets(0.1, 2, 12),
		}),
	}
	if reg != nil {
		reg.MustRegister(
			m.StartupPhase,
			m.StartupConditionReady,
			m.StartupDurationSeconds,
		)
	}
	return m
}

// SetPhase sets the current startup phase gauge.
func (m *StartupMetrics) SetPhase(phase string) {
	if m == nil || m.StartupPhase == nil {
		return
	}
	phaseMap := map[string]float64{
		"init":            0,
		"loading_warm":    1,
		"loading_wal":     2,
		"recompute_stats": 3,
		"cluster_join":    4,
		"ready":           5,
	}
	val, ok := phaseMap[phase]
	if !ok {
		return
	}
	m.StartupPhase.WithLabelValues(phase).Set(val)
}

// SetCondition sets the readiness condition gauge for a named condition.
func (m *StartupMetrics) SetCondition(name string, ready bool) {
	if m == nil || m.StartupConditionReady == nil {
		return
	}
	val := float64(0)
	if ready {
		val = 1
	}
	m.StartupConditionReady.WithLabelValues(name).Set(val)
}

// ObserveStartupDuration records the total startup duration.
func (m *StartupMetrics) ObserveStartupDuration(seconds float64) {
	if m == nil {
		return
	}
	m.StartupDurationSeconds.Observe(seconds)
}
