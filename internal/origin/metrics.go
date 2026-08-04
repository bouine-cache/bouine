package origin

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds Prometheus collectors for origin health events. Counters
// are incremented on the passive and active error paths. All counters are
// nil-safe: a nil Metrics is a no-op, so tests and single-pool setups
// that don't register metrics work without special-casing.
type Metrics struct {
	passiveErrors *prometheus.CounterVec
	probeErrors   *prometheus.CounterVec
	ejections     *prometheus.CounterVec
	restores      *prometheus.CounterVec
}

// RegisterMetrics creates and registers origin Prometheus collectors on
// the provided registerer. Returns a Metrics handle safe for concurrent
// use. A nil registerer returns a zero-value Metrics (all no-ops).
func RegisterMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		return &Metrics{}
	}
	m := &Metrics{
		passiveErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "origin_passive_errors_total",
			Help:      "Passive health errors (5xx responses and connection errors) per upstream target.",
		}, []string{"pool", "target"}),
		probeErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "origin_probe_errors_total",
			Help:      "Active health probe failures per upstream target.",
		}, []string{"pool", "target"}),
		ejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "origin_ejections_total",
			Help:      "Target ejections by health check source (passive or active).",
		}, []string{"pool", "target", "source"}),
		restores: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "origin_restores_total",
			Help:      "Target restores by health check source (active or manual).",
		}, []string{"pool", "target", "source"}),
	}
	reg.MustRegister(m.passiveErrors, m.probeErrors, m.ejections, m.restores)
	return m
}

// incPassiveError increments the passive error counter. Nil-safe.
func (m *Metrics) incPassiveError(pool, target string) {
	if m == nil || m.passiveErrors == nil {
		return
	}
	m.passiveErrors.WithLabelValues(pool, target).Inc()
}

// incProbeError increments the probe error counter. Nil-safe.
func (m *Metrics) incProbeError(pool, target string) {
	if m == nil || m.probeErrors == nil {
		return
	}
	m.probeErrors.WithLabelValues(pool, target).Inc()
}

// incEjection increments the ejection counter. Nil-safe.
func (m *Metrics) incEjection(pool, target, source string) {
	if m == nil || m.ejections == nil {
		return
	}
	m.ejections.WithLabelValues(pool, target, source).Inc()
}

// incRestore increments the restore counter. Nil-safe.
func (m *Metrics) incRestore(pool, target, source string) {
	if m == nil || m.restores == nil {
		return
	}
	m.restores.WithLabelValues(pool, target, source).Inc()
}
