package origin

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds Prometheus collectors for origin health events and
// connection pool diagnostics. Counters are incremented on the
// passive and active error paths. All collectors are nil-safe: a nil
// Metrics is a no-op, so tests and single-pool setups that don't
// register metrics work without special-casing.
type Metrics struct {
	passiveErrors *prometheus.CounterVec
	probeErrors   *prometheus.CounterVec
	ejections     *prometheus.CounterVec
	restores      *prometheus.CounterVec
	// ActiveConnections is the current number of in-flight origin
	// requests per pool and target. Detects pool exhaustion before
	// 502s appear.
	ActiveConnections *prometheus.GaugeVec
	// RequestDuration measures origin response time, separate from
	// bouine's handler latency. Labels: pool, target, status.
	RequestDuration *prometheus.HistogramVec
	// ConnectionErrors counts connection failures by reason (timeout,
	// refused, reset). Distinguishes origin-side failures from
	// pool exhaustion.
	ConnectionErrors *prometheus.CounterVec
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
			Help:      "Passive health errors (5xx responses and connection errors) per upstream target, by error status.",
		}, []string{"pool", "target", "status"}),
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
		ActiveConnections: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "bouine",
			Name:      "origin_active_connections",
			Help:      "Current in-flight origin requests per pool and target. A value near the connection pool cap indicates pool exhaustion.",
		}, []string{"pool", "target"}),
		RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "bouine",
			Name:      "origin_request_duration_seconds",
			Help:      "Origin response time per pool, target, and status. Separate from bouine's handler latency.",
			Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"pool", "target", "status"}),
		ConnectionErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "origin_connection_errors_total",
			Help:      "Origin connection failures by reason (timeout, refused, reset). Distinguishes origin-side failures from pool exhaustion.",
		}, []string{"pool", "target", "reason"}),
	}
	reg.MustRegister(m.passiveErrors, m.probeErrors, m.ejections, m.restores,
		m.ActiveConnections, m.RequestDuration, m.ConnectionErrors)
	return m
}

// incPassiveError increments the passive error counter for the given
// pool, target, and status (e.g., "502", "503", "timeout"). Nil-safe.
func (m *Metrics) incPassiveError(pool, target, status string) {
	if m == nil || m.passiveErrors == nil {
		return
	}
	m.passiveErrors.WithLabelValues(pool, target, status).Inc()
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

// incActiveConnection increments the active connection gauge for the
// given pool and target. Nil-safe.
func (m *Metrics) incActiveConnection(pool, target string) {
	if m == nil || m.ActiveConnections == nil {
		return
	}
	m.ActiveConnections.WithLabelValues(pool, target).Inc()
}

// decActiveConnection decrements the active connection gauge for the
// given pool and target. Nil-safe.
func (m *Metrics) decActiveConnection(pool, target string) {
	if m == nil || m.ActiveConnections == nil {
		return
	}
	m.ActiveConnections.WithLabelValues(pool, target).Dec()
}

// observeRequestDuration records the origin response duration for the
// given pool, target, and status. Nil-safe.
func (m *Metrics) observeRequestDuration(pool, target, status string, dur float64) {
	if m == nil || m.RequestDuration == nil {
		return
	}
	m.RequestDuration.WithLabelValues(pool, target, status).Observe(dur)
}

// incConnectionError increments the connection error counter for the
// given pool, target, and reason (timeout, refused, reset). Nil-safe.
func (m *Metrics) incConnectionError(pool, target, reason string) {
	if m == nil || m.ConnectionErrors == nil {
		return
	}
	m.ConnectionErrors.WithLabelValues(pool, target, reason).Inc()
}
