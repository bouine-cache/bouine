package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// DataPlaneMetrics holds the RED counters for the data-plane pipeline.
// Injected by the engine; consumed by the pipeline and access-log
// middleware.
//
// Stable.
type DataPlaneMetrics struct {
	RequestsTotal    *prometheus.CounterVec
	RequestDuration  *prometheus.HistogramVec
	ResponseBytesOut *prometheus.CounterVec
}

// NewDataPlaneMetrics registers and returns the data-plane RED
// counters on the given registry.
func NewDataPlaneMetrics(reg *prometheus.Registry) *DataPlaneMetrics {
	m := &DataPlaneMetrics{
		RequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "requests_total",
			Help:      "Total number of requests processed by the data plane.",
		}, []string{"method", "status", "route"}),
		RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "bouine",
			Name:      "request_duration_seconds",
			Help:      "Histogram of request durations in seconds.",
			Buckets:   []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"method", "status", "route"}),
		ResponseBytesOut: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "response_bytes_total",
			Help:      "Total bytes written in responses.",
		}, []string{"method", "route"}),
	}
	reg.MustRegister(m.RequestsTotal, m.RequestDuration, m.ResponseBytesOut)
	return m
}

// Middleware wraps an http.Handler and records RED metrics for every
// request. It sits between the access-log middleware and the pipeline
// router.
func (m *DataPlaneMetrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &metricsWriter{ResponseWriter: w, status: 200}

		next.ServeHTTP(sw, r)

		status := strconv.Itoa(sw.status)
		route := r.Header.Get("X-Bouine-Route")
		if route == "" {
			route = "_default"
		}

		m.RequestsTotal.WithLabelValues(r.Method, status, route).Inc()
		m.RequestDuration.WithLabelValues(r.Method, status, route).
			Observe(time.Since(start).Seconds())
		m.ResponseBytesOut.WithLabelValues(r.Method, route).
			Add(float64(sw.bytes))
	})
}

type metricsWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (w *metricsWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *metricsWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, err
}
