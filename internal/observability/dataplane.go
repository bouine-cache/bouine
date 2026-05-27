package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/thylong/bouine/internal/observability/responsewriter"
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
	VaryCapHits      prometheus.Counter // incremented when MaxVariants cap is hit
	Rings            *Rings             // nil when dashboard is disabled
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
		VaryCapHits: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "vary_cap_hits_total",
			Help:      "Number of Vary variant storage attempts rejected because MaxVariants was exceeded.",
		}),
	}
	reg.MustRegister(m.RequestsTotal, m.RequestDuration, m.ResponseBytesOut, m.VaryCapHits)
	return m
}

// Middleware wraps an http.Handler and records RED metrics for every
// request. It sits between the access-log middleware and the pipeline
// router.
func (m *DataPlaneMetrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := responsewriter.New(w)

		next.ServeHTTP(sw, r)

		status := strconv.Itoa(sw.Status)
		route := r.Header.Get("X-Bouine-Route")
		if route == "" {
			route = "_default"
		}

		m.RequestsTotal.WithLabelValues(r.Method, status, route).Inc()
		m.RequestDuration.WithLabelValues(r.Method, status, route).
			Observe(time.Since(start).Seconds())
		m.ResponseBytesOut.WithLabelValues(r.Method, route).
			Add(float64(sw.Bytes))

		// Update ring buffers for the dashboard (if enabled).
		if m.Rings != nil {
			xCache := w.Header().Get("X-Cache")
			durMs := time.Since(start).Milliseconds()
			m.Rings.Request.RecordRequest(xCache, sw.Status, durMs)
			if route != "_default" {
				m.Rings.Route.RecordRoute(route, xCache)
			}
			m.Rings.URL.RecordURL(r.URL.Path, route, xCache)
		}
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
