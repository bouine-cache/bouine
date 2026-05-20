package observability

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"

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
	// Hot-tier storage gauges — updated on every Stats() poll by the engine.
	HotStoreBytes     prometheus.Gauge
	HotStoreEntries   prometheus.Gauge
	HotStoreEvictions prometheus.Counter
	// Cloudflare propagation counters.
	CFPurgeTotal    *prometheus.CounterVec   // labels: operation, status
	CFPurgeDuration *prometheus.HistogramVec // labels: operation
	CFPurgeSkipped  *prometheus.CounterVec   // labels: reason
}

// NewDataPlaneMetrics registers and returns the data-plane RED
// counters on the given registry.
func NewDataPlaneMetrics(reg *prometheus.Registry) *DataPlaneMetrics {
	m := &DataPlaneMetrics{
		RequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "requests_total",
			Help:      "Total number of requests processed by the data plane.",
		}, []string{"method", "status", "cache_result", "route"}),
		RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:                       "bouine",
			Name:                            "request_duration_seconds",
			Help:                            "Histogram of request durations in seconds.",
			Buckets:                         []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 15 * time.Minute,
		}, []string{"method", "status", "cache_result", "route"}),
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
	m.CFPurgeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "cloudflare_purge_total",
		Help:      "Cloudflare cache invalidation API calls by operation and status.",
	}, []string{"operation", "status"})
	m.CFPurgeDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "bouine",
		Name:      "cloudflare_purge_duration_seconds",
		Help:      "Latency of Cloudflare cache invalidation API calls.",
		Buckets:   []float64{.01, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"operation"})
	m.CFPurgeSkipped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "cloudflare_purge_skipped_total",
		Help:      "Invalidations not forwarded to Cloudflare (disabled or incompatible regex).",
	}, []string{"reason"})
	m.HotStoreBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "bouine",
		Name:      "hot_store_bytes",
		Help:      "Current number of bytes used by the hot in-memory cache tier (body + per-entry overhead).",
	})
	m.HotStoreEntries = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "bouine",
		Name:      "hot_store_entries",
		Help:      "Current number of objects stored in the hot in-memory cache tier.",
	})
	m.HotStoreEvictions = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "hot_store_evictions_total",
		Help:      "Total number of objects evicted from the hot tier by SIEVE since boot.",
	})
	reg.MustRegister(m.RequestsTotal, m.RequestDuration, m.ResponseBytesOut, m.VaryCapHits,
		m.CFPurgeTotal, m.CFPurgeDuration, m.CFPurgeSkipped,
		m.HotStoreBytes, m.HotStoreEntries, m.HotStoreEvictions)
	return m
}

// statusStrings is a pre-allocated table of HTTP status code strings
// for codes 100–599. Avoids strconv.Itoa allocation on every request.
// Index 0 → "100", index 499 → "599".
var statusStrings [500]string

// statusString returns the decimal string for an HTTP status code
// with zero allocation for codes 100–599.
func statusString(code int) string {
	if code >= 100 && code < 600 {
		s := statusStrings[code-100]
		if s != "" {
			return s
		}
	}
	// Fallback (unreachable for valid HTTP status codes).
	return "0"
}

func init() {
	for i := range statusStrings {
		code := i + 100
		// Manual integer-to-string, no allocation.
		hundreds := code / 100
		tens := (code % 100) / 10
		ones := code % 10
		statusStrings[i] = string([]byte{
			byte('0' + hundreds),
			byte('0' + tens),
			byte('0' + ones),
		})
	}
}

// Middleware wraps an http.Handler and records RED metrics for every
// request. It sits between the access-log middleware and the pipeline
// router. When a trace is active on the request context, the duration
// histogram observation carries an exemplar with the trace_id so
// Grafana can link high-latency buckets directly to a trace.
func (m *DataPlaneMetrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := responsewriter.Acquire(w)
		defer responsewriter.Release(sw)

		next.ServeHTTP(sw, r)

		status := statusString(sw.Status)
		// Route label is written by the pipeline router as a direct header
		// map entry to avoid CanonicalMIMEHeaderKey on every request.
		route := r.Header.Get("X-Bouine-Route")
		if route == "" {
			route = "_default"
		}
		// cache_result: normalise X-Cache to HIT/MISS/STALE/REVALIDATED/BYPASS.
		cacheResult := normaliseCacheResult(w.Header().Get("X-Cache"))

		m.RequestsTotal.WithLabelValues(r.Method, status, cacheResult, route).Inc()

		// Attach an exemplar when a trace is active so Grafana can link
		// high-latency histogram buckets directly to the matching trace.
		dur := time.Since(start).Seconds()
		obs := m.RequestDuration.WithLabelValues(r.Method, status, cacheResult, route)
		if span := trace.SpanFromContext(r.Context()); span.SpanContext().IsValid() {
			if eo, ok := obs.(prometheus.ExemplarObserver); ok {
				eo.ObserveWithExemplar(dur, prometheus.Labels{
					"trace_id": span.SpanContext().TraceID().String(),
				})
			} else {
				obs.Observe(dur)
			}
		} else {
			obs.Observe(dur)
		}

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

// sampleCounter tracks requests for 1:100 access-log sampling.
// Using atomic so the counter is lock-free.
var sampleCounter atomic.Uint64

// normaliseCacheResult maps X-Cache header values to a stable Prometheus
// label. Unknown values are kept as-is (forward-compatible).
func normaliseCacheResult(xCache string) string {
	switch xCache {
	case "HIT", "MISS", "STALE", "REVALIDATED", "BYPASS":
		return xCache
	case "":
		return "MISS"
	default:
		return xCache
	}
}

// ShouldLogAccess reports whether this request should be written to
// the access log. Only 200-OK responses are sampled at 1:100; all
// other status codes (redirects, errors, unusual 2xx) are always
// logged since they are rare relative to cache-hit volume.
func ShouldLogAccess(status int) bool {
	if status != http.StatusOK {
		return true
	}
	return sampleCounter.Add(1)%100 == 0
}
