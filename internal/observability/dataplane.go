package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel/trace"

	"github.com/thylong/bouine/internal/observability/responsewriter"
	"github.com/thylong/bouine/pkg/api"
	"github.com/thylong/bouine/pkg/header"
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
	// Warm-tier storage gauges — updated on every Stats() poll by the engine.
	WarmStoreBytes     prometheus.Gauge
	WarmStoreEntries   prometheus.Gauge
	WarmStoreSelfHeals prometheus.Counter
	// Cloudflare propagation counters.
	CFPurgeTotal    *prometheus.CounterVec   // labels: operation, status
	CFPurgeDuration *prometheus.HistogramVec // labels: operation
	CFPurgeSkipped  *prometheus.CounterVec   // labels: reason
	// Refresh-before-expiry metrics. Nil when no route enables the feature.
	RefreshTotal        *prometheus.CounterVec // labels: route, result
	RefreshErrorsTotal  *prometheus.CounterVec // labels: route, error_type
	RefreshSkipsTotal   *prometheus.CounterVec // labels: route, reason
	RefreshInFlight     *prometheus.GaugeVec   // labels: route
	RefreshScheduled    *prometheus.GaugeVec   // labels: route
	RefreshRegistrySize *prometheus.GaugeVec   // labels: route
	// WAL async metrics. WALDroppedEntries is a counter; the engine
	// polls the WAL log's DroppedEntries() and adds the delta.
	// WALLastSyncTimestamp is a gauge set from the WAL log's LastSyncTime.
	WALDroppedEntries    prometheus.Counter
	WALLastSyncTimestamp prometheus.Gauge
}

// NewDataPlaneMetrics registers and returns the data-plane RED
// counters on the given registry.
func NewDataPlaneMetrics(reg *prometheus.Registry) *DataPlaneMetrics {
	m := &DataPlaneMetrics{
		RequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "requests_total",
			Help:      "Total number of requests processed by the data plane.",
		}, []string{"method", "status", "cache_result", "source", "route"}),
		RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:                       "bouine",
			Name:                            "request_duration_seconds",
			Help:                            "Histogram of request durations in seconds.",
			Buckets:                         []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 15 * time.Minute,
		}, []string{"method", "status", "cache_result", "source", "route"}),
		ResponseBytesOut: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "response_bytes_total",
			Help:      "Total bytes written in responses.",
		}, []string{"method", "cache_result", "source", "route"}),
		VaryCapHits: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "vary_cap_hits_total",
			Help:      "Number of Vary variant storage attempts rejected because MaxVariants was exceeded.",
		}),
	}
	m.initCFPurgeMetrics()
	m.HotStoreBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "bouine",
		Name:      "hot_store_bytes",
		Help:      "Estimated bytes used by the hot in-memory cache tier for eviction budgeting (body + headers + struct + map overhead). Not a runtime memory metric; for heap usage see go_memstats_heap_alloc_bytes.",
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
	m.WarmStoreBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "bouine",
		Name:      "warm_store_bytes",
		Help:      "Total bytes used by warm-tier disk segments (append-only, pre-compaction).",
	})
	m.WarmStoreEntries = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "bouine",
		Name:      "warm_store_entries",
		Help:      "Current number of objects stored in the warm (L1) disk tier.",
	})
	m.WarmStoreSelfHeals = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "warm_store_self_heals_total",
		Help:      "Total stale warm-tier index entries dropped by the self-heal path since boot. A non-zero rate indicates segment-management bugs or disk faults.",
	})
	m.initRefreshMetrics()
	m.initWALMetrics()
	reg.MustRegister(m.RequestsTotal, m.RequestDuration, m.ResponseBytesOut, m.VaryCapHits,
		m.CFPurgeTotal, m.CFPurgeDuration, m.CFPurgeSkipped,
		m.HotStoreBytes, m.HotStoreEntries, m.HotStoreEvictions,
		m.WarmStoreBytes, m.WarmStoreEntries, m.WarmStoreSelfHeals,
		m.RefreshTotal, m.RefreshErrorsTotal, m.RefreshSkipsTotal,
		m.RefreshInFlight, m.RefreshScheduled, m.RefreshRegistrySize,
		m.WALDroppedEntries, m.WALLastSyncTimestamp)
	return m
}

// initCFPurgeMetrics creates the Cloudflare purge collectors on m.
// Called from NewDataPlaneMetrics; extracted to keep that function under
// the funlen limit.
func (m *DataPlaneMetrics) initCFPurgeMetrics() {
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
}

// initWALMetrics creates the async-WAL collectors on m.
// Called from NewDataPlaneMetrics; extracted to keep that function under
// the funlen limit.
func (m *DataPlaneMetrics) initWALMetrics() {
	m.WALDroppedEntries = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "wal_dropped_entries_total",
		Help:      "WAL entries dropped because the async channel was full. Any non-zero rate means the sync interval is too long for the write rate, or the disk is too slow.",
	})
	m.WALLastSyncTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "bouine",
		Name:      "wal_last_sync_timestamp_seconds",
		Help:      "Unix timestamp of the last successful WAL fsync. If stale by more than 2x wal_sync_interval, the sync loop may be stuck.",
	})
}

// initRefreshMetrics creates the refresh-before-expiry collectors on m.
// Called from NewDataPlaneMetrics; extracted to keep that function under
// the funlen limit.
func (m *DataPlaneMetrics) initRefreshMetrics() {
	m.RefreshTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "refresh_total",
		Help:      "Background refresh fetches by result (304, 200, error).",
	}, []string{"route", "result"})
	m.RefreshErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "refresh_errors_total",
		Help:      "Failed background refresh fetches by error type.",
	}, []string{"route", "error_type"})
	m.RefreshSkipsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "refresh_skips_total",
		Help:      "Skipped background refreshes by reason (evicted, stale, semaphore_full, not_found, negative).",
	}, []string{"route", "reason"})
	m.RefreshInFlight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bouine",
		Name:      "refresh_in_flight",
		Help:      "Current in-flight background refresh goroutines.",
	}, []string{"route"})
	m.RefreshScheduled = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bouine",
		Name:      "refresh_scheduled",
		Help:      "Entries currently in the refresh scheduler heap.",
	}, []string{"route"})
	m.RefreshRegistrySize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bouine",
		Name:      "refresh_registry_size",
		Help:      "Entries currently in the refresh registry.",
	}, []string{"route"})
}

// VaryCapHitsCount returns the current Vary cap hit counter value as int64.
// Used by the dashboard insights engine to detect Vary explosion.
func (m *DataPlaneMetrics) VaryCapHitsCount() int64 {
	if m == nil {
		return 0
	}
	var d dto.Metric
	_ = m.VaryCapHits.(prometheus.Metric).Write(&d)
	return int64(d.GetCounter().GetValue())
}

// CFPurgeSkippedCount returns the total CF purge skip count across all
// reasons by summing the CounterVec label combinations.
func (m *DataPlaneMetrics) CFPurgeSkippedCount() int64 {
	if m == nil || m.CFPurgeSkipped == nil {
		return 0
	}
	var total int64
	for _, reason := range []string{"rate_limited", "batch_full", "disabled"} {
		var d dto.Metric
		if err := m.CFPurgeSkipped.WithLabelValues(reason).(prometheus.Metric).Write(&d); err == nil {
			total += int64(d.GetCounter().GetValue())
		}
	}
	return total
}

// RefreshMetrics is the subset of data-plane metrics for the refresh-before-expiry
// feature. Passed to cache.Handler so it can record background refresh activity
// without a direct dependency on prometheus types.
type RefreshMetrics struct {
	Total        *prometheus.CounterVec // labels: route, result
	Errors       *prometheus.CounterVec // labels: route, error_type
	Skips        *prometheus.CounterVec // labels: route, reason
	InFlight     *prometheus.GaugeVec   // labels: route
	Scheduled    *prometheus.GaugeVec   // labels: route
	RegistrySize *prometheus.GaugeVec   // labels: route
}

// RefreshMetricsForRoute returns nil-safe labelled metric accessors bound to
// a specific route name. The handler calls these directly; each is a no-op
// when the underlying vec is nil (feature disabled or no route opted in).
type RefreshMetricsForRoute struct {
	IncTotal        func(result string)
	IncErrors       func(errorType string)
	IncSkips        func(reason string)
	IncInFlight     func()
	DecInFlight     func()
	SetScheduled    func(float64)
	SetRegistrySize func(float64)
}

// ForRoute returns a RefreshMetricsForRoute bound to the given route label.
// If m is nil or the underlying vecs are nil (metrics disabled), returns
// no-op closures.
func (m *RefreshMetrics) ForRoute(route string) RefreshMetricsForRoute {
	if m == nil || m.Total == nil {
		return RefreshMetricsForRoute{
			IncTotal:        func(string) {},
			IncErrors:       func(string) {},
			IncSkips:        func(string) {},
			IncInFlight:     func() {},
			DecInFlight:     func() {},
			SetScheduled:    func(float64) {},
			SetRegistrySize: func(float64) {},
		}
	}
	return RefreshMetricsForRoute{
		IncTotal:        func(result string) { m.Total.WithLabelValues(route, result).Inc() },
		IncErrors:       func(errType string) { m.Errors.WithLabelValues(route, errType).Inc() },
		IncSkips:        func(reason string) { m.Skips.WithLabelValues(route, reason).Inc() },
		IncInFlight:     func() { m.InFlight.WithLabelValues(route).Inc() },
		DecInFlight:     func() { m.InFlight.WithLabelValues(route).Dec() },
		SetScheduled:    func(v float64) { m.Scheduled.WithLabelValues(route).Set(v) },
		SetRegistrySize: func(v float64) { m.RegistrySize.WithLabelValues(route).Set(v) },
	}
}

// RefreshMetricsVec returns the refresh metrics as a RefreshMetrics struct
// for passing to cache handlers. Returns nil if refresh metrics are nil.
func (m *DataPlaneMetrics) RefreshMetricsVec() *RefreshMetrics {
	if m == nil || m.RefreshTotal == nil {
		return nil
	}
	return &RefreshMetrics{
		Total:        m.RefreshTotal,
		Errors:       m.RefreshErrorsTotal,
		Skips:        m.RefreshSkipsTotal,
		InFlight:     m.RefreshInFlight,
		Scheduled:    m.RefreshScheduled,
		RegistrySize: m.RefreshRegistrySize,
	}
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
		route := r.Header.Get(header.XBouineRoute)
		if route == "" {
			route = "_default"
		}
		// cache_result: normalise X-Cache to HIT/MISS/STALE/REVALIDATED/BYPASS.
		cacheResult := normaliseCacheResult(w.Header().Get(header.XCache))
		source := normaliseSource(w.Header().Get(header.XCacheSource))

		m.RequestsTotal.WithLabelValues(r.Method, status, cacheResult, source, route).Inc()

		// Attach an exemplar when a trace is active so Grafana can link
		// high-latency histogram buckets directly to the matching trace.
		dur := time.Since(start).Seconds()
		obs := m.RequestDuration.WithLabelValues(r.Method, status, cacheResult, source, route)
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

		m.ResponseBytesOut.WithLabelValues(r.Method, cacheResult, source, route).
			Add(float64(sw.Bytes))

		// Update ring buffers for the dashboard (if enabled).
		if m.Rings != nil {
			xCache := w.Header().Get(header.XCache)
			durMs := time.Since(start).Milliseconds()
			m.Rings.Request.RecordRequest(xCache, sw.Status, durMs)
			if route != "_default" {
				m.Rings.Route.RecordRoute(route, xCache, sw.Status, durMs)
			}
			m.Rings.URL.RecordURL(r.URL.Path, route, xCache)
			// Sample origin response headers on miss/bypass (origin was contacted).
			if m.Rings.HeaderRing != nil && (xCache == "MISS" || xCache == "BYPASS") {
				m.Rings.HeaderRing.Sample(route, w.Header(), sw.Status)
			}
		}
	})
}

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

// normaliseSource maps X-Cache-Source header values to a stable Prometheus
// label. Empty string is preserved (BYPASS, only-if-cached 504). Unknown
// values default to empty for forward-compatibility.
func normaliseSource(xCacheSource string) string {
	switch xCacheSource {
	case string(api.SourceHot), string(api.SourceWarm), string(api.SourcePeer), string(api.SourceOrigin):
		return xCacheSource
	default:
		return ""
	}
}
