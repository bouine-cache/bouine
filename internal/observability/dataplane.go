package observability

import (
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// HeaderVal returns the first value for key from h via direct map access,
// avoiding the CanonicalMIMEHeaderKey allocation that http.Header.Get
// performs. The caller must pass an already-canonical key.

// DataPlaneMetrics holds the RED counters for the data-plane pipeline.
// Injected by the engine; consumed by the pipeline middleware which
// records both metrics and access log entries.
//
// Stable.
type DataPlaneMetrics struct {
	// Cloudflare token rotation metrics.
	CFTokenRotated         prometheus.Counter
	StreamingFallbackTotal prometheus.Counter
	// Streaming miss buffer metrics. The gauge tracks total bytes held
	// in live SetBodyStreamWriter tee buffers; the counter tracks how
	// many cacheable misses fell back to the synchronous buffered path
	// because the streaming memory cap was exceeded.
	StreamingBufferBytes prometheus.Gauge
	VaryCapHits          prometheus.Counter // incremented when MaxVariants cap is hit
	// HTTP smuggling rejection counter. Incremented when the h1parser
	// detects CL+TE conflict, duplicate Content-Length, or obs-fold.
	HTTPSmugglingRejected prometheus.Counter
	// accessLog receives structured access log entries. nil disables
	// access logging (used in tests and when the operator sets log
	// level above Info).
	accessLog Logger
	// RequestQueueDepth is the current number of in-flight HTTP
	// requests being processed by the data plane. A rising value
	// indicates CPU starvation before timeouts appear.
	RequestQueueDepth prometheus.Gauge
	// MetricsResetTotal counts metrics re-initialization events. A
	// non-zero value indicates the process restarted or metrics were
	// re-registered, which explains histogram count discontinuities.
	MetricsResetTotal    prometheus.Counter
	WALLastSyncTimestamp prometheus.Gauge
	// WAL async metrics. WALDroppedEntries is a counter; the engine
	// polls the WAL log's DroppedEntries() and adds the delta.
	// WALLastSyncTimestamp is a gauge set from the WAL log's LastSyncTime.
	WALDroppedEntries prometheus.Counter
	CFDLQDepth        prometheus.Gauge // current queue depth
	CFCircuitState    prometheus.Gauge // 0=closed, 1=open, 2=half_open
	// Hot-tier storage gauges — updated on every Stats() poll by the engine.
	HotStoreBytes     prometheus.Gauge
	HotStoreEntries   prometheus.Gauge
	HotStoreEvictions prometheus.Counter
	// HotStoreMaxBytes is the configured hot-tier byte budget. Set once
	// at startup from config. Enables fill ratio computation:
	// hot_store_bytes / hot_store_max_bytes.
	HotStoreMaxBytes prometheus.Gauge
	// Warm-tier storage gauges — updated on every Stats() poll by the engine.
	WarmStoreBytes     prometheus.Gauge
	WarmStoreEntries   prometheus.Gauge
	WarmStoreSelfHeals prometheus.Counter
	// WarmStoreMaxBytes is the configured warm-tier byte budget. Set
	// once at startup from config. Enables fill ratio computation:
	// warm_store_bytes / warm_store_max_bytes.
	WarmStoreMaxBytes prometheus.Gauge
	// Cloudflare circuit breaker metrics.
	CFCircuitRejected prometheus.Counter     // calls rejected because circuit open
	CFTokenAvailable  prometheus.Gauge       // number of tokens not in cooldown
	CFBatchDeduped    *prometheus.CounterVec // labels: kind
	CFDLQExpired      *prometheus.CounterVec // labels: kind
	RequestsTotal     *prometheus.CounterVec
	CFBatchFlushErr   *prometheus.CounterVec   // labels: kind, error_type
	CFPurgeSkipped    *prometheus.CounterVec   // labels: reason
	CFPurgeDuration   *prometheus.HistogramVec // labels: operation
	// Cloudflare propagation counters.
	CFPurgeTotal    *prometheus.CounterVec // labels: operation, status
	RequestDuration *prometheus.HistogramVec
	// Cloudflare retry queue (DLQ) metrics.
	CFDLQEnqueued *prometheus.CounterVec // labels: kind
	CFDLQDropped  *prometheus.CounterVec // labels: kind
	CFDLQRetried  *prometheus.CounterVec // labels: kind
	Rings         *Rings                 // nil when dashboard is disabled
	routeIDs      map[string]int
	// Refresh-before-expiry metrics. Nil when no route enables the feature.
	RefreshTotal        *prometheus.CounterVec // labels: route, result
	RefreshErrorsTotal  *prometheus.CounterVec // labels: route, error_type
	RefreshSkipsTotal   *prometheus.CounterVec // labels: route, reason
	RefreshInFlight     *prometheus.GaugeVec   // labels: route
	RefreshScheduled    *prometheus.GaugeVec   // labels: route
	RefreshRegistrySize *prometheus.GaugeVec   // labels: route
	// Cloudflare batching metrics.
	CFBatchFlushed *prometheus.CounterVec // labels: kind
	// nowFunc returns the current time. Defaults to time.Now; the engine
	// injects platform.CoarseNow (~2-4ns vs ~25-40ns) to reduce hit-path
	// CPU cost. Injected rather than imported directly to respect the L7
	// layering rule (observability cannot import internal/platform).
	nowFunc          func() time.Time
	ResponseBytesOut *prometheus.CounterVec
	// routeTable holds pre-resolved Prometheus collectors indexed by route
	// ID, eliminating per-request WithLabelValues hash lookups for common
	// label tuples. nil when PreResolveRoutes has not been called (tests,
	// minimal configs). When nil, the middleware falls back to
	// WithLabelValues for all requests.
	routeTable []*routeMetrics
	// accessSampleRate is the 1-in-N sampling rate for Info-level access
	// log entries. 0 means always log (no sampling). The cache key is
	// used for deterministic sampling so the same key is always logged
	// or always skipped.
	accessSampleRate uint64
	accessCounter    atomic.Uint64
	fallbackCount    atomic.Uint64
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
			Namespace:                      "bouine",
			Name:                           "request_duration_seconds",
			Help:                           "Histogram of request durations in seconds.",
			Buckets:                        []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			NativeHistogramBucketFactor:    1.1,
			NativeHistogramMaxBucketNumber: 100,
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
	m.HotStoreMaxBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "bouine",
		Name:      "hot_store_max_bytes",
		Help:      "Configured hot-tier byte budget. Set once at startup. Compute fill ratio: hot_store_bytes / hot_store_max_bytes.",
	})
	m.WarmStoreMaxBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "bouine",
		Name:      "warm_store_max_bytes",
		Help:      "Configured warm-tier byte budget. Set once at startup. Compute fill ratio: warm_store_bytes / warm_store_max_bytes.",
	})
	m.MetricsResetTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "metrics_reset_total",
		Help:      "Metrics re-initialization events. Non-zero indicates the process restarted or metrics were re-registered, explaining histogram count discontinuities.",
	})
	m.RequestQueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "bouine",
		Name:      "request_queue_depth",
		Help:      "Current number of in-flight HTTP requests being processed by the data plane. A rising value indicates CPU starvation before timeouts appear.",
	})
	m.initRefreshMetrics()
	m.initWALMetrics()
	m.initStreamingMetrics()
	m.HTTPSmugglingRejected = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "http_smuggling_rejected_total",
		Help:      "Total HTTP smuggling attempts rejected by the h1parser (CL+TE conflict, duplicate Content-Length, obs-fold).",
	})
	reg.MustRegister(m.RequestsTotal, m.RequestDuration, m.ResponseBytesOut, m.VaryCapHits,
		m.CFPurgeTotal, m.CFPurgeDuration, m.CFPurgeSkipped,
		m.CFBatchFlushed, m.CFBatchDeduped, m.CFBatchFlushErr,
		m.CFTokenRotated, m.CFTokenAvailable,
		m.CFCircuitRejected, m.CFCircuitState,
		m.CFDLQEnqueued, m.CFDLQDropped, m.CFDLQRetried, m.CFDLQExpired, m.CFDLQDepth,
		m.HotStoreBytes, m.HotStoreEntries, m.HotStoreEvictions, m.HotStoreMaxBytes,
		m.WarmStoreBytes, m.WarmStoreEntries, m.WarmStoreSelfHeals, m.WarmStoreMaxBytes,
		m.RefreshTotal, m.RefreshErrorsTotal, m.RefreshSkipsTotal,
		m.RefreshInFlight, m.RefreshScheduled, m.RefreshRegistrySize,
		m.WALDroppedEntries, m.WALLastSyncTimestamp,
		m.MetricsResetTotal, m.RequestQueueDepth,
		m.HTTPSmugglingRejected,
		m.StreamingBufferBytes, m.StreamingFallbackTotal)
	return m
}

// initStreamingMetrics creates the streaming buffer gauge and fallback
// counter. Called by NewDataPlaneMetrics; extracted to keep
// NewDataPlaneMetrics under the funlen limit.
func (m *DataPlaneMetrics) initStreamingMetrics() {
	m.StreamingBufferBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "bouine",
		Name:      "streaming_buffer_bytes",
		Help:      "Total bytes held in live streaming tee buffers across concurrent streamMissTee calls.",
	})
	m.StreamingFallbackTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "streaming_fallback_total",
		Help:      "Cacheable misses that fell back to synchronous buffering because the streaming memory cap was exceeded.",
	})
}

// SetNowFunc injects a custom time function for the metrics middleware.
// The engine calls this with platform.CoarseNow on Linux to reduce
// hit-path CPU cost (~2-4ns vs ~25-40ns for time.Now).
func (m *DataPlaneMetrics) SetNowFunc(fn func() time.Time) {
	if fn == nil {
		fn = time.Now
	}
	m.nowFunc = fn
}

// Label dimension sizes for the pre-resolved metrics array.
const (
	metricMethodSlots = 3 // GET=0, HEAD=1, other=2
	metricStatusSlots = 8 // 200=0,206=1,304=2,301=3,302=4,404=5,500=6,other=7
	metricResultSlots = 5 // HIT=0,MISS=1,STALE=2,REVALIDATED=3,BYPASS=4
	metricSourceSlots = 5 // HOT=0,WARM=1,PEER=2,ORIGIN=3,NONE=4
)

// routeMetrics holds pre-resolved Prometheus collectors for a single route,
// indexed by [method][status][cacheResult][source]. This eliminates the
// per-request WithLabelValues hash lookup for common label tuples.
type routeMetrics struct {
	requestsTotal   [metricMethodSlots][metricStatusSlots][metricResultSlots][metricSourceSlots]prometheus.Counter
	requestDuration [metricMethodSlots][metricStatusSlots][metricResultSlots][metricSourceSlots]prometheus.Observer
	responseBytes   [metricMethodSlots][metricResultSlots][metricSourceSlots]prometheus.Counter
}

// methodIndex maps HTTP methods to array indices.
func methodIndex(method string) int {
	switch method {
	case "GET":
		return 0
	case "HEAD":
		return 1
	default:
		return 2
	}
}

// methodIndexBytes is methodIndex over a []byte. The switch form lets the
// compiler elide the string([]byte) conversion — zero allocation.
func methodIndexBytes(method []byte) int {
	switch string(method) {
	case "GET":
		return 0
	case "HEAD":
		return 1
	default:
		return 2
	}
}

// statusIndex maps common HTTP status codes to array indices. Returns -1
// for uncommon codes, signalling the middleware to fall back to WithLabelValues.
func statusIndex(code int) int {
	switch code {
	case 200:
		return 0
	case 206:
		return 1
	case 304:
		return 2
	case 301:
		return 3
	case 302:
		return 4
	case 404:
		return 5
	case 500:
		return 6
	default:
		return -1
	}
}

// cacheResultIndex maps cache result strings to array indices. Returns -1
// for unknown values.
func cacheResultIndex(s string) int {
	switch s {
	case "HIT":
		return 0
	case "MISS":
		return 1
	case "STALE":
		return 2
	case "REVALIDATED":
		return 3
	case "BYPASS":
		return 4
	default:
		return -1
	}
}

// cacheResultIndexBytes is cacheResultIndex over a []byte, zero-alloc
// (see methodIndexBytes).
func cacheResultIndexBytes(s []byte) int {
	switch string(s) {
	case "HIT":
		return 0
	case "MISS":
		return 1
	case "STALE":
		return 2
	case "REVALIDATED":
		return 3
	case "BYPASS":
		return 4
	default:
		return -1
	}
}

// sourceIndex maps source strings to array indices. Returns -1 for unknown.
func sourceIndex(s string) int {
	switch s {
	case string(api.SourceHot):
		return 0
	case string(api.SourceWarm):
		return 1
	case string(api.SourcePeer):
		return 2
	case string(api.SourceOrigin):
		return 3
	case "":
		return 4
	default:
		return -1
	}
}

// sourceIndexBytes is sourceIndex over a []byte, zero-alloc
// (see methodIndexBytes).
func sourceIndexBytes(s []byte) int {
	switch string(s) {
	case string(api.SourceHot):
		return 0
	case string(api.SourceWarm):
		return 1
	case string(api.SourcePeer):
		return 2
	case string(api.SourceOrigin):
		return 3
	case "":
		return 4
	default:
		return -1
	}
}

// PreResolveRoutes builds the pre-resolved metrics array for the given route
// names. Each route gets its own set of pre-resolved counters, observers, and
// byte counters for all common (method, status, cacheResult, source) tuples.
// Uncommon tuples fall back to WithLabelValues at runtime.
func (m *DataPlaneMetrics) PreResolveRoutes(routeNames []string) {
	m.routeIDs = make(map[string]int, len(routeNames)+1)
	m.routeTable = make([]*routeMetrics, 0, len(routeNames)+1)

	// Index 0 is the _default route (used when no route header is set).
	m.routeIDs["_default"] = 0
	m.routeTable = append(m.routeTable, m.buildRouteMetrics("_default"))

	for _, name := range routeNames {
		if name == "" || name == "_default" {
			continue
		}
		m.routeIDs[name] = len(m.routeTable)
		m.routeTable = append(m.routeTable, m.buildRouteMetrics(name))
	}
}

// buildRouteMetrics pre-resolves all counter/observer instances for a single
// route by calling WithLabelValues once per tuple at init time. Subsequent
// requests use direct array indexing instead of hash lookups.
func (m *DataPlaneMetrics) buildRouteMetrics(route string) *routeMetrics {
	rm := &routeMetrics{}
	methods := []string{"GET", "HEAD", ""}
	statuses := []string{"200", "206", "304", "301", "302", "404", "500", "0"}
	results := []string{"HIT", "MISS", "STALE", "REVALIDATED", "BYPASS"}
	sources := []string{
		string(api.SourceHot), string(api.SourceWarm),
		string(api.SourcePeer), string(api.SourceOrigin), "",
	}
	for mi := range methods {
		for si := range statuses {
			for ri := range results {
				for src := range sources {
					rm.requestsTotal[mi][si][ri][src] =
						m.RequestsTotal.WithLabelValues(methods[mi], statuses[si], results[ri], sources[src], route)
					rm.requestDuration[mi][si][ri][src] =
						m.RequestDuration.WithLabelValues(methods[mi], statuses[si], results[ri], sources[src], route)
				}
			}
		}
	}
	// ResponseBytesOut has 4 labels (no status).
	for mi := range methods {
		for ri := range results {
			for src := range sources {
				rm.responseBytes[mi][ri][src] =
					m.ResponseBytesOut.WithLabelValues(methods[mi], results[ri], sources[src], route)
			}
		}
	}
	return rm
}

// SetAccessLog configures the access logger and sampling rate for the the
// merged middleware. logger receives Warn for non-200 responses (always)
// and Info for 200 responses (sampled 1-in-sampleRate by cache key).
// sampleRate=0 disables sampling (every request is logged).
func (m *DataPlaneMetrics) SetAccessLog(logger Logger, sampleRate uint64) {
	m.accessLog = logger
	m.accessSampleRate = sampleRate
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
	m.CFBatchFlushed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "cloudflare_batch_flushed_total",
		Help:      "Number of Cloudflare batch flushes by kind (urls, tags, prefixes, hosts).",
	}, []string{"kind"})
	m.CFBatchDeduped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "cloudflare_batch_deduped_total",
		Help:      "Number of duplicate purge items deduplicated before reaching the Cloudflare API.",
	}, []string{"kind"})
	m.CFBatchFlushErr = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "cloudflare_batch_flush_error_total",
		Help:      "Cloudflare batch flush errors by kind and error type.",
	}, []string{"kind", "error_type"})
	m.CFTokenRotated = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "cloudflare_token_rotated_total",
		Help:      "Number of times a Cloudflare API token was marked as rate-limited and rotated.",
	})
	m.CFTokenAvailable = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "bouine",
		Name:      "cloudflare_token_available",
		Help:      "Number of Cloudflare API tokens currently available (not in cooldown).",
	})
	m.CFCircuitRejected = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "cloudflare_circuit_rejected_total",
		Help:      "Number of CF purge calls rejected because the circuit breaker was open.",
	})
	m.CFCircuitState = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "bouine",
		Name:      "cloudflare_circuit_state",
		Help:      "Circuit breaker state: 0=closed, 1=open, 2=half_open.",
	})
	m.CFDLQEnqueued = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "cloudflare_dlq_enqueued_total",
		Help:      "Number of failed purge items enqueued to the retry queue.",
	}, []string{"kind"})
	m.CFDLQDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "cloudflare_dlq_dropped_total",
		Help:      "Number of failed purge items dropped because the retry queue was full.",
	}, []string{"kind"})
	m.CFDLQRetried = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "cloudflare_dlq_retried_total",
		Help:      "Number of purge items retried from the retry queue.",
	}, []string{"kind"})
	m.CFDLQExpired = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "cloudflare_dlq_expired_total",
		Help:      "Number of purge items expired from the retry queue after max retries.",
	}, []string{"kind"})
	m.CFDLQDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "bouine",
		Name:      "cloudflare_dlq_depth",
		Help:      "Current retry queue depth (number of pending items).",
	})
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

// Middleware wraps an http.Handler and records RED metrics + structured
// access log for every request. Metrics are always recorded; the access
// log entry is sampled at the logger's configured rate (1-in-N for 200
// OK, always for non-200).
//
// This merged middleware replaces the former separate accesslog + metrics
// middleware pair, halving the ResponseWriter pool acquires and wrapper
// layers on the Write path from two to one.

// FastHTTPMiddleware wraps a fasthttp.RequestHandler and records RED
// metrics + structured access log for every request. This is the
// fasthttp-native middleware, wired into the data-plane handler chain.
func (m *DataPlaneMetrics) FastHTTPMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	nowFunc := m.nowFunc
	if nowFunc == nil {
		nowFunc = time.Now
	}
	return func(ctx *fasthttp.RequestCtx) {
		ctx.Request.Header.Del(header.XBouineRoute)

		m.RequestQueueDepth.Inc()
		defer m.RequestQueueDepth.Dec()

		start := nowFunc()
		next(ctx)

		statusCode := ctx.Response.StatusCode()
		if statusCode == 0 {
			statusCode = 200
		}
		status := statusString(statusCode)
		route := "_default"
		if rv := ctx.UserValue(header.XBouineRoute); rv != nil {
			if rs, ok := rv.(string); ok && rs != "" {
				route = rs
			}
		}
		// Classify X-Cache and X-Cache-Source from the raw header bytes.
		// The byte switches are zero-alloc (the compiler elides string([]byte)
		// in switch positions); the label strings for the metrics paths are
		// derived from the classification below instead of converting the
		// header values per request.
		xCacheBytes := ctx.Response.Header.Peek(header.XCache)
		cacheResultIdx := cacheResultIndexBytes(xCacheBytes)
		var cacheResult string
		switch cacheResultIdx {
		case 0:
			cacheResult = "HIT"
		case 1:
			cacheResult = "MISS"
		case 2:
			cacheResult = "STALE"
		case 3:
			cacheResult = "REVALIDATED"
		case 4:
			cacheResult = "BYPASS"
		default:
			// Empty or unknown: empty counts as MISS, anything else as
			// UNKNOWN (closed label set, see normaliseCacheResult).
			if len(xCacheBytes) == 0 {
				cacheResult = "MISS"
			} else {
				cacheResult = "UNKNOWN"
			}
		}
		srcIdx := sourceIndexBytes(ctx.Response.Header.Peek(header.XCacheSource))
		var source string
		switch srcIdx {
		case 0:
			source = string(api.SourceHot)
		case 1:
			source = string(api.SourceWarm)
		case 2:
			source = string(api.SourcePeer)
		case 3:
			source = string(api.SourceOrigin)
		default:
			source = ""
		}

		elapsed := time.Since(start)
		dur := elapsed.Seconds()
		bytesOut := float64(len(ctx.Response.Body()))

		m.recordFastHTTPMetrics(ctx.Method(), statusCode, status, route, cacheResult, source, dur, bytesOut)

		m.recordFastHTTPRings(cacheResult, cacheResultIdx, statusCode, route, ctx.Path(), elapsed, &ctx.Response.Header)

		if m.accessLog != nil {
			msg := accessLogMessage(cacheResult, statusCode)
			if statusCode != fasthttp.StatusOK {
				attrs := m.buildFastHTTPAccessLogAttrs(ctx, cacheResult, elapsed, statusCode)
				m.accessLog.Warn(msg, attrs...)
			} else {
				keyVal := ctx.UserValue("cacheKey")
				var key api.Key
				if k, ok := keyVal.(api.Key); ok {
					key = k
				}
				if m.shouldLogAccess(key) {
					attrs := m.buildFastHTTPAccessLogAttrs(ctx, cacheResult, elapsed, statusCode)
					m.accessLog.Info(msg, attrs...)
				}
			}
		}
	}
}

// recordFastHTTPMetrics increments the RED counters. The method is
// passed as []byte and classified with methodIndexBytes (zero-alloc on
// the pre-resolved path). The fallback passes the real method string —
// WithLabelValues allocates there anyway, and squashing uncommon
// methods to "OTHER" would destroy the method dimension exactly where
// it matters most.
func (m *DataPlaneMetrics) recordFastHTTPMetrics(method []byte, code int, status, route, cacheResult, source string, dur, bytesOut float64) {
	mi := methodIndexBytes(method)
	if rm, ok := m.lookupRouteMetrics(route); ok {
		si := statusIndex(code)
		ri := cacheResultIndex(cacheResult)
		src := sourceIndex(source)
		if si >= 0 && ri >= 0 && src >= 0 {
			rm.requestsTotal[mi][si][ri][src].Inc()
			rm.requestDuration[mi][si][ri][src].Observe(dur)
			rm.responseBytes[mi][ri][src].Add(bytesOut)
			return
		}
		m.fallbackCount.Add(1)
	}
	label := string(method)
	m.RequestsTotal.WithLabelValues(label, status, cacheResult, source, route).Inc()
	m.RequestDuration.WithLabelValues(label, status, cacheResult, source, route).Observe(dur)
	m.ResponseBytesOut.WithLabelValues(label, cacheResult, source, route).Add(bytesOut)
}

// buildFastHTTPAccessLogAttrs constructs the structured-log attribute
// slice for a fasthttp access log entry.
func (m *DataPlaneMetrics) buildFastHTTPAccessLogAttrs(ctx *fasthttp.RequestCtx, cacheResult string, elapsed time.Duration, status int) []any {
	attrs := []any{
		"method", string(ctx.Method()),
		"host", string(ctx.Host()),
		"path", string(ctx.Path()),
		"proto", "HTTP/1.1",
		"status", status,
		"bytes_out", len(ctx.Response.Body()),
		"dur_ms", elapsed.Milliseconds(),
		"remote", ctx.RemoteAddr().String(),
		"cache_status", cacheResult,
	}
	return attrs
}

// recordMetrics increments the RED counters using pre-resolved labels when
// available, falling back to WithLabelValues for uncommon tuples.

// observeDuration records the request duration on the given observer,
// attaching a trace exemplar when a valid span context is available.

// RecordHit implements api.FastPathMetrics. It increments the RED
// counters for a fast-path hit without going through the middleware
// chain. Called by the h1parser after serving a cache hit.
func (m *DataPlaneMetrics) RecordHit(method, route, cacheResult, source string, status, bytesOut int, duration time.Duration) {
	if route == "" {
		// The engine-level fast path carries no route name (the store is
		// shared across routes), so hits arrive with route="". Dashboards
		// and the middleware use "_default" for unlabelled traffic;
		// mapping here keeps fast-path hits on the pre-resolved array
		// path instead of the WithLabelValues fallback, and keeps the
		// label set consistent with miss-path metrics.
		route = "_default"
	}
	dur := duration.Seconds()
	if rm, ok := m.lookupRouteMetrics(route); ok {
		mi := methodIndex(method)
		si := statusIndex(status)
		ri := cacheResultIndex(cacheResult)
		src := sourceIndex(source)
		if si >= 0 && ri >= 0 && src >= 0 {
			rm.requestsTotal[mi][si][ri][src].Inc()
			rm.requestDuration[mi][si][ri][src].Observe(dur)
			rm.responseBytes[mi][ri][src].Add(float64(bytesOut))
			return
		}
		m.fallbackCount.Add(1)
	}
	m.RequestsTotal.WithLabelValues(method, strconv.Itoa(status), cacheResult, source, route).Inc()
	m.RequestDuration.WithLabelValues(method, strconv.Itoa(status), cacheResult, source, route).Observe(dur)
	m.ResponseBytesOut.WithLabelValues(method, cacheResult, source, route).Add(float64(bytesOut))
}

// IncrementSmugglingRejected increments the HTTP smuggling rejection
// counter. Called by the h1parser when it detects CL+TE conflict,
// duplicate Content-Length, or obs-fold.
func (m *DataPlaneMetrics) IncrementSmugglingRejected() {
	m.HTTPSmugglingRejected.Inc()
}

// recordRings updates the dashboard ring buffers for non-HIT requests.

// recordFastHTTPRings updates the dashboard ring buffers for non-HIT
// requests from the fasthttp middleware path. cacheResultIdx is the
// pre-classified X-Cache index (cacheResultIndexBytes) so the ring methods
// can early-return on HIT without the caller materializing header strings;
// path is passed as []byte and converted only when the URL ring actually
// records (sampling gate first).
func (m *DataPlaneMetrics) recordFastHTTPRings(cacheResult string, cacheResultIdx int, status int, route string, path []byte, elapsed time.Duration, hdr *fasthttp.ResponseHeader) {
	if m.Rings == nil || cacheResultIdx == 0 { // 0 == HIT
		return
	}
	durMs := elapsed.Milliseconds()
	m.Rings.Request.RecordRequest(cacheResult, status, durMs)
	if route != "_default" {
		m.Rings.Route.RecordRoute(route, cacheResult, status, durMs)
	}
	m.Rings.URL.RecordURL(string(path), route, cacheResult)
	if m.Rings.HeaderRing != nil && (cacheResultIdx == 1 || cacheResultIdx == 4) { // MISS or BYPASS
		m.Rings.HeaderRing.SampleFastHTTP(route, hdr, status)
	}
}

// lookupRouteMetrics returns the pre-resolved metrics for the given route
// name. Returns ok=false when pre-resolved metrics are not initialized or
// the route name is not in the route table.
func (m *DataPlaneMetrics) lookupRouteMetrics(route string) (*routeMetrics, bool) {
	if m.routeTable == nil || m.routeIDs == nil {
		return nil, false
	}
	id, ok := m.routeIDs[route]
	if !ok || id >= len(m.routeTable) {
		return nil, false
	}
	return m.routeTable[id], true
}

// accessLogMessage returns a human-readable log message based on the
// cache result and HTTP status code.
func accessLogMessage(cacheResult string, status int) string {
	if status != fasthttp.StatusOK {
		return "request completed with error"
	}
	switch cacheResult {
	case "HIT":
		return "served cache hit"
	case "MISS":
		return "served cache miss"
	case "BYPASS":
		return "bypassed cache"
	case "STALE":
		return "served stale response"
	case "REVALIDATED":
		return "served revalidated response"
	case "":
		return "served uncached response"
	default:
		return "served response (unknown cache status)"
	}
}

// buildAccessLogAttrs constructs the structured-log attribute slice for
// an access log entry. Called only when the sampling decision is positive
// or the status is non-200, so the 20-element []any allocation is avoided
// for the vast majority of hit requests.

// shouldLogAccess returns true when this request should emit an Info-level
// access log entry. Uses key-based deterministic sampling when a cache key
// is available (same key always logged or always skipped), and counter-based
// sampling as a fallback for requests without a cache key (bypass, no-route).
func (m *DataPlaneMetrics) shouldLogAccess(key api.Key) bool {
	if m.accessSampleRate == 0 {
		return true
	}
	if !key.IsZero() {
		return key.Hash64()%m.accessSampleRate == 0
	}
	return m.accessCounter.Add(1)%m.accessSampleRate == 0
}

// normaliseCacheResult maps X-Cache header values to a stable Prometheus
// label. Unknown values are mapped to UNKNOWN to keep the Prometheus label
// set closed and prevent cardinality bombs from attacker-controlled or
// misconfigured X-Cache response headers.
func normaliseCacheResult(xCache string) string {
	switch xCache {
	case "HIT", "MISS", "STALE", "REVALIDATED", "BYPASS":
		return xCache
	case "":
		return "MISS"
	default:
		return "UNKNOWN"
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
