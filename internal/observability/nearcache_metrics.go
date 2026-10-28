package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

// NearCacheMetrics holds the experimental near-cache counters
// (ADR-0071). The engine registers them on the metrics registry and
// injects them into cache.NearCacheCounters; the cache layer itself
// only sees the Inc interface.
//
// Unstable — the experimental feature owns its own surface.
type NearCacheMetrics struct {
	// Hits counts near-cache serves (fresh entries only).
	Hits prometheus.Counter
	// Admissions counts objects stored into the near cache after the
	// popularity gate and admission guards.
	Admissions prometheus.Counter
	// Invalidations counts refresh/ban-driven removals observed by
	// the near cache (purges are deleted silently alongside the main
	// store, exactly like every other invalidation surface).
	Invalidations prometheus.Counter
}

// NewNearCacheMetrics builds the near-cache counters. Namespaced
// bouine_near_cache_* with no labels: the counters are per-node by
// construction (one registry per node), and the near cache is one
// shared instance per engine — route/pool labels would be all-ones.
func NewNearCacheMetrics() *NearCacheMetrics {
	return &NearCacheMetrics{
		Hits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "bouine_near_cache_hits_total",
			Help: "Near-cache serves: hot remote-owned keys answered from the local near copy instead of a peer RPC (ADR-0071).",
		}),
		Admissions: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "bouine_near_cache_admissions_total",
			Help: "Objects admitted into the near cache after the popularity gate and freshness/variant guards (ADR-0071).",
		}),
		Invalidations: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "bouine_near_cache_invalidations_total",
			Help: "Refresh/ban-driven removals observed by the near cache (ADR-0071). A sustained non-zero rate means invalidation traffic is fighting near-cache reuse.",
		}),
	}
}
