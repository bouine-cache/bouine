package api

import "time"

// CacheResult describes how a request was served from a cache
// perspective. Clients MUST tolerate unknown values.
//
// Stable.
type CacheResult string

const (
	// CacheResultHit — served entirely from the cache.
	CacheResultHit CacheResult = "hit"
	// CacheResultMiss — not in the cache; fetched from origin or peer.
	CacheResultMiss CacheResult = "miss"
	// CacheResultStaleHit — served stale (SWR / SIE window).
	CacheResultStaleHit CacheResult = "stale_hit"
	// CacheResultRevalidated — origin confirmed freshness (304).
	CacheResultRevalidated CacheResult = "revalidated"
	// CacheResultBypass — route or directive forbade caching.
	CacheResultBypass CacheResult = "bypass"
)

// RequestContext is the canonical metadata captured for a single
// processed request. Phase 1+ populate fields as features land.
//
// Unstable until the data plane ships.
type RequestContext struct {
	RequestID    string        `json:"request_id"`
	StartTime    time.Time     `json:"start_time"`
	Method       string        `json:"method"`
	Host         string        `json:"host"`
	URL          string        `json:"url"`
	Route        string        `json:"route,omitempty"`
	UpstreamPool string        `json:"upstream_pool,omitempty"`
	CacheResult  CacheResult   `json:"cache_result,omitempty"`
	Status       int           `json:"status,omitempty"`
	BytesIn      int64         `json:"bytes_in,omitempty"`
	BytesOut     int64         `json:"bytes_out,omitempty"`
	Duration     time.Duration `json:"duration_ns,omitempty"`
	PeerHops     int           `json:"peer_hops,omitempty"`
}

// VersionInfo is the JSON shape returned by GET /version on the admin
// API.
//
// Stable.
type VersionInfo struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Date    string `json:"date"`
}

// HealthStatus is the JSON shape returned by /healthz and /readyz.
//
// Stable.
type HealthStatus struct {
	Status string `json:"status"`
}
