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

// Source describes where a cached response was served from. It is the
// value carried by the X-Cache-Source response header and the `source`
// Prometheus label on data-plane request metrics. Clients MUST tolerate
// unknown values.
//
// Stable.
type Source string

const (
	// SourceHot — served from the hot tier (in-RAM, L0).
	SourceHot Source = "hot"
	// SourceWarm — served from the warm tier (mmap disk, L1), promoted
	// to hot on access.
	SourceWarm Source = "warm"
	// SourcePeer — served from a cluster peer via peer-fetch RPC.
	SourcePeer Source = "peer"
	// SourceOrigin — fetched from the upstream origin (including error
	// responses and write-through proxy).
	SourceOrigin Source = "origin"
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
