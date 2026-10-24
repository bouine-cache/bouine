package admin

import "time"

// StatusReport is the JSON document served by GET /v1/status and
// rendered by `bouine status` (issue #813): the node's one-glance
// operational overview: identity, cache counters, upstream pool
// health, cluster view, and evaluated insight rules.
//
// Unstable: the admin-surface epic (#812..#817) may still adjust
// sections additively.
type StatusReport struct {
	Origins  []StatusPool    `json:"origins,omitempty"`
	Insights []StatusInsight `json:"insights,omitempty"`
	Node     StatusNode      `json:"node"`
	Cluster  StatusCluster   `json:"cluster"`
	Cache    StatusCache     `json:"cache"`
}

// StatusNode identifies the serving node and its process state.
type StatusNode struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	Commit    string `json:"commit,omitempty"`
	StartedAt string `json:"started_at,omitempty"`
	// UptimeSec is seconds since process start.
	UptimeSec int64 `json:"uptime_sec"`
	// Ready mirrors the /readyz gate. A drained node reports false
	// here (drain marks the node not-ready), so `bouine status` is
	// the fastest way to see a mid-drain pod.
	Ready bool `json:"ready"`
}

// StatusCache holds the node-local cache counters since boot. Tier
// MaxBytes of 0 means unlimited (no configured budget).
type StatusCache struct {
	Hits      int64      `json:"hits"`
	Misses    int64      `json:"misses"`
	HitRatio  float64    `json:"hit_ratio"`
	Evictions int64      `json:"evictions"`
	Hot       StatusTier `json:"hot"`
	Warm      StatusTier `json:"warm"`
}

// StatusTier is one storage tier's fill level.
type StatusTier struct {
	Entries   int64 `json:"entries"`
	Bytes     int64 `json:"bytes"`
	MaxBytes  int64 `json:"max_bytes,omitempty"`
	DiskBytes int64 `json:"disk_bytes,omitempty"`
}

// StatusPool is one upstream pool's target health snapshot. The
// manual health override (bouine origins drain/restore, the
// backend.set_health equivalent) is a separate write op; AdminOverride
// is reserved for it and always "none" today.
type StatusPool struct {
	Name    string         `json:"name"`
	Targets []StatusTarget `json:"targets"`
}

// StatusTarget reports one origin target's health-transition state.
// Counters are consecutive streaks, not totals: the passive and probe
// paths reset on restore. LastChange is empty when no transition was
// recorded since boot.
type StatusTarget struct {
	Addr              string `json:"addr"`
	AdminOverride     string `json:"admin_override"`
	LastChange        string `json:"last_change,omitempty"`
	PassiveErrors     int64  `json:"passive_errors"`
	ProbeErrors       int64  `json:"probe_errors"`
	ProbeSuccesses    int64  `json:"probe_successes"`
	ConsecutiveErrors int64  `json:"consecutive_errors"`
	Healthy           bool   `json:"healthy"`
}

// StatusCluster is the cluster section. In node-local mode it lists
// peers (identity only, no request counters; those need a peer
// fan-out). With ?cluster=1 the request counters are filled from the
// dashboard aggregator's one-round-trip peer-metrics collection.
type StatusCluster struct {
	Peers     []StatusPeer    `json:"peers,omitempty"`
	PeerFetch StatusPeerFetch `json:"peer_fetch"`
}

// StatusPeer is one gossip peer.
type StatusPeer struct {
	Name      string `json:"name"`
	Addr      string `json:"addr,omitempty"`
	AdminAddr string `json:"admin_addr,omitempty"`
	DataAddr  string `json:"data_addr,omitempty"`
	Version   string `json:"version,omitempty"`
	JoinedAt  string `json:"joined_at,omitempty"`
	// Stale marks a peer that was unreachable during the last
	// aggregation and whose numbers come from its last-known summary.
	Stale bool `json:"stale,omitempty"`
	// Requests/Hits/Misses are ring-window counters; only populated
	// in cluster mode (they require the peer fan-out).
	Requests int64 `json:"requests,omitempty"`
	Hits     int64 `json:"hits,omitempty"`
	Misses   int64 `json:"misses,omitempty"`
}

// StatusPeerFetch is the local node's peer-fetch telemetry.
type StatusPeerFetch struct {
	Hits         int64   `json:"hits"`
	Misses       int64   `json:"misses"`
	HopLimitHits int64   `json:"hop_limit_hits"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
}

// StatusInsight is one evaluated insight rule, mapped from the
// dashboard insights engine (same rules the dashboard renders; no
// separate engine).
type StatusInsight struct {
	ID       string   `json:"id"`
	Severity string   `json:"severity"`
	Category string   `json:"category,omitempty"`
	Title    string   `json:"title"`
	Detail   string   `json:"detail,omitempty"`
	Evidence string   `json:"evidence,omitempty"`
	Action   string   `json:"action,omitempty"`
	Routes   []string `json:"routes,omitempty"`
}

// FormatStatusTime renders a Unix-nanosecond timestamp as RFC3339 for
// the wire, or "" when zero. Keeping the conversion here means engine
// closures stay free of formatting concerns.
func FormatStatusTime(unixNano int64) string {
	if unixNano == 0 {
		return ""
	}
	return time.Unix(0, unixNano).UTC().Format(time.RFC3339)
}
