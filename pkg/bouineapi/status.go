package bouineapi

import (
	"context"
)

// StatusReport mirrors the admin GET /v1/status document (issue #813).
// Wire-compatible with admin.StatusReport; add fields, never remove.
//
// Unstable until the admin-surface epic settles.
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
	UptimeSec int64  `json:"uptime_sec"`
	Ready     bool   `json:"ready"`
}

// StatusCache holds the node-local cache counters since boot.
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

// StatusPool is one upstream pool's target health snapshot.
type StatusPool struct {
	Name    string         `json:"name"`
	Targets []StatusTarget `json:"targets"`
}

// StatusTarget reports one origin target's health state. Counters
// are consecutive streaks; AdminOverride is reserved for the future
// drain/restore write op and is "none" today.
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

// StatusCluster is the cluster section; per-peer request counters are
// only populated when Status was requested with cluster=true.
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
	Stale     bool   `json:"stale,omitempty"`
	Requests  int64  `json:"requests,omitempty"`
	Hits      int64  `json:"hits,omitempty"`
	Misses    int64  `json:"misses,omitempty"`
}

// StatusPeerFetch is the local node's peer-fetch telemetry.
type StatusPeerFetch struct {
	Hits         int64   `json:"hits"`
	Misses       int64   `json:"misses"`
	HopLimitHits int64   `json:"hop_limit_hits"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
}

// StatusInsight is one evaluated insight rule.
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

// Status fetches the node-status overview (bouine status, issue
// #813). cluster=true asks the node to aggregate peer stats with one
// peer-metrics round-trip; the default is node-local.
func (c *Client) Status(ctx context.Context, cluster bool) (*StatusReport, error) {
	var out StatusReport
	path := "/v1/status"
	if cluster {
		path += "?cluster=1"
	}
	if err := c.get(ctx, path, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
