package cmd

import (
	"context"
	"sort"
	"time"

	"github.com/bouine-cache/bouine/internal/admin"
	"github.com/bouine-cache/bouine/internal/buildinfo"
	"github.com/bouine-cache/bouine/internal/dashboard"
	"github.com/bouine-cache/bouine/internal/dashboard/insights"
	"github.com/bouine-cache/bouine/internal/origin"
)

// statusFn builds the GET /v1/status document (issue #813) for the
// admin server. Default mode is node-local: no peer fan-out, so a
// node's status never depends on peer reachability. cluster=true
// performs one peer-metrics round-trip through the dashboard
// aggregator and fills the per-peer request counters and cluster-wide
// insight evaluation.
func (e *engine) statusFn(rs *runState, dash *dashboard.Handler) func(ctx context.Context, cluster bool) admin.StatusReport {
	return func(ctx context.Context, cluster bool) admin.StatusReport {
		return admin.StatusReport{
			Node:     e.statusNode(rs),
			Cache:    e.statusCache(rs),
			Origins:  statusOrigins(rs.pools),
			Cluster:  e.statusCluster(rs, dash, ctx, cluster),
			Insights: statusInsights(dash, ctx, cluster),
		}
	}
}

func (e *engine) statusNode(rs *runState) admin.StatusNode {
	return admin.StatusNode{
		Name:      rs.rings.NodeName,
		Version:   buildinfo.Version,
		Commit:    buildinfo.Commit,
		StartedAt: admin.FormatStatusTime(e.startTime.UnixNano()),
		UptimeSec: int64(time.Since(e.startTime).Seconds()),
		Ready:     rs.seq.IsReady(),
	}
}

func (e *engine) statusCache(rs *runState) admin.StatusCache {
	st := rs.store.Stats()
	var ratio float64
	if total := st.Hits + st.Misses; total > 0 {
		ratio = float64(st.Hits) / float64(total)
	}
	return admin.StatusCache{
		Hits:      st.Hits,
		Misses:    st.Misses,
		HitRatio:  ratio,
		Evictions: st.Evictions,
		Hot: admin.StatusTier{
			Entries:  st.HotEntries,
			Bytes:    st.HotBytes,
			MaxBytes: e.cfg.Storage.HotMaxBytes.Bytes(),
		},
		Warm: admin.StatusTier{
			Entries:   st.WarmEntries,
			Bytes:     st.WarmBytes,
			MaxBytes:  e.cfg.Storage.WarmMaxBytes.Bytes(),
			DiskBytes: st.WarmDiskBytes,
		},
	}
}

// statusOrigins snapshots every upstream pool. AdminOverride is the
// reserved field for the future drain/restore write op and reports
// "none" until that lands.
func statusOrigins(pools map[string]*origin.Pool) []admin.StatusPool {
	if len(pools) == 0 {
		return nil
	}
	out := make([]admin.StatusPool, 0, len(pools))
	for name, p := range pools {
		targets := p.Targets()
		pool := admin.StatusPool{
			Name:    name,
			Targets: make([]admin.StatusTarget, len(targets)),
		}
		for i, t := range targets {
			pool.Targets[i] = admin.StatusTarget{
				Addr:              t.Addr,
				Healthy:           t.Healthy,
				AdminOverride:     "none",
				PassiveErrors:     t.PassiveErrors,
				ProbeErrors:       t.ProbeErrors,
				ProbeSuccesses:    t.ProbeSuccesses,
				ConsecutiveErrors: t.ConsecutiveErrors,
				LastChange:        admin.FormatStatusTime(t.LastChangeUnixNano),
			}
		}
		out = append(out, pool)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// statusCluster builds the cluster section. Local mode lists peer
// identity only; cluster mode joins that identity with the one
// round-trip peer-metrics counters (requests/hits/misses over the
// ring window, from each peer's /v1/peer/metrics).
func (e *engine) statusCluster(rs *runState, dash *dashboard.Handler, ctx context.Context, cluster bool) admin.StatusCluster {
	out := admin.StatusCluster{}
	identity := map[string]admin.StatusPeer{}
	if rs.peersFn != nil {
		for _, p := range rs.peersFn() {
			identity[p.Name] = admin.StatusPeer{
				Name:      p.Name,
				Addr:      p.Addr,
				AdminAddr: p.AdminAddr,
				DataAddr:  p.DataAddr,
				Version:   p.Version,
				JoinedAt:  admin.FormatStatusTime(p.JoinedAt.UnixNano()),
			}
		}
	}
	if rs.peerFetcher != nil {
		hits, misses, hopLimitHits, latN, latSumMs := rs.peerFetcher.PeerFetchStats()
		var avg float64
		if latN > 0 {
			avg = float64(latSumMs) / float64(latN)
		}
		out.PeerFetch = admin.StatusPeerFetch{
			Hits:         hits,
			Misses:       misses,
			HopLimitHits: hopLimitHits,
			AvgLatencyMs: avg,
		}
	}
	if !cluster {
		for _, p := range identity {
			out.Peers = append(out.Peers, p)
		}
		sortPeers(out.Peers)
		return out
	}
	for _, pr := range dash.ClusterMetricsSnapshot(ctx) {
		peer := identity[pr.NodeName]
		peer.Name = pr.NodeName
		peer.Stale = pr.Stale
		for _, b := range pr.Summary.RequestSnap {
			peer.Requests += b.Requests
			peer.Hits += b.Hits
			peer.Misses += b.Misses
		}
		out.Peers = append(out.Peers, peer)
		delete(identity, pr.NodeName)
	}
	// Peers that joined but were not in the fan-out result still
	// deserve a row (identity only).
	for _, p := range identity {
		out.Peers = append(out.Peers, p)
	}
	sortPeers(out.Peers)
	return out
}

func sortPeers(peers []admin.StatusPeer) {
	sort.Slice(peers, func(i, j int) bool { return peers[i].Name < peers[j].Name })
}

// statusInsights maps the dashboard insight engine's output (same
// rules the dashboard renders; no separate engine) to the wire shape.
func statusInsights(dash *dashboard.Handler, ctx context.Context, cluster bool) []admin.StatusInsight {
	var raw []insights.Insight
	if cluster {
		raw = dash.ClusterInsightSnapshot(ctx)
	} else {
		raw = dash.LocalInsightSnapshot(ctx)
	}
	out := make([]admin.StatusInsight, len(raw))
	for i, r := range raw {
		out[i] = admin.StatusInsight{
			ID:       r.ID,
			Severity: string(r.Severity),
			Category: string(r.Category),
			Title:    r.Title,
			Detail:   r.Detail,
			Evidence: r.Evidence,
			Action:   r.Action,
			Routes:   r.Routes,
		}
	}
	return out
}
