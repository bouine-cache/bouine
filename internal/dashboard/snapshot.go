package dashboard

import (
	"context"

	"github.com/bouine-cache/bouine/internal/dashboard/insights"
	"github.com/bouine-cache/bouine/internal/observability"
)

// ClusterMetricsSnapshot performs one peer-metrics fan-out (the same
// aggregator path as the dashboard pages) and returns the per-peer
// results, including unreachable peers marked stale. It is the data
// source for `bouine status --cluster` (issue #813): one round-trip
// across every peer's /v1/peer/metrics.
func (h *Handler) ClusterMetricsSnapshot(ctx context.Context) []PeerResult {
	_, peers := h.agg.Collect(ctx)
	return peers
}

// LocalInsightSnapshot evaluates the insight rules against
// node-local data only (rings plus last-known peer health), with no
// peer fan-out, so a node's status never depends on peer
// reachability. Used by GET /v1/status default mode.
func (h *Handler) LocalInsightSnapshot(ctx context.Context) []insights.Insight {
	ins, _ := h.evaluateInsights(ctx, h.cfg.Rings.Summary(), h.localPeerResults())
	return ins
}

// ClusterInsightSnapshot evaluates the rules after one peer fan-out
// (the dashboard insights-page data path).
func (h *Handler) ClusterInsightSnapshot(ctx context.Context) []insights.Insight {
	merged, peers := h.agg.Collect(ctx)
	ins, _ := h.evaluateInsights(ctx, merged, peers)
	return ins
}

// evaluateInsights collects insight inputs and runs the engine,
// tracking the previous store snapshot between calls so
// delta-based rules (e.g. eviction rate) see the same state
// progression the dashboard page does.
func (h *Handler) evaluateInsights(ctx context.Context, merged observability.MetricsSummary, peers []PeerResult) ([]insights.Insight, insights.InsightData) {
	data := h.collectInsightData(merged, peers)
	h.prevStoreStatsMu.Lock()
	data.PrevStoreStats = h.prevStoreStats
	h.prevStoreStats = data.StoreStats
	h.prevStoreStatsMu.Unlock()
	return h.insightEngine.Evaluate(ctx, data), data
}

// localPeerResults derives PeerResults from the gossip peer list and
// the local peer-health ring without touching the network. Peers
// below full health in the ring are marked stale.
func (h *Handler) localPeerResults() []PeerResult {
	var peers []PeerResult
	if h.cfg.PeersFn == nil {
		return peers
	}
	health := map[string]float64{}
	if h.cfg.Rings != nil && h.cfg.Rings.Peer != nil {
		health = h.cfg.Rings.Peer.PeerHealth()
	}
	infos := h.cfg.PeersFn()
	peers = make([]PeerResult, 0, len(infos))
	for _, p := range infos {
		stale := false
		if hp, ok := health[p.Name]; ok && hp < 100 {
			stale = true
		}
		peers = append(peers, PeerResult{NodeName: p.Name, Stale: stale})
	}
	return peers
}
