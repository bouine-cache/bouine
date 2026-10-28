package cluster

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/bouine-cache/bouine/internal/config"
)

// Metrics holds Prometheus counters for cluster-level events.
// Nil pointers are safe to use — Inc/Add/Gauge.Set are no-ops when
// the underlying collector is nil. This lets single-node mode skip
// registration entirely.
//
// Stable.
type Metrics struct {
	// ModeInfo is a constant gauge set to 1 for the active cluster
	// mode label. Registers once at startup.
	ModeInfo *prometheus.GaugeVec
	// InvalidationsGossip tracks invalidation events received via
	// gossip (both purge and ban, all modes).
	InvalidationsGossip *prometheus.CounterVec
	// InvalidationsHTTP tracks invalidation events sent via HTTP
	// fan-out (strong mode only).
	InvalidationsHTTP *prometheus.CounterVec
	// BroadcastFailures counts HTTP fan-out failures by type (purge, ban),
	// labelled by reason (dial, timeout, 5xx). Non-zero indicates peers
	// may have missed an invalidation; gossip provides redundant delivery.
	BroadcastFailures *prometheus.CounterVec
	// GossipDrops counts memberlist "handler queue full" warnings —
	// messages dropped because the receiving node's handoff queue
	// overflowed. Non-zero indicates the HandoffQueueDepth may need
	// tuning or invalidation bursts need throttling. See issue #201.
	GossipDrops prometheus.Counter
	// RingEmpty counts the number of times Owner was called while the
	// consistent-hash ring had zero vnodes. Non-zero indicates a
	// correctness regression: the node is failing open to single-node
	// ownership. See issue #305.
	RingEmpty prometheus.Counter
	// BroadcastOverflows counts batcher queue overflows. Non-zero
	// means invalidation events bypass batching and fall back to
	// unbatched delivery (delivery preserved, batching win lost).
	// See ADR-0044.
	BroadcastOverflows prometheus.Counter
	// GossipOversizedDrops counts gossip frames dropped because they
	// cannot fit memberlist's UDP gossip window even in an empty
	// round. Non-zero indicates a bug or a pathological single event:
	// batches are split at enqueue time, so this guard should stay at
	// zero. See issue #754.
	GossipOversizedDrops prometheus.Counter
	// PeerFetchVariantMismatch counts peer-fetch RPCs rejected by the
	// RFC 9111 §4.1 variant-assertion gate, labelled by side:
	// "server" (the owner answered a requested variant with another
	// variant's body or the primary-key Vary resolver) or "consumer"
	// (the fetched object's stored variant does not select the local
	// request). A sustained non-zero rate indicates a mixed-version
	// fleet or a peer serving wrong-variant content. See issue #633.
	PeerFetchVariantMismatch *prometheus.CounterVec
	// PeerFetchResolverAnswers counts the ADR-0068 resolver-metadata
	// answer class, labelled by side: "server" (the owner answered a
	// blank-assertion ask with the metadata-only Vary resolver so the
	// requester can derive the Vary list it could not compute locally)
	// or "consumer" (the requester completed the two-step variant
	// resolution and served the variant the resolver routed it to).
	// Zero means Vary cold misses on non-owners go to origin instead of
	// converging through the owner — e.g. a mixed-version fleet.
	PeerFetchResolverAnswers *prometheus.CounterVec
	// PeerFetchDerivedFallback counts ADR-0071 config-derived Vary
	// derivations that proved wrong: the bare ask's metadata carried a
	// Vary list beyond the policy-derived one, so the route fell back
	// to the ADR-0068 two-step at one extra peer RPC per Vary cold
	// miss. Sustained non-zero means a route's origin declares Vary
	// fields outside its cache.key include list; the warn log carries
	// the route and both lists.
	PeerFetchDerivedFallback prometheus.Counter

	// broadcastFailuresTotal is a lock-free total of all broadcast
	// failures, used by the dashboard insights engine without needing
	// to read Prometheus dto.Metric from the cluster package.
	broadcastFailuresTotal atomic.Int64
	// peerFetchVariantMismatchTotal is a lock-free total across both
	// sides, exposed via PeerFetchVariantMismatchCount for the
	// insights engine.
	peerFetchVariantMismatchTotal atomic.Int64
}

// RegisterMetrics creates and registers the cluster metrics on
// the given registry. Pass nil to disable metrics (single-node mode).
func RegisterMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		return &Metrics{}
	}
	m := &Metrics{
		ModeInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "bouine",
			Name:      "cluster_mode_info",
			Help:      "Cluster consistency mode. Always 1; the label identifies the mode (strong, eventual).",
		}, []string{"mode"}),
		InvalidationsGossip: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "cluster_invalidations_gossip_total",
			Help:      "Invalidation events received via gossip, by type (purge, ban).",
		}, []string{"type"}),
		InvalidationsHTTP: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "cluster_invalidations_http_total",
			Help:      "Invalidation events sent via HTTP fan-out, by type (purge, ban). Strong mode only.",
		}, []string{"type"}),
		BroadcastFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "cluster_broadcast_failures_total",
			Help:      "HTTP fan-out failures by invalidation type and reason. Gossip provides redundant delivery.",
		}, []string{"type", "reason"}),
		GossipDrops: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "cluster_gossip_drops_total",
			Help:      "Memberlist handler queue full warnings — messages dropped because the receiving node's handoff queue overflowed.",
		}),
		RingEmpty: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "cluster_ring_empty_total",
			Help:      "Number of times Owner was called with an empty consistent-hash ring. Non-zero indicates a silent correctness regression.",
		}),
		BroadcastOverflows: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "cluster_broadcast_overflows_total",
			Help:      "Invalidation batcher queue overflows. Events fall back to unbatched delivery; delivery is preserved.",
		}),
		GossipOversizedDrops: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "cluster_gossip_oversized_drops_total",
			Help:      "Gossip frames dropped because they cannot fit the UDP gossip window. Non-zero indicates a bug or a pathological event; see issue #754.",
		}),
		PeerFetchVariantMismatch: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "peer_fetch_variant_mismatch_total",
			Help:      "Peer-fetch RPCs rejected by the RFC 9111 variant-assertion gate, by side (server, consumer). Sustained non-zero rate indicates a mixed-version fleet or a peer serving wrong-variant content.",
		}, []string{"side"}),
		PeerFetchResolverAnswers: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "peer_fetch_resolver_answers_total",
			Help:      "Metadata-only Vary resolver answers served over peer fetch, by side (server, consumer). Zero consumer-side means Vary cold misses on non-owners bypass owner convergence (ADR-0068).",
		}, []string{"side"}),
		PeerFetchDerivedFallback: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "peer_fetch_derived_fallback_total",
			Help:      "Config-derived Vary derivations (ADR-0071) that proved wrong: the bare ask's metadata carried a Vary list beyond the policy-derived one, so the route fell back to the ADR-0068 two-step at one extra peer RPC per Vary cold miss. Sustained non-zero means a route's origin declares Vary fields outside its cache.key include list; the warn log carries the route and both lists.",
		}),
	}
	reg.MustRegister(
		m.ModeInfo,
		m.InvalidationsGossip,
		m.InvalidationsHTTP,
		m.BroadcastFailures,
		m.GossipDrops,
		m.RingEmpty,
		m.BroadcastOverflows,
		m.GossipOversizedDrops,
		m.PeerFetchVariantMismatch,
		m.PeerFetchResolverAnswers,
		m.PeerFetchDerivedFallback,
	)
	return m
}

// SetMode sets the cluster_mode_info gauge to 1 for the given mode
// and resets any other mode to 0.
func (m *Metrics) SetMode(mode string) {
	if m == nil || m.ModeInfo == nil {
		return
	}
	for _, label := range []string{string(config.ClusterModeStrong), string(config.ClusterModeEventual)} {
		if label == mode {
			m.ModeInfo.WithLabelValues(label).Set(1)
		} else {
			m.ModeInfo.WithLabelValues(label).Set(0)
		}
	}
}

// IncGossipInvalidation increments the gossip invalidation counter for
// the given type ("purge" or "ban").
func (m *Metrics) IncGossipInvalidation(typ string) {
	if m == nil || m.InvalidationsGossip == nil {
		return
	}
	m.InvalidationsGossip.WithLabelValues(typ).Inc()
}

// IncHTTPInvalidation increments the HTTP fan-out invalidation counter
// for the given type ("purge" or "ban").
func (m *Metrics) IncHTTPInvalidation(typ string) {
	if m == nil || m.InvalidationsHTTP == nil {
		return
	}
	m.InvalidationsHTTP.WithLabelValues(typ).Inc()
}

// IncBroadcastFailure increments the broadcast-failure counter for
// the given invalidation type ("purge" or "ban") and reason ("dial",
// "timeout", "5xx", "marshal").
func (m *Metrics) IncBroadcastFailure(typ, reason string) {
	if m == nil || m.BroadcastFailures == nil {
		return
	}
	m.BroadcastFailures.WithLabelValues(typ, reason).Inc()
	m.broadcastFailuresTotal.Add(1)
}

// IncGossipDrop increments the gossip-drops counter. Called when
// memberlist logs a "handler queue full" warning.
func (m *Metrics) IncGossipDrop() {
	if m == nil || m.GossipDrops == nil {
		return
	}
	m.GossipDrops.Inc()
}

// IncBroadcastOverflow increments the batcher-overflow counter.
func (m *Metrics) IncBroadcastOverflow() {
	if m == nil || m.BroadcastOverflows == nil {
		return
	}
	m.BroadcastOverflows.Inc()
}

// IncGossipOversizedDrop increments the oversized-frame drop counter.
func (m *Metrics) IncGossipOversizedDrop() {
	if m == nil || m.GossipOversizedDrops == nil {
		return
	}
	m.GossipOversizedDrops.Inc()
}

// IncPeerFetchVariantMismatch increments the variant-mismatch counter
// for the given side ("server" or "consumer"). Nil-safe: single-node
// mode never registers the vec.
func (m *Metrics) IncPeerFetchVariantMismatch(side string) {
	if m == nil || m.PeerFetchVariantMismatch == nil {
		return
	}
	m.PeerFetchVariantMismatch.WithLabelValues(side).Inc()
	m.peerFetchVariantMismatchTotal.Add(1)
}

// PeerFetchVariantMismatchCount returns the total number of variant
// mismatches across both sides. Used by the dashboard insights engine.
func (m *Metrics) PeerFetchVariantMismatchCount() int64 {
	if m == nil {
		return 0
	}
	return m.peerFetchVariantMismatchTotal.Load()
}

// IncPeerFetchResolverAnswer increments the resolver-answer counter for
// the given side ("server" or "consumer"). Nil-safe: single-node mode
// never registers the vec.
func (m *Metrics) IncPeerFetchResolverAnswer(side string) {
	if m == nil || m.PeerFetchResolverAnswers == nil {
		return
	}
	m.PeerFetchResolverAnswers.WithLabelValues(side).Inc()
}

// IncPeerFetchDerivedFallback increments the derived-fallback counter:
// a config-derived Vary derivation (ADR-0071) proved wrong and the
// route fell back to the ADR-0068 two-step. Nil-safe: single-node mode
// never registers the counter.
func (m *Metrics) IncPeerFetchDerivedFallback() {
	if m == nil || m.PeerFetchDerivedFallback == nil {
		return
	}
	m.PeerFetchDerivedFallback.Inc()
}

// IncRingEmpty increments the ring-empty counter. Called when Owner
// is called with a zero-member ring.
func (m *Metrics) IncRingEmpty() {
	if m == nil || m.RingEmpty == nil {
		return
	}
	m.RingEmpty.Inc()
}

// BroadcastFailuresCount returns the total number of broadcast failures
// across all types and reasons. Used by the dashboard insights engine.
func (m *Metrics) BroadcastFailuresCount() int64 {
	if m == nil {
		return 0
	}
	return m.broadcastFailuresTotal.Load()
}
