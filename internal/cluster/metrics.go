package cluster

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
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

	// broadcastFailuresTotal is a lock-free total of all broadcast
	// failures, used by the dashboard insights engine without needing
	// to read Prometheus dto.Metric from the cluster package.
	broadcastFailuresTotal atomic.Int64
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
	}
	reg.MustRegister(
		m.ModeInfo,
		m.InvalidationsGossip,
		m.InvalidationsHTTP,
		m.BroadcastFailures,
		m.GossipDrops,
		m.RingEmpty,
	)
	return m
}

// SetMode sets the cluster_mode_info gauge to 1 for the given mode
// and resets any other mode to 0.
func (m *Metrics) SetMode(mode string) {
	if m == nil || m.ModeInfo == nil {
		return
	}
	for _, label := range []string{"strong", "eventual"} {
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
