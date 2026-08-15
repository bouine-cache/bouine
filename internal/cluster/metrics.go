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
	// GossipQueueDropped counts messages dropped from the local gossip
	// broadcast queue because it was full (drop-newest). Non-zero
	// indicates the GossipQueueDepth may need tuning or invalidation
	// bursts need throttling. See issue #297.
	GossipQueueDropped prometheus.Counter
	// GossipQueueDepth is the current number of pending messages in the
	// local gossip broadcast queue. Updated on every GetBroadcasts drain
	// (memberlist gossip interval, ~200ms-1s), not on every enqueue, to
	// avoid lock-held gauge updates on the hot path. See issue #297.
	GossipQueueDepth prometheus.Gauge

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
		GossipQueueDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "cluster_gossip_queue_dropped_total",
			Help:      "Messages dropped from the local gossip broadcast queue because it was full (drop-newest). See issue #297.",
		}),
		GossipQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bouine",
			Name:      "cluster_gossip_queue_depth",
			Help:      "Current number of pending messages in the local gossip broadcast queue.",
		}),
	}
	reg.MustRegister(
		m.ModeInfo,
		m.InvalidationsGossip,
		m.InvalidationsHTTP,
		m.BroadcastFailures,
		m.GossipDrops,
		m.GossipQueueDropped,
		m.GossipQueueDepth,
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

// IncGossipQueueDropped increments the gossip-queue-dropped counter.
// Called when QueueBroadcast drops a message because the local gossip
// queue is full (drop-newest). See issue #297.
func (m *Metrics) IncGossipQueueDropped() {
	if m == nil || m.GossipQueueDropped == nil {
		return
	}
	m.GossipQueueDropped.Inc()
}

// SetGossipQueueDepth sets the gossip-queue-depth gauge to the current
// number of pending messages. Called from GetBroadcasts after draining.
// See issue #297.
func (m *Metrics) SetGossipQueueDepth(n int) {
	if m == nil || m.GossipQueueDepth == nil {
		return
	}
	m.GossipQueueDepth.Set(float64(n))
}

// BroadcastFailuresCount returns the total number of broadcast failures
// across all types and reasons. Used by the dashboard insights engine.
func (m *Metrics) BroadcastFailuresCount() int64 {
	if m == nil {
		return 0
	}
	return m.broadcastFailuresTotal.Load()
}
