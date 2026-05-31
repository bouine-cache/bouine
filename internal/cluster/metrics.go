package cluster

import (
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
	// ReplicationsSent tracks full-mode replication events sent via
	// gossip (full mode only).
	ReplicationsSent prometheus.Counter
	// ReplicationsReceived tracks full-mode replication events received
	// via gossip and stored locally (full mode only).
	ReplicationsReceived prometheus.Counter
	// ReplicationBytes tracks the approximate byte size of replicated
	// objects sent or received via gossip.
	ReplicationBytes *prometheus.CounterVec
	// BroadcastFailures counts HTTP fan-out failures by type (purge, ban),
	// labelled by reason (dial, timeout, 5xx). Non-zero indicates peers
	// may have missed an invalidation; gossip provides redundant delivery.
	BroadcastFailures *prometheus.CounterVec
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
			Help:      "Cluster consistency mode. Always 1; the label identifies the mode (strong, eventual, full).",
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
		ReplicationsSent: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "cluster_replications_sent_total",
			Help:      "Cached objects broadcast to peers via gossip in full mode.",
		}),
		ReplicationsReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "cluster_replications_received_total",
			Help:      "Cached objects received from peers via gossip and stored locally in full mode.",
		}),
		ReplicationBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "cluster_replication_bytes_total",
			Help:      "Approximate byte size of replicated objects sent or received via gossip.",
		}, []string{"direction"}),
		BroadcastFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "cluster_broadcast_failures_total",
			Help:      "HTTP fan-out failures by invalidation type and reason. Gossip provides redundant delivery.",
		}, []string{"type", "reason"}),
	}
	reg.MustRegister(
		m.ModeInfo,
		m.InvalidationsGossip,
		m.InvalidationsHTTP,
		m.ReplicationsSent,
		m.ReplicationsReceived,
		m.ReplicationBytes,
		m.BroadcastFailures,
	)
	return m
}

// SetMode sets the cluster_mode_info gauge to 1 for the given mode
// and resets any other mode to 0.
func (m *Metrics) SetMode(mode string) {
	if m == nil || m.ModeInfo == nil {
		return
	}
	for _, label := range []string{"strong", "eventual", "full"} {
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

// IncReplicationSent increments the replication-sent counter.
func (m *Metrics) IncReplicationSent() {
	if m == nil || m.ReplicationsSent == nil {
		return
	}
	m.ReplicationsSent.Inc()
}

// IncReplicationReceived increments the replication-received counter.
func (m *Metrics) IncReplicationReceived() {
	if m == nil || m.ReplicationsReceived == nil {
		return
	}
	m.ReplicationsReceived.Inc()
}

// AddReplicationBytes adds the given number of bytes to the
// replication-bytes counter for the given direction ("sent" or "received").
func (m *Metrics) AddReplicationBytes(direction string, bytes float64) {
	if m == nil || m.ReplicationBytes == nil {
		return
	}
	m.ReplicationBytes.WithLabelValues(direction).Add(bytes)
}

// IncBroadcastFailure increments the broadcast-failure counter for
// the given invalidation type ("purge" or "ban") and reason ("dial",
// "timeout", "5xx", "marshal").
func (m *Metrics) IncBroadcastFailure(typ, reason string) {
	if m == nil || m.BroadcastFailures == nil {
		return
	}
	m.BroadcastFailures.WithLabelValues(typ, reason).Inc()
}
