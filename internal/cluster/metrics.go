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
	// ReplicationsSent tracks full-mode replication events sent via
	// gossip (full mode only).
	ReplicationsSent prometheus.Counter
	// ReplicationsReceived tracks full-mode replication events received
	// via HTTP and stored locally (full mode only).
	ReplicationsReceived prometheus.Counter
	// ReplicationsDropped counts replication POSTs dropped because the
	// semaphore was full or the peer POST failed (full mode only).
	// Anti-entropy heals any gaps. Sustained growth indicates the cluster
	// is overloaded or peers are unreachable.
	ReplicationsDropped prometheus.Counter
	// ReplicationBytes tracks the approximate byte size of replicated
	// objects sent or received via HTTP.
	ReplicationBytes *prometheus.CounterVec
	// BroadcastFailures counts HTTP fan-out failures by type (purge, ban),
	// labelled by reason (dial, timeout, 5xx). Non-zero indicates peers
	// may have missed an invalidation; gossip provides redundant delivery.
	BroadcastFailures *prometheus.CounterVec
	// AntiEntropyReconcile counts anti-entropy reconciliation rounds.
	AntiEntropyReconcile prometheus.Counter
	// AntiEntropyRepaired counts individual keys backfilled.
	AntiEntropyRepaired prometheus.Counter
	// AntiEntropyKeysRepaired is the gauge of keys repaired in the last round.
	AntiEntropyKeysRepaired prometheus.Gauge
	// AntiEntropyFetchFailures counts peer key-set fetch failures.
	AntiEntropyFetchFailures prometheus.Counter
	// AntiEntropyCooldownSkips counts keys skipped as "missing" because
	// they were within their backfill cooldown window (#187). Non-zero
	// indicates SIEVE is evicting freshly-backfilled keys before the next
	// round; sustained growth suggests the hot tier is undersized.
	AntiEntropyCooldownSkips prometheus.Counter
	// AntiEntropyChurnSkips counts anti-entropy rounds skipped because
	// SIEVE was evicting recently-backfilled keys faster than the
	// reconciler was inserting them (#187, fix #5). Sustained increments
	// indicate the hot tier is undersized for the working set — backfill
	// is wasted work until the tier grows or the working set shrinks.
	AntiEntropyChurnSkips prometheus.Counter
	// OnReplicationBytes, if set, is invoked on every replication byte
	// record with the direction ("sent"/"received") and byte count. Used
	// to feed the dashboard's replication ring without coupling the
	// cluster package to the observability layer.
	OnReplicationBytes func(direction string, bytes float64)

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
			Help:      "Cached objects received from peers via HTTP and stored locally in full mode.",
		}),
		ReplicationsDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "cluster_replications_dropped_total",
			Help:      "Replication POSTs dropped because the semaphore was full or the peer POST failed. Anti-entropy heals any gaps.",
		}),
		ReplicationBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "cluster_replication_bytes_total",
			Help:      "Approximate byte size of replicated objects sent or received via HTTP.",
		}, []string{"direction"}),
		BroadcastFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "cluster_broadcast_failures_total",
			Help:      "HTTP fan-out failures by invalidation type and reason. Gossip provides redundant delivery.",
		}, []string{"type", "reason"}),
	}
	m.initAntiEntropyMetrics()
	reg.MustRegister(
		m.ModeInfo,
		m.InvalidationsGossip,
		m.InvalidationsHTTP,
		m.ReplicationsSent,
		m.ReplicationsReceived,
		m.ReplicationsDropped,
		m.ReplicationBytes,
		m.BroadcastFailures,
		m.AntiEntropyReconcile,
		m.AntiEntropyRepaired,
		m.AntiEntropyKeysRepaired,
		m.AntiEntropyFetchFailures,
		m.AntiEntropyCooldownSkips,
		m.AntiEntropyChurnSkips,
	)
	return m
}

// initAntiEntropyMetrics creates the anti-entropy counters and gauges on
// m. Extracted from RegisterMetrics to keep it under the funlen limit.
func (m *Metrics) initAntiEntropyMetrics() {
	m.AntiEntropyReconcile = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "cluster_anti_entropy_reconcile_total",
		Help:      "Anti-entropy reconciliation rounds completed.",
	})
	m.AntiEntropyRepaired = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "cluster_anti_entropy_repaired_total",
		Help:      "Cache keys backfilled by the anti-entropy reconciler.",
	})
	m.AntiEntropyKeysRepaired = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "bouine",
		Name:      "cluster_anti_entropy_keys_repaired",
		Help:      "Keys repaired in the last anti-entropy round.",
	})
	m.AntiEntropyFetchFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "cluster_anti_entropy_fetch_failures_total",
		Help:      "Anti-entropy peer key-set fetch failures.",
	})
	m.AntiEntropyCooldownSkips = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "cluster_anti_entropy_cooldown_skips_total",
		Help:      "Keys skipped as missing by anti-entropy because they were within their backfill cooldown window.",
	})
	m.AntiEntropyChurnSkips = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "bouine",
		Name:      "cluster_anti_entropy_churn_skips_total",
		Help:      "Anti-entropy rounds skipped because SIEVE was evicting recently-backfilled keys faster than the reconciler inserted them.",
	})
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

// IncReplicationDropped increments the replication-dropped counter.
func (m *Metrics) IncReplicationDropped() {
	if m == nil || m.ReplicationsDropped == nil {
		return
	}
	m.ReplicationsDropped.Inc()
}

// AddReplicationBytes adds the given number of bytes to the
// replication-bytes counter for the given direction ("sent" or "received").
func (m *Metrics) AddReplicationBytes(direction string, bytes float64) {
	if m == nil {
		return
	}
	if m.OnReplicationBytes != nil {
		m.OnReplicationBytes(direction, bytes)
	}
	if m.ReplicationBytes == nil {
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
	m.broadcastFailuresTotal.Add(1)
}

// BroadcastFailuresCount returns the total number of broadcast failures
// across all types and reasons. Used by the dashboard insights engine.
func (m *Metrics) BroadcastFailuresCount() int64 {
	if m == nil {
		return 0
	}
	return m.broadcastFailuresTotal.Load()
}

// IncAntiEntropyReconcile increments the reconcile counter for the
// given direction ("sent" or "received").
func (m *Metrics) IncAntiEntropyReconcile() {
	if m == nil || m.AntiEntropyReconcile == nil {
		return
	}
	m.AntiEntropyReconcile.Inc()
}

// IncAntiEntropyRepaired increments the repaired-key counter.
func (m *Metrics) IncAntiEntropyRepaired() {
	if m == nil || m.AntiEntropyRepaired == nil {
		return
	}
	m.AntiEntropyRepaired.Inc()
}

// SetAntiEntropyKeysRepaired sets the gauge for keys repaired in the
// last round.
func (m *Metrics) SetAntiEntropyKeysRepaired(v float64) {
	if m == nil || m.AntiEntropyKeysRepaired == nil {
		return
	}
	m.AntiEntropyKeysRepaired.Set(v)
}

// IncAntiEntropyFetchFailure increments the peer key-set fetch failure counter.
func (m *Metrics) IncAntiEntropyFetchFailure() {
	if m == nil || m.AntiEntropyFetchFailures == nil {
		return
	}
	m.AntiEntropyFetchFailures.Inc()
}

// AddAntiEntropyCooldownSkips adds n to the cooldown-skips counter, which
// tracks keys skipped as "missing" because they were within their backfill
// cooldown window (#187).
func (m *Metrics) AddAntiEntropyCooldownSkips(n float64) {
	if m == nil || m.AntiEntropyCooldownSkips == nil {
		return
	}
	m.AntiEntropyCooldownSkips.Add(n)
}

// IncAntiEntropyChurnSkip increments the churn-skips counter, which tracks
// anti-entropy rounds skipped because SIEVE was evicting recently-backfilled
// keys faster than the reconciler inserted them (#187, fix #5).
func (m *Metrics) IncAntiEntropyChurnSkip() {
	if m == nil || m.AntiEntropyChurnSkips == nil {
		return
	}
	m.AntiEntropyChurnSkips.Inc()
}
