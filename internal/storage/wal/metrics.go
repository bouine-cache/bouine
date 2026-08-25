package wal

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds Prometheus collectors for WAL write operations. Nil
// pointers are safe to use — Inc/Observe/Set are no-ops when the
// underlying collector is nil. This lets deployments without a WAL
// (ephemeral mode) skip registration entirely.
//
// Stable.
type Metrics struct {
	// WriteDuration measures the wall time of a single drainAndSync
	// cycle — the batch write + fsync. High values indicate disk I/O
	// pressure.
	WriteDuration prometheus.Histogram
	// WriteQueueDepth is the current number of entries buffered in the
	// async sync channel. Near syncChSize means drops are imminent.
	WriteQueueDepth prometheus.Gauge
	// WriteTotal counts all async WAL write attempts (Enqueue +
	// EnqueueBatch entries), including entries that were dropped because
	// the async channel was full. Sync-mode writes (when syncCh is nil)
	// are not counted — in sync mode there are no drops, so the drop rate
	// is always 0. Combined with WALDroppedEntries, computes the drop
	// rate: drops / writes.
	WriteTotal prometheus.Counter
}

// RegisterMetrics creates and registers WAL Prometheus collectors on
// the provided registerer. Returns a Metrics handle safe for concurrent
// use. A nil registerer returns a zero-value Metrics (all no-ops).
func RegisterMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		return &Metrics{}
	}
	m := &Metrics{
		WriteDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "bouine",
			Name:      "wal_write_duration_seconds",
			Help:      "Wall time of a single WAL drain-and-sync cycle (batch write + fsync). High values indicate disk I/O pressure.",
			Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
		}),
		WriteQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bouine",
			Name:      "wal_write_queue_depth",
			Help:      "Current number of entries buffered in the async WAL sync channel. Near the channel capacity (4096) means drops are imminent.",
		}),
		WriteTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bouine",
			Name:      "wal_write_total",
			Help:      "Total WAL write attempts (Enqueue + EnqueueBatch entries, including drops). Drop rate: bouine_wal_dropped_entries_total / bouine_wal_write_total.",
		}),
	}
	reg.MustRegister(m.WriteDuration, m.WriteQueueDepth, m.WriteTotal)
	return m
}

// ObserveWriteDuration records the duration of a drain-and-sync cycle.
// Safe to call on a nil Metrics.
func (m *Metrics) ObserveWriteDuration(d time.Duration) {
	if m == nil || m.WriteDuration == nil {
		return
	}
	m.WriteDuration.Observe(d.Seconds())
}

// SetQueueDepth sets the current async channel depth gauge. Safe to
// call on a nil Metrics.
func (m *Metrics) SetQueueDepth(depth int) {
	if m == nil || m.WriteQueueDepth == nil {
		return
	}
	m.WriteQueueDepth.Set(float64(depth))
}

// IncWriteTotal increments the write attempt counter by n. Safe to call
// on a nil Metrics.
func (m *Metrics) IncWriteTotal(n int) {
	if m == nil || m.WriteTotal == nil {
		return
	}
	m.WriteTotal.Add(float64(n))
}
