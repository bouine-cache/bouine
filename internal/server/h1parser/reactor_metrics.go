package h1parser

// reactor_metrics.go — the async metrics path for the reactor loop
// (W3 of docs/plans/h1-reactor-perf-round-4.md).
//
// The metrics hook (DataPlaneMetrics.RecordHit: pool-table lookup,
// index switches, counter increments, one histogram Observe) costs
// CPU that produces no cache work. On the blocking path that cost is
// spread across per-connection goroutines; on the reactor it is
// serial loop time added to every multiplexed connection's latency.
// This ring moves the hook invocation off the loop goroutine: the
// hit path pushes a small fixed-size record, a drainer goroutine per
// loop applies the hook.
//
// Which strings may be retained? The hook's arguments come from the
// FastPathResponse (Pool: the handler's stable poolName field,
// CacheResult: "HIT"/"STALE" literals, Source: api.Source constants) —
// none alias the connection read buffer.
//
// Concurrency contract: single producer (the loop goroutine, via
// pushHit), single consumer (the drainer). The ring is an array of
// fixed-size records indexed by atomic head/tail: the producer only
// stores tail after fully populating the slot; the consumer reads
// slots only when head != tail, copying them out before advancing
// head. sync/atomic Store/Load pairs establish the happens-before
// edge, so the race detector sees no torn-slot race.
//
// Overflow policy: drop-newest with a dropped counter. Drop-oldest
// would let the producer rewrite a slot mid-consumer-read (records
// are shared, not double-buffered). Blocking the loop is the cost
// this file exists to avoid. Sustained overflow is observable: the
// counter is read at reactor shutdown and logged (see serveFastPath),
// and the runbook documents the failure mode.

import (
	"sync/atomic"
	"time"
)

// hitMetricsRecord is one fast-path hit observation, value-copied
// through the ring. Strings are stable handler-owned values.
type hitMetricsRecord struct {
	pool        string
	cacheResult string
	source      string
	durNs       int64
	bytesOut    int
	status      int
}

// metricsRingCap is the SPSC ring capacity, a power of two. 2048
// slots cover ~100k RPS of hits against the 20ms drain interval —
// the drop counter only moves if the drainer stalls beyond that.
const metricsRingCap = 2048

// metricsRing is a fixed-capacity SPSC ring of hit records.
type metricsRing struct {
	records [metricsRingCap]hitMetricsRecord
	head    atomic.Uint64 // consumer position (next to pop)
	tail    atomic.Uint64 // producer position (next to push)
	dropped atomic.Uint64
}

// pushHit stores rec without blocking. Returns false (and counts the
// drop) when the ring is full.
func (r *metricsRing) pushHit(rec hitMetricsRecord) bool {
	tail := r.tail.Load()
	head := r.head.Load()
	if tail-head >= metricsRingCap {
		r.dropped.Add(1)
		return false
	}
	r.records[tail&(metricsRingCap-1)] = rec
	r.tail.Store(tail + 1)
	return true
}

// drain pops up to len(dst) records into dst, returning the count. The
// consumer copies records out before advancing head, so a slot is
// never re-written mid-read.
func (r *metricsRing) drain(dst []hitMetricsRecord) int {
	head := r.head.Load()
	tail := r.tail.Load()
	// The ring capacity bounds tail-head by construction (pushHit
	// refuses to exceed it), so the narrowing below is in-range by
	// invariant; the clamp also keeps it within len(dst).
	avail := tail - head
	if avail > uint64(len(dst)) {
		avail = uint64(len(dst))
	}
	n := int(avail) //nolint:gosec // G115: avail is clamped to len(dst) (an int) and bounded by the ring capacity
	for i := range n {
		dst[i] = r.records[(head+uint64(i))&(metricsRingCap-1)]
	}
	r.head.Store(head + uint64(n))
	return n
}

// droppedTotal returns the count of records dropped by a full ring.
func (r *metricsRing) droppedTotal() uint64 { return r.dropped.Load() }

// metricsDrainer is the consumer half: it polls its ring, applies the
// hook to each record, and exits after a final drain once stopped.
// Owned by the reactor transport; one per loop with a metrics hook.
type metricsDrainer struct {
	ring *metricsRing
	hook func(pool, cacheResult, source string, status, bytesOut int, duration time.Duration)
}

// metricsDrainBatch is the drainer's pop batch size; draining loops
// until a batch comes back short, bounding per-tick hook work.
const metricsDrainBatch = 256

// drainOnce applies one batch of records. Reports whether the ring may
// still hold records (the caller loops).
func (d *metricsDrainer) drainOnce() bool {
	var batch [metricsDrainBatch]hitMetricsRecord
	n := d.ring.drain(batch[:])
	for i := range n {
		rec := &batch[i]
		d.hook(rec.pool, rec.cacheResult, rec.source,
			rec.status, rec.bytesOut, time.Duration(rec.durNs))
	}
	return n == len(batch)
}

// run drives the drainer until stop closes, then drains fully and
// returns. The owner runs it on its own goroutine and joins it via the
// returned-when-done contract of drainDone (see reactorEpoll.Close).
func (d *metricsDrainer) run(stop <-chan struct{}, drainDone chan<- struct{}) {
	ticker := time.NewTicker(metricsDrainInterval)
	defer ticker.Stop()
	defer close(drainDone)
	for {
		select {
		case <-stop:
			for d.drainOnce() {
			}
			return
		case <-ticker.C:
			for d.drainOnce() {
			}
		}
	}
}

// metricsDrainInterval paces the drainer's poll: 20ms bounds metric
// staleness at a granularity no scrape interval can observe (Prometheus
// scrapes are 15-30s), at 50 wakeups/s — negligible next to the loop's
// own activity under load.
const metricsDrainInterval = 20 * time.Millisecond
