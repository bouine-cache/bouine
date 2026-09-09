package cluster

import (
	"sync"
	"time"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/api"
)

// broadcastBatchSize is the event count that triggers an immediate
// flush of the invalidation batcher (ADR-0044). 256 events × ~50 B per
// purge event ≈ 13 KiB, well inside memberlist's UDP gossip budget
// and a single HTTP request.
const broadcastBatchSize = 256

// broadcastBatchFlushInterval bounds how long an event may wait in
// the batcher before a flush is forced, so low-traffic purge latency
// stays within 10 ms.
const broadcastBatchFlushInterval = 10 * time.Millisecond

// broadcastQueueCap bounds the batcher's pending queue. On overflow,
// events bypass the batcher and are sent unbatched immediately —
// delivery is preserved at the cost of the batching win.
const broadcastQueueCap = 4096

// invalidationBatcher buffers purge and refresh events and flushes
// them in batches of broadcastBatchSize or every
// broadcastBatchFlushInterval (ADR-0044). It converts a per-event
// per-peer goroutine + HTTP POST fan-out into one flush with one POST
// per peer per batch.
type invalidationBatcher struct {
	logger       observability.Logger
	flushPurge   func([]api.PurgeEvent)
	flushRefresh func([]api.RefreshEvent)
	onOverflow   func()
	metrics      *Metrics
	wakeup       chan struct{}
	done         chan struct{}
	refreshQueue []api.RefreshEvent
	purgeQueue   []api.PurgeEvent
	wg           sync.WaitGroup
	refreshMu    sync.Mutex
	purgeMu      sync.Mutex
	// purgeFlushing is true while a synchronous (idle-path) flush is
	// delivering purges. Events arriving in that window queue for the
	// next batch instead of each triggering its own synchronous flush.
	// refreshFlushing mirrors it for refresh events.
	purgeFlushing   bool
	refreshFlushing bool
}

// newInvalidationBatcher starts the flush loop goroutine. Close stops
// it and flushes any remaining events.
func newInvalidationBatcher(
	logger observability.Logger,
	metrics *Metrics,
	flushPurge func([]api.PurgeEvent),
	flushRefresh func([]api.RefreshEvent),
	onOverflow func(),
) *invalidationBatcher {
	b := &invalidationBatcher{
		logger:       logger,
		metrics:      metrics,
		flushPurge:   flushPurge,
		flushRefresh: flushRefresh,
		onOverflow:   onOverflow,
		wakeup:       make(chan struct{}, 1),
		done:         make(chan struct{}),
	}
	b.wg.Add(1)
	go b.loop()
	return b
}

// enqueuePurge absorbs a purge event. It returns (batch, true) when
// the caller must flush synchronously — either the queue is idle (the
// common single-purge case, which keeps the historical fan-out-before-
// return latency the admin API relies on) or the queue is full
// (overflow). The returned batch contains evt and, on the idle path,
// anything the flush loop had not yet picked up. It returns (nil,
// false) when the batcher keeps the event for coalesced delivery.
func (b *invalidationBatcher) enqueuePurge(evt api.PurgeEvent) ([]api.PurgeEvent, bool) {
	b.purgeMu.Lock()
	if len(b.purgeQueue) >= broadcastQueueCap {
		b.purgeMu.Unlock()
		b.onOverflow()
		return []api.PurgeEvent{evt}, true
	}
	if len(b.purgeQueue) > 0 || b.purgeFlushing {
		b.purgeQueue = append(b.purgeQueue, evt)
		b.purgeMu.Unlock()
		b.signal()
		return nil, false
	}
	// Idle: nothing queued and nothing in flight — hand evt to the
	// caller for synchronous delivery and mark the window so
	// concurrent enqueues queue behind it instead of each flushing.
	b.purgeFlushing = true
	b.purgeMu.Unlock()
	return []api.PurgeEvent{evt}, true
}

// donePurgeFlush clears the synchronous-flush window. Called by the
// caller after its synchronous flushPurge returns.
func (b *invalidationBatcher) donePurgeFlush() {
	b.purgeMu.Lock()
	b.purgeFlushing = false
	b.purgeMu.Unlock()
}

// takePurgeBatch drains the entire purge queue and returns it. Used
// by the flush loop and drain paths; the queue is left empty.
func (b *invalidationBatcher) takePurgeBatch() []api.PurgeEvent {
	b.purgeMu.Lock()
	batch := b.purgeQueue
	b.purgeQueue = nil
	b.purgeMu.Unlock()
	return batch
}

// enqueueRefresh absorbs a refresh event. Same contract as
// enqueuePurge.
func (b *invalidationBatcher) enqueueRefresh(evt api.RefreshEvent) ([]api.RefreshEvent, bool) {
	b.refreshMu.Lock()
	if len(b.refreshQueue) >= broadcastQueueCap {
		b.refreshMu.Unlock()
		b.onOverflow()
		return []api.RefreshEvent{evt}, true
	}
	if len(b.refreshQueue) > 0 || b.refreshFlushing {
		b.refreshQueue = append(b.refreshQueue, evt)
		b.refreshMu.Unlock()
		b.signal()
		return nil, false
	}
	b.refreshFlushing = true
	b.refreshMu.Unlock()
	return []api.RefreshEvent{evt}, true
}

// doneRefreshFlush clears the synchronous-flush window for refresh
// events.
func (b *invalidationBatcher) doneRefreshFlush() {
	b.refreshMu.Lock()
	b.refreshFlushing = false
	b.refreshMu.Unlock()
}

// takeRefreshBatch drains the entire refresh queue and returns it.
func (b *invalidationBatcher) takeRefreshBatch() []api.RefreshEvent {
	b.refreshMu.Lock()
	batch := b.refreshQueue
	b.refreshQueue = nil
	b.refreshMu.Unlock()
	return batch
}

// signal wakes the flush loop. Non-blocking: at most one wakeup is
// pending; the loop drains everything on wake.
func (b *invalidationBatcher) signal() {
	select {
	case b.wakeup <- struct{}{}:
	default:
	}
}

// loop flushes queued events on batch-size or interval triggers.
func (b *invalidationBatcher) loop() {
	defer b.wg.Done()
	ticker := time.NewTicker(broadcastBatchFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.done:
			b.drainFlush()
			return
		case <-b.wakeup:
			b.flushDue()
		case <-ticker.C:
			b.drainFlush()
		}
	}
}

// flushDue flushes any queue that has reached broadcastBatchSize.
func (b *invalidationBatcher) flushDue() {
	if batch := b.takePurgeBatch(); len(batch) >= broadcastBatchSize {
		b.flushPurge(batch)
	} else if len(batch) > 0 {
		// Under threshold: re-queue; the interval timer will flush.
		b.purgeMu.Lock()
		b.purgeQueue = append(batch, b.purgeQueue...)
		b.purgeMu.Unlock()
	}
	if batch := b.takeRefreshBatch(); len(batch) >= broadcastBatchSize {
		b.flushRefresh(batch)
	} else if len(batch) > 0 {
		b.refreshMu.Lock()
		b.refreshQueue = append(batch, b.refreshQueue...)
		b.refreshMu.Unlock()
	}
}

// drainFlush flushes all remaining events regardless of size.
// Called on each interval tick and on Close: interval delivery bounds
// the latency of sub-threshold batches to broadcastBatchFlushInterval.
func (b *invalidationBatcher) drainFlush() {
	if batch := b.takePurgeBatch(); len(batch) > 0 {
		b.flushPurge(batch)
	}
	if batch := b.takeRefreshBatch(); len(batch) > 0 {
		b.flushRefresh(batch)
	}
}

// close stops the loop and flushes pending events. It joins the loop
// goroutine so Close semantics match the engine's supervised shutdown.
func (b *invalidationBatcher) close() {
	close(b.done)
	b.wg.Wait()
}
