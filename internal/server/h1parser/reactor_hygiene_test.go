package h1parser

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/bouine-cache/bouine/pkg/api"
)

// TestReactor_HitCounterBatches pins the telemetry batching contract:
// hits accumulate in the Parser-local pending counter and apply to the
// sink in batches (IncrementReactorHitN), never one shared-counter
// increment per hit. With reactorSpinBudget as the flush threshold,
// serving exactly that many hits must apply one batch.
func TestReactor_HitCounterBatches(t *testing.T) {
	t.Parallel()
	rec := &fakeReactorMetrics{}
	p := New(nil, noopHandler, WithReactorMetrics(rec))

	for range pendingHitFlush - 1 {
		p.noteReactorHit()
	}
	assert.Equal(t, uint64(0), rec.hits, "hits below the batch threshold must not touch the sink")
	assert.Equal(t, uint64(pendingHitFlush-1), p.pendingReactorHits.Load(),
		"the pending counter holds the unflushed hits")

	p.noteReactorHit() // hits the threshold
	assert.Equal(t, uint64(pendingHitFlush), rec.hits,
		"the threshold-crossing hit applies one batch to the sink")
	assert.Equal(t, uint64(0), p.pendingReactorHits.Load(), "the pending counter drains")

	// A handoff also flushes (staleness bound under miss-heavy traffic).
	for range 3 {
		p.noteReactorHit()
	}
	p.noteReactorHandoff(api.ReactorHandoffMiss)
	assert.Equal(t, uint64(pendingHitFlush+3), rec.hits,
		"a handoff flushes the pending batch alongside its own counter")
	assert.Equal(t, uint64(1), rec.handoffs[api.ReactorHandoffMiss])

	// Explicit flush is idempotent on an empty pending counter.
	p.flushReactorHits()
	assert.Equal(t, uint64(pendingHitFlush+3), rec.hits, "empty flush must not double-apply")
}

// TestReactor_HitBatcherAdapter pins api.ReactorHitBatcher: an
// implementation that only knows the single-increment surface adapts
// to the batched method via the embedded helper.
func TestReactor_HitBatcherAdapter(t *testing.T) {
	t.Parallel()
	var calls int
	b := api.ReactorHitBatcher{HitOne: func() { calls++ }}
	b.IncrementReactorHitN(5)
	assert.Equal(t, 5, calls)
	b.IncrementReactorHitN(0)
	assert.Equal(t, 5, calls)
}
