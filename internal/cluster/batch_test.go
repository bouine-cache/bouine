package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

// gossipOversizedDrops reads the oversized-drop counter from the
// registry without pulling the prometheus testutil dependency (which
// would add an indirect module requirement).
func gossipOversizedDrops(t *testing.T, reg *prometheus.Registry) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == "bouine_cluster_gossip_oversized_drops_total" {
			require.Len(t, mf.GetMetric(), 1)
			return mf.GetMetric()[0].GetCounter().GetValue()
		}
	}
	return 0
}

func samplePurgeEvents(n int) []api.PurgeEvent {
	evts := make([]api.PurgeEvent, n)
	for i := range n {
		evts[i] = api.PurgeEvent{
			Key:      testkey.Key(uint64(i)),
			VaryKey:  "gzip",
			Issuer:   "node-0",
			IssuedAt: time.Now(),
			Seq:      uint64(i + 1),
		}
	}
	return evts
}

func sampleRefreshEvents(n int) []api.RefreshEvent {
	evts := make([]api.RefreshEvent, n)
	for i := range n {
		evts[i] = api.RefreshEvent{
			Key:      testkey.Key(uint64(i)),
			Issuer:   "node-0",
			IssuedAt: time.Now(),
			Seq:      uint64(i + 1),
		}
	}
	return evts
}

// TestPurgeBatchGossip_RoundTrip verifies the batch gossip codec
// round-trips every event byte-for-byte.
func TestPurgeBatchGossip_RoundTrip(t *testing.T) {
	t.Parallel()
	evts := samplePurgeEvents(64)
	body, err := EncodePurgeBatchGossip(evts)
	require.NoError(t, err)
	got, err := DecodePurgeBatchGossip(body)
	require.NoError(t, err)
	require.Len(t, got, len(evts))
	for i := range evts {
		assert.Equal(t, evts[i].Key, got[i].Key)
		assert.Equal(t, evts[i].VaryKey, got[i].VaryKey)
		assert.Equal(t, evts[i].Issuer, got[i].Issuer)
		assert.Equal(t, evts[i].Seq, got[i].Seq)
		assert.True(t, evts[i].IssuedAt.Equal(got[i].IssuedAt))
	}
}

// TestPurgeBatchHTTP_RoundTrip verifies the batch HTTP codec.
func TestPurgeBatchHTTP_RoundTrip(t *testing.T) {
	t.Parallel()
	evts := samplePurgeEvents(256)
	body, err := EncodePurgeBatchHTTP(evts)
	require.NoError(t, err)
	got, err := DecodePurgeBatchHTTP(body)
	require.NoError(t, err)
	require.Len(t, got, 256)
	assert.Equal(t, evts[255].Key, got[255].Key)
}

// TestPurgeBatchDecode_RejectsBadFrames verifies decode failures:
// short frame, bad magic, wrong version, wrong msgType, oversized
// count, trailing bytes.
func TestPurgeBatchDecode_RejectsBadFrames(t *testing.T) {
	t.Parallel()
	good, err := EncodePurgeBatchGossip(samplePurgeEvents(2))
	require.NoError(t, err)

	_, err = DecodePurgeBatchGossip(good[:4])
	assert.Error(t, err, "short frame")

	corrupt := append([]byte(nil), good...)
	corrupt[0] = 0x00
	_, err = DecodePurgeBatchGossip(corrupt)
	assert.Error(t, err, "bad magic")

	corrupt = append([]byte(nil), good...)
	corrupt[1] = 9
	_, err = DecodePurgeBatchGossip(corrupt)
	assert.Error(t, err, "unsupported version")

	oversized, err := EncodePurgeBatchGossip(nil)
	require.NoError(t, err)
	oversized[3] = 0xFF
	oversized[4] = 0xFF
	oversized[5] = 0xFF
	oversized[6] = 0x7F
	_, err = DecodePurgeBatchGossip(oversized)
	assert.Error(t, err, "count over batchMaxEvents")
}

// TestRefreshBatch_CodecRoundTrip covers both refresh batch wire
// variants.
func TestRefreshBatch_CodecRoundTrip(t *testing.T) {
	t.Parallel()
	evts := sampleRefreshEvents(3)

	gbody, err := EncodeRefreshBatchGossip(evts)
	require.NoError(t, err)
	got, err := DecodeRefreshBatchGossip(gbody)
	require.NoError(t, err)
	require.Len(t, got, 3)
	for i := range evts {
		assert.Equal(t, evts[i].Key, got[i].Key)
		assert.Equal(t, evts[i].Seq, got[i].Seq)
	}

	hbody, err := EncodeRefreshBatchHTTP(evts)
	require.NoError(t, err)
	hgot, err := DecodeRefreshBatchHTTP(hbody)
	require.NoError(t, err)
	require.Len(t, hgot, 3)
}

// TestSeqTracker_DedupsPerIssuer verifies the receive-side dedup:
// increasing sequences pass, replays and regressions drop, and empty
// issuers never dedup.
func TestSeqTracker_DedupsPerIssuer(t *testing.T) {
	t.Parallel()
	tr := newSeqTracker()

	assert.False(t, tr.seen("a", 1))
	assert.False(t, tr.seen("a", 2))
	assert.True(t, tr.seen("a", 2), "replay must drop")
	assert.True(t, tr.seen("a", 1), "regression must drop")
	assert.False(t, tr.seen("b", 1), "independent issuer")
	assert.False(t, tr.seen("", 1), "empty issuer never drops")
	assert.False(t, tr.seen("", 1), "empty issuer never drops")
}

// TestSeqTracker_RestartRearmsAfterTTL verifies a restarted issuer
// with a fresh sequence is accepted once the old high-watermark
// expires.
func TestSeqTracker_RestartRearmsAfterTTL(t *testing.T) {
	t.Parallel()
	tr := newSeqTracker()
	now := time.Now()
	tr.now = func() time.Time { return now }

	assert.False(t, tr.seen("a", 100))
	now = now.Add(seqTrackerTTL + time.Minute)
	assert.False(t, tr.seen("a", 1), "fresh sequence after TTL must be accepted")
}

// TestSeqTracker_AcceptsOutOfOrderWithinWindow verifies split batch
// frames arriving out of order (separate datagrams, memberlist's LIFO
// handoff) are applied rather than swallowed by the high-watermark
// regression rule (issue #754). Only true replays within the window
// dedup; Seqs older than the window still drop as stale.
func TestSeqTracker_AcceptsOutOfOrderWithinWindow(t *testing.T) {
	t.Parallel()
	tr := newSeqTracker()

	// Frames delivered out of order: 60-79 arrive after 0-59 and 80-99.
	for _, seq := range []uint64{0, 20, 40, 59, 80, 99} {
		assert.False(t, tr.seen("a", seq), "first sight of seq %d must apply", seq)
	}
	for _, seq := range []uint64{60, 79, 70} {
		assert.False(t, tr.seen("a", seq), "out-of-order seq %d within the window must apply", seq)
	}
	// Delivered events replay as duplicates exactly once.
	delivered := []uint64{0, 20, 40, 59, 80, 99, 60, 79, 70}
	for _, seq := range delivered {
		assert.True(t, tr.seen("a", seq), "replay of seq %d must dedup", seq)
	}
	// Stale replay far below the watermark drops: advance the
	// watermark past the window first.
	assert.False(t, tr.seen("a", seqDedupWindow*2))
	assert.True(t, tr.seen("a", 1), "seq below the window is a stale replay")
}

// TestSeqTracker_WindowBoundaryRoundTrip exercises the bitmap slide:
// sequences advancing past the window wrap cleanly, and a gap crossing
// the 4096 boundary dedups correctly.
func TestSeqTracker_WindowBoundaryRoundTrip(t *testing.T) {
	t.Parallel()
	tr := newSeqTracker()

	// Warm up, then walk forward in window-sized strides with
	// out-of-order fills behind the watermark. Strides start above
	// seqDedupWindow so the stale-seq computation never underflows.
	assert.False(t, tr.seen("a", 1))
	for s := uint64(2) * seqDedupWindow; s <= seqDedupWindow*4; s += seqDedupWindow {
		assert.False(t, tr.seen("a", s), "stride %d must advance the window", s)
		assert.True(t, tr.seen("a", s), "replay of stride %d must dedup", s)
		// An out-of-order seq from the previous window's range is now
		// stale (beyond the window) and must drop.
		assert.True(t, tr.seen("a", s-seqDedupWindow-1), "stale seq below the window must drop")
		// A fresh out-of-order seq within the window still applies.
		assert.False(t, tr.seen("a", s-1), "adjacent out-of-order seq must apply")
	}
}

// TestBatcher_IdleDeliversSynchronously verifies the idle-queue
// contract: an event arriving on an empty queue is returned to the
// caller for synchronous delivery, never queued.
func TestBatcher_IdleDeliversSynchronously(t *testing.T) {
	t.Parallel()
	b := newInvalidationBatcher(nil, nil, func([]api.PurgeEvent) {}, func([]api.RefreshEvent) {}, func() {})
	defer b.close()

	batch, deliver := b.enqueuePurge(api.PurgeEvent{Issuer: "n0", Seq: 1})
	require.True(t, deliver)
	require.Len(t, batch, 1)

	rbatch, deliver := b.enqueueRefresh(api.RefreshEvent{Issuer: "n0", Seq: 1})
	require.True(t, deliver)
	require.Len(t, rbatch, 1)
}

// TestBatcher_AsyncEnqueueNeverDeliversSynchronously pins the contract
// the data-plane invalidation hook relies on (issue #753): the async
// variant never returns the idle-queue synchronous flush, so a proxied
// request's fan-out cost is one enqueue; the flush loop delivers the
// event instead. Overflow keeps the delivery-preserving fallback.
func TestBatcher_AsyncEnqueueNeverDeliversSynchronously(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var purges int
	b := newInvalidationBatcher(
		nil,
		nil,
		func(evts []api.PurgeEvent) { mu.Lock(); purges += len(evts); mu.Unlock() },
		func([]api.RefreshEvent) {},
		func() {},
	)

	// Idle queue: the sync variant would deliver synchronously; the
	// async variant must queue for the flush loop.
	batch, deliver := b.enqueuePurgeAsync(api.PurgeEvent{Issuer: "n0", Seq: 1})
	require.False(t, deliver, "async enqueue must never take the idle synchronous path")
	require.Nil(t, batch)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return purges == 1
	}, broadcastBatchFlushInterval*10, time.Millisecond,
		"flush loop must deliver the async-enqueued event")

	// Overflow: delivery is preserved via the unbatched fallback.
	// Join the flush loop before stuffing: a 10 ms tick under -race
	// on a slow CI runner can drain the queue between appends (4097
	// lock cycles exceed one interval), which would drop the queue
	// below cap and break the capacity check. With the loop joined
	// the queue is stable and the overflow path is deterministic.
	b.close()
	for i := range broadcastQueueCap + 1 {
		b.purgeMu.Lock()
		b.purgeQueue = append(b.purgeQueue, api.PurgeEvent{Issuer: "n0", Seq: uint64(100 + i)})
		b.purgeMu.Unlock()
	}
	over, deliver := b.enqueuePurgeAsync(api.PurgeEvent{Issuer: "n0", Seq: 999})
	require.True(t, deliver, "overflow must return the unbatched fallback batch")
	require.Len(t, over, 1)
}

// TestBatcher_StormCoalescesAndFlushes verifies the storm path: once
// events are queued, subsequent events coalesce and the interval
// flush delivers them as one batch.
func TestBatcher_StormCoalescesAndFlushes(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var purges int
	b := newInvalidationBatcher(
		nil,
		nil,
		func(evts []api.PurgeEvent) { mu.Lock(); purges += len(evts); mu.Unlock() },
		func([]api.RefreshEvent) {},
		func() {},
	)
	defer b.close()

	// First event on the idle queue: caller delivers synchronously.
	_, deliver := b.enqueuePurge(api.PurgeEvent{Issuer: "n0", Seq: 1})
	require.True(t, deliver)
	// Prime the queue so the next events coalesce: simulate a caller
	// that has not yet flushed by enqueueing behind a pending batch.
	// Use takePurgeBatch's inverse — append directly, as flushDue
	// would re-queue.
	b.purgeMu.Lock()
	b.purgeQueue = append(b.purgeQueue, api.PurgeEvent{Issuer: "n0", Seq: 2})
	b.purgeMu.Unlock()
	for i := 3; i <= 10; i++ {
		_, deliver = b.enqueuePurge(api.PurgeEvent{Issuer: "n0", Seq: uint64(i)})
		require.False(t, deliver, "queued event must coalesce")
	}
	// The interval tick (or wakeup) flushes the queued storm.
	b.signal()
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return purges == 9
	}, 2*time.Second, 5*time.Millisecond, "queued events must flush as one batch")
}

// TestGossipPurgeBatch_AppliesAndDedups verifies the gossip receive
// path applies every event in a batch exactly once, and that a replay
// of the same batch is fully deduped.
func TestGossipPurgeBatch_AppliesAndDedups(t *testing.T) {
	t.Parallel()
	c := minimalCluster(t, "node-1")
	c.seqs = newSeqTracker() // minimalCluster skips New; wire the tracker explicitly
	var mu sync.Mutex
	var applied []api.PurgeEvent
	c.SetInvalidator(Invalidator{
		PurgeFn: func(_ context.Context, evt api.PurgeEvent) error {
			mu.Lock()
			applied = append(applied, evt)
			mu.Unlock()
			return nil
		},
	})
	body, err := EncodePurgeBatchGossip(samplePurgeEvents(8))
	require.NoError(t, err)

	c.NotifyMsg(body)
	c.NotifyMsg(body) // replay via the HTTP path's gossip fallback

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, applied, 8, "duplicate batch delivery must dedup")
}

// TestBroadcastPurge_BatchesUnderStorm verifies that a concurrent
// burst of purge events coalesces into a small number of batched HTTP
// POSTs per peer instead of one POST per event. Serial callers each
// find an idle queue and deliver synchronously; batching emerges from
// overlapping enqueues, so the burst is issued from many goroutines.
func TestBroadcastPurge_BatchesUnderStorm(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := fasthttptest.NewServer(t, func(_ *fasthttp.RequestCtx) {
		calls.Add(1)
	})
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.peers["node-1"] = &Member{Info: api.PeerInfo{Name: "node-1", AdminAddr: srv.Addr}}

	b := NewBroadcaster(c, nil)
	defer b.Close()

	const events = 300
	var start sync.WaitGroup
	start.Add(1)
	var wg sync.WaitGroup
	for range events {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start.Wait() // maximize enqueue overlap
			b.BroadcastPurge(context.Background(), testkey.Key(1), "")
		}()
	}
	start.Done()
	wg.Wait()

	// Concurrent enqueues coalesce: at most a handful of batches. The
	// exact count depends on scheduling; what matters is orders of
	// magnitude below one POST per event.
	require.LessOrEqual(t, int(calls.Load()), events/10, "burst must not produce one POST per event")
}

// TestEncodePurgeBatchGossipBudgeted_SplitsUnderBudget verifies the
// frame-splitting encoder (issue #754): every frame fits the gossip
// UDP window, every event is delivered exactly once, and decode
// round-trips each sub-batch independently.
func TestEncodePurgeBatchGossipBudgeted_SplitsUnderBudget(t *testing.T) {
	t.Parallel()
	evts := samplePurgeEvents(256)

	frames, err := EncodePurgeBatchGossipBudgeted(evts, gossipFrameBudget)
	require.NoError(t, err)
	require.NotEmpty(t, frames, "256 events must produce frames")

	var got []api.PurgeEvent
	for i, frame := range frames {
		require.LessOrEqualf(t, len(frame), gossipFrameBudget,
			"frame %d of %d is %d bytes, exceeds budget %d",
			i, len(frames), len(frame), gossipFrameBudget)
		decoded, err := DecodePurgeBatchGossip(frame)
		require.NoErrorf(t, err, "frame %d must decode standalone", i)
		got = append(got, decoded...)
	}
	require.Len(t, got, len(evts), "split frames must carry every event exactly once")
	for i := range evts {
		assert.Equal(t, evts[i].Key, got[i].Key, "event order must be preserved")
	}
}

// TestEncodePurgeBatchGossipBudgeted_NoSplitWhenSmall verifies small
// batches encode as a single frame under the budget — the common case
// stays one datagram.
func TestEncodePurgeBatchGossipBudgeted_NoSplitWhenSmall(t *testing.T) {
	t.Parallel()
	frames, err := EncodePurgeBatchGossipBudgeted(samplePurgeEvents(4), gossipFrameBudget)
	require.NoError(t, err)
	require.Len(t, frames, 1)
}

// TestEncodeRefreshBatchGossipBudgeted_SplitsUnderBudget mirrors the
// purge split test for refresh batches.
func TestEncodeRefreshBatchGossipBudgeted_SplitsUnderBudget(t *testing.T) {
	t.Parallel()
	evts := sampleRefreshEvents(256)

	frames, err := EncodeRefreshBatchGossipBudgeted(evts, gossipFrameBudget)
	require.NoError(t, err)
	require.NotEmpty(t, frames)

	var got []api.RefreshEvent
	for i, frame := range frames {
		require.LessOrEqualf(t, len(frame), gossipFrameBudget, "frame %d exceeds budget", i)
		decoded, err := DecodeRefreshBatchGossip(frame)
		require.NoErrorf(t, err, "frame %d must decode standalone", i)
		got = append(got, decoded...)
	}
	require.Len(t, got, len(evts))
}

// TestFlushPurgeBatch_EnqueuesBudgetedFrames verifies the flush path
// end-to-end through the real queue: a 256-event flush leaves only
// frames deliverable by memberlist's gossip round (issue #754).
func TestFlushPurgeBatch_EnqueuesBudgetedFrames(t *testing.T) {
	t.Parallel()
	c := minimalCluster(t, "node-0")
	reg := prometheus.NewRegistry()
	c.metrics.Store(RegisterMetrics(reg))

	b := NewBroadcaster(c, nil)
	b.flushPurgeBatch(samplePurgeEvents(256))
	b.Close()

	// Drain the gossip queue the way memberlist does: per round with
	// compound/userMsg overhead and the default LAN limit.
	const compoundOverhead = 2
	const userMsgOverhead = 1
	rounds := 0
	for {
		msgs := c.GetBroadcasts(compoundOverhead+userMsgOverhead, 1395)
		if len(msgs) == 0 {
			break
		}
		rounds++
		require.Less(t, rounds, 5000, "queue must drain; oversized frame loops forever")
	}
	require.NotZero(t, rounds, "batched flush must deliver frames")
}

// TestGetBroadcasts_DropsOversizedFrame pins the drain-side valve
// (issue #754): a frame that cannot fit any gossip round is dropped
// with a metric instead of wedging the queue forever, and smaller
// frames behind it still deliver.
func TestGetBroadcasts_DropsOversizedFrame(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	c := minimalCluster(t, "node-0")
	c.metrics.Store(m)

	small := make([]byte, 64)
	c.QueueBroadcast(make([]byte, gossipMaxFrame+1))
	c.QueueBroadcast(small)

	// memberlist's call shape: overhead 3, limit 1395 (DefaultLANConfig
	// UDPBufferSize 1400 minus compound/header overheads).
	msgs := c.GetBroadcasts(3, 1395)

	require.Len(t, msgs, 1, "only the deliverable frame may return")
	require.Equal(t, small, msgs[0])
	require.Empty(t, c.gossipQueue, "oversized frame must not be re-queued")
	dropped := gossipOversizedDrops(t, reg)
	require.Equal(t, 1.0, dropped, "drop must be counted once")
}

// TestGetBroadcasts_RequeuesFullRoundFrames verifies the valve does not
// over-fire: a frame that fits a full gossip round but not the current
// round's remaining budget (memberlist's own traffic) is re-queued, and
// the oversized-drop counter stays at zero.
func TestGetBroadcasts_RequeuesFullRoundFrames(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	c := minimalCluster(t, "node-0")
	c.metrics.Store(m)

	// Two 1,000-byte frames: both fit a full 1,395-byte round alone
	// (with overhead), but not together.
	f1 := make([]byte, 1000)
	f2 := make([]byte, 1000)
	c.QueueBroadcast(f1)
	c.QueueBroadcast(f2)

	msgs := c.GetBroadcasts(3, 1395)
	require.Len(t, msgs, 1, "first frame fills the round")

	// Shrinking round (memberlist traffic) still must not drop it.
	msgs = c.GetBroadcasts(3, 1395)
	require.Len(t, msgs, 1, "second frame delivers next round")
	require.Empty(t, c.gossipQueue)
	require.Equal(t, 0.0, gossipOversizedDrops(t, reg))
}
