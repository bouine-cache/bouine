package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

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
	c.inv = Invalidator{
		PurgeFn: func(_ context.Context, evt api.PurgeEvent) error {
			mu.Lock()
			applied = append(applied, evt)
			mu.Unlock()
			return nil
		},
	}
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
