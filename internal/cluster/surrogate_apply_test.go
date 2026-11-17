package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/api"
)

// newSurrogateTestCluster builds a Cluster with the apply loop running,
// mirroring New's wiring without memberlist.
func newSurrogateTestCluster(t *testing.T) *Cluster {
	t.Helper()
	c := &Cluster{
		cfg:     Config{NodeName: "node-0", Mode: "strong", GossipApplyTimeout: 5 * time.Second},
		peers:   make(map[string]*Member),
		ring:    newRing(256),
		seqs:    newSeqTracker(),
		done:    make(chan struct{}),
		logger:  observability.NoopLogger{},
		applyCh: make(chan api.RefreshSurrogateEvent, gossipSurrogateApplyQueue),
		applyWg: &sync.WaitGroup{},
	}
	c.metrics.Store(&Metrics{})
	c.startApplyLoop()
	t.Cleanup(func() {
		c.closeOnce.Do(func() { close(c.done) })
		c.applyWg.Wait()
	})
	return c
}

func gossipSurrogateFrame(t *testing.T, issuer string, seq uint64, tag string) []byte {
	t.Helper()
	buf, err := EncodeRefreshSurrogateGossip(api.RefreshSurrogateEvent{
		SurrogateKey: tag,
		Issuer:       issuer,
		IssuedAt:     time.Now(),
		Seq:          seq,
	})
	require.NoError(t, err)
	return buf
}

func TestGossipRefreshSurrogate_DedupViaHTTPThenGossip(t *testing.T) {
	t.Parallel()
	c := newSurrogateTestCluster(t)
	var applied atomic.Int32
	c.inv.Store(&Invalidator{
		RefreshSurrogateFn: func(ctx context.Context, evt api.RefreshSurrogateEvent) error {
			applied.Add(1)
			return nil
		},
	})

	// Simulate the HTTP delivery recording the seq (peer handler path).
	require.False(t, c.SeenFromPeer("node-a", 7))
	// The gossip copy must be deduped in the packet handler.
	c.handleGossipRefreshSurrogate(gossipSurrogateFrame(t, "node-a", 7, "tag-1"))
	// And a fresh gossip seq must be applied exactly once.
	c.handleGossipRefreshSurrogate(gossipSurrogateFrame(t, "node-a", 8, "tag-2"))
	deadline := time.Now().Add(2 * time.Second)
	for applied.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	require.Equal(t, int32(1), applied.Load(), "only the unseen seq is applied")
}

func TestGossipRefreshSurrogate_QueueFullDropIsRedeliverable(t *testing.T) {
	t.Parallel()
	c := newSurrogateTestCluster(t)
	// Block the apply worker so the queue fills.
	release := make(chan struct{})
	var applied atomic.Int32
	c.inv.Store(&Invalidator{
		RefreshSurrogateFn: func(ctx context.Context, evt api.RefreshSurrogateEvent) error {
			applied.Add(1)
			<-release
			return nil
		},
	})

	// Fill the bounded queue with the first event occupying the worker.
	c.handleGossipRefreshSurrogate(gossipSurrogateFrame(t, "node-b", 1, "fill"))
	deadline := time.Now().Add(2 * time.Second)
	for applied.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	for i := uint64(2); i <= gossipSurrogateApplyQueue+1; i++ {
		c.handleGossipRefreshSurrogate(gossipSurrogateFrame(t, "node-b", i, "fill"))
	}
	// This one overflows and is dropped.
	c.handleGossipRefreshSurrogate(gossipSurrogateFrame(t, "node-b", 999, "dropped"))
	// While the worker is blocked, queued events have NOT been recorded
	// yet (recording happens in the apply loop): the dropped seq and the
	// queued seqs must all be unseen, so a redelivery of any of them is
	// accepted by the packet-handler dedup check.
	require.False(t, c.peekFromPeer("node-b", 999), "dropped event seq must stay unseen")
	require.False(t, c.peekFromPeer("node-b", 2), "queued event seq is recorded only once applied")
	close(release)

	// After the worker drains, the applied seqs are recorded and peek
	// reports them, while the dropped seq 999 was never applied and so
	// stays unseen for a redelivery.
	drainDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(drainDeadline) {
		if c.peekFromPeer("node-b", 2) && c.peekFromPeer("node-b", gossipSurrogateApplyQueue+1) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.True(t, c.peekFromPeer("node-b", 2), "applied seq must be recorded")
	require.False(t, c.peekFromPeer("node-b", 999), "dropped seq must remain redeliverable")
}

func TestGossipRefreshSurrogate_InvalidTagDropped(t *testing.T) {
	t.Parallel()
	c := newSurrogateTestCluster(t)
	var applied atomic.Int32
	c.inv.Store(&Invalidator{
		RefreshSurrogateFn: func(ctx context.Context, evt api.RefreshSurrogateEvent) error {
			applied.Add(1)
			return nil
		},
	})
	for _, tag := range []string{"", "has space", "has,comma", string(make([]byte, 300))} {
		c.handleGossipRefreshSurrogate(gossipSurrogateFrame(t, "node-c", 1, tag))
	}
	// Give the apply loop a chance to (not) run.
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, int32(0), applied.Load())
}

func TestSeqTracker_PeekDoesNotRecord(t *testing.T) {
	t.Parallel()
	tr := newSeqTracker()
	require.False(t, tr.peek("a", 5))
	require.False(t, tr.seen("a", 5), "first seen records and accepts")
	require.True(t, tr.peek("a", 5), "peek sees the recorded seq")
	require.True(t, tr.seen("a", 5), "seen dedups")
	require.False(t, tr.peek("a", 6), "unseen seq not reported")
	require.False(t, tr.peek("", 1), "empty issuer never deduped")
}
