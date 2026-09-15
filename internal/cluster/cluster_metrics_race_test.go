package cluster

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSetMetrics_ConcurrentReads is the race regression for issue #450:
// SetMetrics writes the metrics pointer while memberlist goroutines
// (gossip dispatch, reconcile loop) read it. Before the fix the field
// was a plain *Metrics; the -race detector flags the write/read pair
// when a read site runs concurrently with SetMetrics. The production
// sequence is SetMetrics racing memberlist's own goroutines, which this
// test simulates with direct readers of every read site the issue
// lists: Owner (IncRingEmpty), the gossip handlers
// (IncGossipInvalidation), and QueueBroadcast/GetBroadcasts.
func TestSetMetrics_ConcurrentReads(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	readers := []func(){
		func() { c.metrics.Load().IncRingEmpty() },
		func() { c.metrics.Load().IncGossipInvalidation("purge") },
		func() { c.metrics.Load().IncGossipInvalidation("ban") },
		func() { c.metrics.Load().IncGossipInvalidation("refresh") },
		func() { c.metrics.Load().IncGossipInvalidation("purge_batch") },
		func() { c.metrics.Load().IncGossipInvalidation("refresh_batch") },
		func() { c.metrics.Load().IncBroadcastOverflow() },
	}
	for _, r := range readers {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					r()
				}
			}
		}()
	}

	// SetMetrics concurrently with the readers: before the atomic
	// conversion this raced on the plain pointer field.
	for range 100 {
		c.SetMetrics(&Metrics{})
	}
	close(stop)
	wg.Wait()
}

// TestSetMetrics_NilThenReal verifies the documented ordering contract:
// reads before SetMetrics must not panic (typed nil via Load) and
// reads after it must observe the registered metrics.
func TestSetMetrics_NilThenReal(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	// Before SetMetrics: metrics.Load() is nil; the Metrics methods
	// are nil-receiver-safe (issue #450's suggested fix relies on it).
	require.NotPanics(t, func() {
		c.metrics.Load().IncRingEmpty()
		c.metrics.Load().IncGossipInvalidation("purge")
	})

	m := RegisterMetrics(nil)
	c.SetMetrics(m)
	require.NotPanics(t, func() {
		c.metrics.Load().IncRingEmpty()
	})
	require.Equal(t, m, c.metrics.Load())
}
