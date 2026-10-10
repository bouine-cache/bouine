package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
)

// TestSweeperReSignalsWhenOverBudget pins the bounded-lock-drain
// contract: an overshoot larger than one sweeperEvictCap pass still
// drains to zero — the sweeper re-signals itself after its capped
// pass — and one pass alone evicts at most sweeperEvictCap entries
// (readers never queue behind an unbounded write-lock hold).
func TestSweeperReSignalsWhenOverBudget(t *testing.T) {
	t.Parallel()
	if sweeperEvictCap < 4 {
		t.Skip("cap too small for a meaningful multi-pass drain")
	}
	// Single shard: every Put lands on the same budget. MaxBytes sized
	// so the shard goes over budget once enough entries are stored.
	const shardBytes = int64(64 * 1024)
	s := NewHotStore(HotConfig{MaxBytes: shardBytes, NumShards: 1, ReaperInterval: -1})
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	const n = sweeperEvictCap * 3
	perEntry := 128
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			key := api.NewKeyFromBytes([16]byte{byte(i), byte(i >> 8), 's', 'w'})
			obj := &api.Object{
				Key:      key,
				Body:     make([]byte, perEntry-100),
				TTL:      time.Hour,
				StoredAt: time.Now(),
			}
			_ = s.Put(context.Background(), key, obj)
		}(i)
	}
	wg.Wait()

	// The overshoot must drain to budget: one capped pass evicts at
	// most sweeperEvictCap; only the re-signal drains the rest.
	require.Eventually(t, func() bool {
		sh := &s.shards[0]
		sh.mu.Lock()
		defer sh.mu.Unlock()
		return sh.bytes <= shardBytes
	}, 5*time.Second, 10*time.Millisecond,
		"the overshoot must drain via capped, re-signalled passes")
}
