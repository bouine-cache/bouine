package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage/wal"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
)

// TestRace_HitsVsWarmEncode is the race regression for issue #218.
//
// The racing pair: hot.Get's slow path increments the STORED object's
// Hits under the shard write lock, while the warm-sync cycle
// (TieredStore.writeHotOnlyToWarm → encodeObject) reads obj.Hits from
// the same stored pointer with NO lock held. The fix makes the
// increment atomic.AddUint64 and pairs it with atomic.LoadUint64 in
// the encoder; under -race the pre-fix plain read/write fails here.
//
// The test drives both sides of the pair on the live stored object:
//   - the increment side via hot.Get slow path (the SIEVE visited bit
//     is cleared directly — same package — because Put marks entries
//     visited on insert (#484), so the first Get would never increment),
//   - the encode side via writeHotOnlyToWarm, exactly as the warm-sync
//     cycle invokes it, on a key whose warm backup was removed so the
//     key is hot-only and actually gets encoded.
func TestRace_HitsVsWarmEncode(t *testing.T) {
	t.Parallel()
	ts := newTieredStoreWithDir(t, t.TempDir())
	defer func() { _ = ts.Close(context.Background()) }()

	key := testkey.Hash([]byte("race-hits"))
	require.NoError(t, ts.Put(context.Background(), key, bigObj(key, 2048)))

	// Remove the warm backup so the key becomes hot-only and the
	// warm-sync cycle's writeHotOnlyToWarm actually encodes it
	// (otherwise Put's own backup marks the key as backed and the
	// encode hammer below is skipped).
	wSeg, wErr := ts.warm.Delete(key)
	require.NoError(t, wErr, "warm copy must exist after Put above BodyThreshold")
	_ = wSeg

	// Clear the SIEVE visited bit so the next Get takes the slow path
	// (the increment site).
	clearVisited := func() {
		shard := ts.hot.shard(key)
		shard.mu.Lock()
		defer shard.mu.Unlock()
		if e := shard.entries[key]; e != nil {
			e.entry.ClearVisited()
		}
	}
	clearVisited()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var increments atomic.Uint64

	// Get hammer: drives the slow-path increment; re-clears the visited
	// bit after every Get so the next Get re-enters the increment.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				got, _, err := ts.hot.Get(context.Background(), key)
				if err == nil && got != nil && got.Hits > 0 {
					increments.Add(1)
				}
				clearVisited()
			}
		}
	}()

	// Warm-encode hammer: the reader side, verbatim from the warm-sync
	// cycle.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				hotOnly := ts.collectHotOnlyKeys()
				var walEntries []wal.Entry
				_, _, _ = ts.writeHotOnlyToWarm(context.Background(), hotOnly, &walEntries)
			}
		}
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if increments.Load() > 10 {
			break
		}
	}
	close(stop)
	wg.Wait()

	require.Greater(t, increments.Load(), uint64(0),
		"the hammer must exercise the Hits increment; the race pair is not in play")
}
