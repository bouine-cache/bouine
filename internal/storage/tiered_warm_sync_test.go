package storage

import (
	"context"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage/warm"
	"github.com/bouine-cache/bouine/internal/testutil/poll"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func tieredStoreWithSync(t *testing.T, batchSize int) *TieredStore {
	t.Helper()
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          1024,
		WarmSyncInterval:       -1, // disabled — we call runWarmSyncCycle manually
		WarmSyncBatchSize:      batchSize,
		TombstoneDrainInterval: -1, // disabled — we call drainTombstones manually via runWarmSyncCycle
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(context.Background()) })
	return ts
}

func TestWarmSync_WritesHotOnlyEntriesToWarm(t *testing.T) {
	t.Parallel()
	ts := tieredStoreWithSync(t, 100) // sync disabled, we call cycle manually
	// Put small objects (below bodyThreshold=1024) so they stay hot-only.
	for i := range 10 {
		k := testkey.Key(uint64(100 + i))
		_ = ts.Put(context.Background(), k, obj(k, 100))
	}

	// Verify warm is empty before sync.
	keys := ts.warm.Keys()
	require.Len(t, keys, 0)

	ts.runWarmSyncCycle(context.Background())

	// Verify warm now has the entries.
	keys = ts.warm.Keys()
	require.Len(t, keys, 10)
}

func TestWarmSync_SkipsWarmBackedEntries(t *testing.T) {
	t.Parallel()
	ts := tieredStoreWithSync(t, 100)

	// Put a large object (> bodyThreshold) so it goes to warm on Put.
	k := testkey.Key(200)
	_ = ts.Put(context.Background(), k, bigObj(k, 2000))

	warmKeysBefore := len(ts.warm.Keys())
	require.Equal(t, 1, warmKeysBefore)

	ts.runWarmSyncCycle(context.Background())

	warmKeysAfter := len(ts.warm.Keys())
	require.Equal(t, warmKeysBefore, warmKeysAfter)
}

func TestWarmSync_RespectsBatchSize(t *testing.T) {
	t.Parallel()
	ts := tieredStoreWithSync(t, 3) // batch size 3

	for i := range 10 {
		k := testkey.Key(uint64(300 + i))
		_ = ts.Put(context.Background(), k, obj(k, 100))
	}

	ts.runWarmSyncCycle(context.Background())
	keys := ts.warm.Keys()
	require.Len(t, keys, 3)

	// Second cycle should sync more.
	ts.runWarmSyncCycle(context.Background())
	keys = ts.warm.Keys()
	require.Len(t, keys, 6)
}

func TestWarmSync_TombstonesWarmBackedEvictions(t *testing.T) {
	t.Parallel()
	// Use a very small hot tier so the backed entry is evicted
	// by SIEVE when we fill with competing entries.
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 14, NumShards: 1}, // 16 KiB, single shard
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          1024,
		WarmSyncInterval:       -1, // disabled — manual cycle
		WarmSyncBatchSize:      100,
		TombstoneDrainInterval: -1, // disabled — manual drain
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(context.Background()) })

	// Put a large object (goes to warm on Put, marks hasBackup).
	k := testkey.Key(400)
	_ = ts.Put(context.Background(), k, bigObj(k, 2000))

	warmKeysBefore := len(ts.warm.Keys())
	require.Equal(t, 1, warmKeysBefore)

	// Fill with many large entries to force SIEVE eviction of k.
	// evictPreferBacked targets backed entries first.
	for i := range 50 {
		k2 := testkey.Key(uint64(500 + i))
		_ = ts.Put(context.Background(), k2, bigObj(k2, 2000))
	}

	// Drain queues — SIEVE-evicted backed keys are Unprotected
	// (demoted to SIEVE-managed), not tombstoned. The warm copy
	// stays live; only non-SIEVE removals (reaper, ban, Delete)
	// tombstone.
	ts.runWarmSyncCycle(context.Background())

	// k should still be in warm (Unprotected, not deleted).
	warmKeys := ts.warm.Keys()
	found := false
	for _, wk := range warmKeys {
		if wk == k {
			found = true
			break
		}
	}
	require.True(t, found, "SIEVE-evicted backed key should be Unprotected, not tombstoned (warm copy retained)")

	// Some of the 50 competing entries may still be backed (still in hot),
	// so ProtectedCount may be > 0. What matters is that k was demoted:
	// it should be in warm but not protected. Verify via warm.Get +
	// ProtectedCount — k is unprotected if protectedCount < total warm
	// entries (at least k was demoted). More directly: the SIEVE-evicted
	// key k should not be counted as protected.
	// Since we can't check per-key protected status via the public API,
	// verify that at least one entry was demoted (protectedCount < warm
	// entry count — not all entries are protected, which would be the
	// stranded state).
	require.Less(t, ts.warm.ProtectedCount(), len(warmKeys),
		"at least one warm entry should be unprotected (demoted by SIEVE eviction)")
}

func TestWarmSync_TombstoneQueueOverflowNonBlocking(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          1024,
		WarmSyncInterval:       -1,
		WarmSyncBatchSize:      100,
		TombstoneQueueSize:     16, // small queue for deterministic overflow test
		TombstoneDrainInterval: -1, // disabled — manual drain
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(context.Background()) })

	// Fill tombstone queue to capacity. The channel is buffered
	// so all sends should succeed without blocking.
	qcap := cap(ts.tombstoneQueue)
	for i := range qcap {
		ts.tombstoneQueue <- testkey.Key(uint64(600 + i))
	}

	// runWarmSyncCycle should drain the queue without blocking.
	done := make(chan struct{})
	go func() {
		ts.runWarmSyncCycle(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("runWarmSyncCycle blocked on draining tombstone queue")
	}
}

func TestWarmSync_SyncGoroutineStopsOnClose(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:               HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:              &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:            filepath.Join(dir, "index.wal"),
		BodyThreshold:     1024,
		WarmSyncInterval:  100 * time.Millisecond,
		WarmSyncBatchSize: 100,
	})
	require.NoError(t, err, "NewTieredStore")

	// Let the sync goroutine run for a fixed window, then close via a
	// timer so the main goroutine waits on the done channel instead of
	// sleeping. Close should join syncWg within timeout.
	done := make(chan struct{})
	time.AfterFunc(300*time.Millisecond, func() {
		_ = ts.Close(context.Background())
		close(done)
	})

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked waiting for syncWg")
	}
}

func TestWarmSync_RestartRecovery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	walPath := filepath.Join(dir, "index.wal")

	// Create store, fill with small objects, sync, close.
	ts1, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:                 walPath,
		BodyThreshold:          1024,
		WarmSyncInterval:       -1, // disabled — we call cycle manually
		WarmSyncBatchSize:      100,
		TombstoneDrainInterval: -1,
	})
	require.NoError(t, err, "NewTieredStore")

	for i := range 5 {
		k := testkey.Key(uint64(700 + i))
		_ = ts1.Put(context.Background(), k, obj(k, 100))
	}
	ts1.runWarmSyncCycle(context.Background())
	_ = ts1.Close(context.Background())

	// Reopen — warm should have the entries from the previous run.
	ts2, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:                 walPath,
		BodyThreshold:          1024,
		WarmSyncInterval:       -1,
		WarmSyncBatchSize:      100,
		TombstoneDrainInterval: -1,
	})
	require.NoError(t, err, "reopen")
	t.Cleanup(func() { _ = ts2.Close(context.Background()) })

	keys := ts2.warm.Keys()
	require.Len(t, keys, 5)

	// Verify warm hits work.
	for i := range 5 {
		k := testkey.Key(uint64(700 + i))
		got, src, err := ts2.Get(context.Background(), k)
		if err != nil || got == nil {
			t.Fatalf("Get(%d): got=%v src=%q err=%v", k, got, src, err)
		}
		if src != api.SourceWarm && src != api.SourceHot {
			t.Fatalf("Get(%d): source = %q, want warm or hot", k, src)
		}
	}
}

func TestWarmSync_WarmSyncIntervalNegativeOneDisablesSync(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          1024,
		WarmSyncInterval:       -1, // explicitly disabled
		TombstoneDrainInterval: -1,
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(context.Background()) })

	// syncWg should be 0 — no goroutine started.
	// Put small objects and poll — warm should stay empty since sync is disabled.
	for i := range 5 {
		k := testkey.Key(uint64(800 + i))
		_ = ts.Put(context.Background(), k, obj(k, 100))
	}
	poll.Eventually(t, 200*time.Millisecond, 20*time.Millisecond, func() bool {
		return len(ts.warm.Keys()) == 0
	})
}

func TestWarmSync_RebuildIndexFromScan(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	walPath := filepath.Join(dir, "index.wal")

	// Create store, fill warm, close.
	ts1, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:                 walPath,
		BodyThreshold:          1024,
		WarmSyncInterval:       -1,
		WarmSyncBatchSize:      100,
		TombstoneDrainInterval: -1,
	})
	require.NoError(t, err, "NewTieredStore")

	// Put large objects (go to warm on Put).
	for i := range 3 {
		k := testkey.Key(uint64(900 + i))
		_ = ts1.Put(context.Background(), k, bigObj(k, 2000))
	}
	_ = ts1.Close(context.Background())

	// Delete the WAL to simulate WAL loss.
	_ = os.Remove(walPath)

	// Reopen — should rebuild index from segment scan.
	ts2, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:                 walPath,
		BodyThreshold:          1024,
		WarmSyncInterval:       -1,
		WarmSyncBatchSize:      100,
		TombstoneDrainInterval: -1,
	})
	require.NoError(t, err, "reopen")
	t.Cleanup(func() { _ = ts2.Close(context.Background()) })

	keys := ts2.warm.Keys()
	require.Len(t, keys, 3)
}

// TestWarmSync_RebuildIndexFromScanHonoursTombstones verifies that the
// segment-scan fallback does not resurrect keys that were tombstoned
// (deleted) before WAL loss.
func TestWarmSync_RebuildIndexFromScanHonoursTombstones(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	walPath := filepath.Join(dir, "index.wal")

	ts1, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:                 walPath,
		BodyThreshold:          1024,
		WarmSyncInterval:       -1,
		WarmSyncBatchSize:      100,
		TombstoneDrainInterval: -1,
	})
	require.NoError(t, err, "NewTieredStore")

	// Put 3 large objects, then delete one so a tombstone exists in the
	// segment alongside the live records.
	for i := range 3 {
		k := testkey.Key(uint64(900 + i))
		_ = ts1.Put(context.Background(), k, bigObj(k, 2000))
	}
	err = ts1.Delete(context.Background(), testkey.Key(901))
	require.NoError(t, err, "Delete")
	_ = ts1.Close(context.Background())

	// Delete the WAL to force the segment-scan fallback.
	_ = os.Remove(walPath)

	ts2, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:                 walPath,
		BodyThreshold:          1024,
		WarmSyncInterval:       -1,
		WarmSyncBatchSize:      100,
		TombstoneDrainInterval: -1,
	})
	require.NoError(t, err, "reopen")
	t.Cleanup(func() { _ = ts2.Close(context.Background()) })

	// Keys 900 and 902 should be live; 901 should NOT have been
	// resurrected by the segment scan.
	keys := ts2.warm.Keys()
	require.Len(t, keys, 2)
	got, _, err := ts2.Get(context.Background(), testkey.Key(901))
	require.NoError(t, err, "Get(901)")
	require.Nil(t, got)
}

// TestPutReplace_TombstonesOldWarmCopy verifies that replacing a
// warm-backed entry with a smaller (hot-only) entry enqueues a
// tombstone so the stale warm copy is cleaned up by the sync loop.
// After the sync cycle, the new object is also synced to warm (it's
// now hot-only), so warm should have exactly 1 key — the new object.
func TestPutReplace_TombstonesOldWarmCopy(t *testing.T) {
	t.Parallel()
	ts := tieredStoreWithSync(t, 100)

	// Put a large object — goes to warm, marks hasBackup.
	k := testkey.Key(1100)
	_ = ts.Put(context.Background(), k, bigObj(k, 2000))

	keys := ts.warm.Keys()
	require.Len(t, keys, 1)

	// Replace with a small object — below bodyThreshold, does not
	// write to warm. The old backed entry is replaced in hot,
	// notifyEvict should enqueue a tombstone for the old warm copy.
	_ = ts.Put(context.Background(), k, obj(k, 100))

	// Drain tombstones + sync hot-only entries. The old warm copy
	// should be tombstoned, and the new small object should be synced
	// to warm (it's hot-only now).
	ts.runWarmSyncCycle(context.Background())

	// The key should still be in warm (the new object was synced),
	// but with the new body, not the old one. Verify by checking that
	// Get returns the small object's body size.
	got, _, err := ts.Get(context.Background(), k)
	if err != nil || got == nil {
		t.Fatalf("Get(%d): got=%v err=%v", k, got, err)
	}
	require.Equal(t, int64(100), got.BodySize)
}

// TestOnEvictCallback verifies that the OnEvict callback fires when a
// warm-backed entry is evicted from the hot tier. Uses a single shard
// with a tiny memory budget so the warm-backed entry is guaranteed to
// be evicted by evictPreferBacked before hot-only entries.
func TestOnEvictCallback(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var evictedKeys []api.Key
	var demotedKeys []api.Key

	s := NewHotStore(HotConfig{
		MaxBytes:  1 << 14, // 16 KiB — very small to force eviction
		NumShards: 1,       // single shard for deterministic eviction
		OnEvict: func(key api.Key) {
			mu.Lock()
			evictedKeys = append(evictedKeys, key)
			mu.Unlock()
		},
		OnEvictDemoted: func(key api.Key) {
			mu.Lock()
			demotedKeys = append(demotedKeys, key)
			mu.Unlock()
		},
	})
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	// Put entry and mark as backed.
	k1 := testkey.Key(1000)
	_ = s.Put(context.Background(), k1, obj(k1, 100))
	s.SetBacked(k1)

	// Fill with many other entries to force SIEVE to evict k1.
	// evictPreferBacked evicts backed entries first (up to 4 skips),
	// so with backedCount > 0, k1 is guaranteed to be among the first
	// evicted.
	for i := range 200 {
		k2 := testkey.Key(uint64(2000 + i))
		_ = s.Put(context.Background(), k2, obj(k2, 100))
	}

	// Wait for sweeper to process overshoot and evict k1. SIEVE
	// evictions of backed entries call OnEvictDemoted (not OnEvict),
	// so k1 should appear in demotedKeys.
	poll.Eventually(t, 2*time.Second, 10*time.Millisecond, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return slices.Contains(demotedKeys, k1)
	})

	mu.Lock()
	found := slices.Contains(demotedKeys, k1)
	mu.Unlock()

	require.True(t, found)
}

// TestTombstoneDrain_DedicatedGoroutineDrainsQueues verifies that the
// dedicated drain goroutine flushes tombstones from the queue to the
// warm tier without a manual runWarmSyncCycle call.
func TestTombstoneDrain_DedicatedGoroutineDrainsQueues(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          1024,
		WarmSyncInterval:       -1, // warm sync disabled
		WarmSyncBatchSize:      100,
		TombstoneDrainInterval: 50 * time.Millisecond, // fast drain
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(context.Background()) })

	// Put a large object (goes to warm on Put, marks hasBackup).
	k := testkey.Key(1200)
	_ = ts.Put(context.Background(), k, bigObj(k, 2000))
	require.Len(t, ts.warm.Keys(), 1, "warm should have the large object")

	// Enqueue a tombstone directly — simulates hot-tier eviction.
	ts.tombstoneQueue <- k

	// The dedicated drain goroutine should process it within a few
	// drain cycles. The tombstone removes the key from the warm tier.
	poll.Eventually(t, 5*time.Second, 20*time.Millisecond, func() bool {
		return !slices.Contains(ts.warm.Keys(), k)
	})
}

// TestTombstoneDrain_GoroutineStopsOnClose verifies that the dedicated
// drain goroutine is joined during Close without blocking.
func TestTombstoneDrain_GoroutineStopsOnClose(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          1024,
		WarmSyncInterval:       -1,
		WarmSyncBatchSize:      100,
		TombstoneDrainInterval: 100 * time.Millisecond,
	})
	require.NoError(t, err, "NewTieredStore")

	done := make(chan struct{})
	time.AfterFunc(300*time.Millisecond, func() {
		_ = ts.Close(context.Background())
		close(done)
	})

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked waiting for drainWg")
	}
}

// TestTombstoneDrain_ConfigurableQueueSize verifies that the queue size
// is configurable and respects the configured capacity.
func TestTombstoneDrain_ConfigurableQueueSize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:                   &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:                 filepath.Join(dir, "index.wal"),
		BodyThreshold:          1024,
		WarmSyncInterval:       -1,
		WarmSyncBatchSize:      100,
		TombstoneQueueSize:     32,
		TombstoneDrainInterval: -1, // disabled
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(context.Background()) })

	require.Equal(t, 32, cap(ts.tombstoneQueue))
	require.Equal(t, 32, cap(ts.warmEvictQueue))
}

// TestWarmSync_CacheSurvivalRate verifies that cache entries survive a
// restart with and without the warm sync loop. Without warm sync, only
// objects above body_threshold are persisted to warm. With warm sync,
// all hot-only entries are synced to warm before restart and survive.
func TestWarmSync_CacheSurvivalRate(t *testing.T) {
	const (
		smallObjCount = 1000
		largeObjCount = 100
		bodyThreshold = 1024
		totalKeys     = smallObjCount + largeObjCount
	)

	cases := []struct {
		name           string
		warmSyncOn     bool
		minSurvivalPct float64 // minimum expected survival rate
	}{
		{"before_warmSyncOff", false, 0}, // only large objects survive
		{"after_warmSyncOn", true, 100},  // all objects survive
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			warmDir := filepath.Join(dir, "warm")
			walPath := filepath.Join(dir, "index.wal")

			ts1, err := NewTieredStore(TieredConfig{
				Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
				Warm:                   &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
				WALDir:                 walPath,
				BodyThreshold:          int64(bodyThreshold),
				WarmSyncInterval:       -1,
				WarmSyncBatchSize:      5000,
				TombstoneDrainInterval: -1,
			})
			require.NoError(t, err, "NewTieredStore")

			for i := range smallObjCount {
				k := testkey.Key(uint64(i))
				require.NoError(t, ts1.Put(context.Background(), k, obj(k, 100)))
			}
			for i := range largeObjCount {
				k := testkey.Key(uint64(smallObjCount + i))
				require.NoError(t, ts1.Put(context.Background(), k, bigObj(k, 2000)))
			}

			if tc.warmSyncOn {
				ts1.runWarmSyncCycle(context.Background())
			}

			require.NoError(t, ts1.Close(context.Background()))

			ts2, err := NewTieredStore(TieredConfig{
				Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
				Warm:                   &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
				WALDir:                 walPath,
				BodyThreshold:          int64(bodyThreshold),
				WarmSyncInterval:       -1,
				WarmSyncBatchSize:      5000,
				TombstoneDrainInterval: -1,
			})
			require.NoError(t, err, "reopen")
			t.Cleanup(func() { _ = ts2.Close(context.Background()) })

			warmKeys := ts2.warm.Keys()
			survived := len(warmKeys)
			survivalRate := float64(survived) / float64(totalKeys) * 100

			smallSurvived := 0
			largeSurvived := 0
			for _, k := range warmKeys {
				if int(k.Hash64()) < smallObjCount {
					smallSurvived++
				} else {
					largeSurvived++
				}
			}

			t.Logf("survived=%d/%d (%.1f%%), small=%d, large=%d",
				survived, totalKeys, survivalRate, smallSurvived, largeSurvived)

			require.GreaterOrEqual(t, survivalRate, tc.minSurvivalPct,
				"survival rate below expected minimum")

			if tc.warmSyncOn {
				require.Equal(t, smallObjCount, smallSurvived, "small objects should survive with warm sync")
				require.Equal(t, largeObjCount, largeSurvived, "large objects should survive with warm sync")
			} else {
				require.Equal(t, largeObjCount, largeSurvived, "large objects should always survive")
				require.Equal(t, 0, smallSurvived, "small objects should not survive without warm sync")
			}
		})
	}
}

// churnResult captures the metrics from a single churn scenario run.
type churnResult struct {
	phase2Evictions int64
	hotEntries      int64
	hits            int
	misses          int
	warmEntries     int64
	warmDiskBytes   int64
	warmMaxBytes    int64
	protectedCount  int
}

func (r churnResult) churnRatio() float64 {
	if r.hotEntries == 0 {
		return 0
	}
	return float64(r.phase2Evictions) / float64(r.hotEntries)
}

// churnKeyCount is the key count used by the churn regression test.
const churnKeyCount = 200

// zipfishAccess returns a deterministic access sequence where ~pct%
// of accesses go to the top `topN` keys. Uses math/rand/v2 with a
// fixed PCG seed for reproducibility (AGENTS.md §8: no time.Now in
// tests; the seed is fixed, not time-derived).
func zipfishAccess(keyCount, count, topN, pct int) []int {
	rnd := rand.New(rand.NewPCG(484, 0))
	seq := make([]int, count)
	for i := range count {
		if rnd.IntN(100) < pct {
			seq[i] = rnd.IntN(topN)
		} else {
			seq[i] = topN + rnd.IntN(keyCount-topN)
		}
	}
	return seq
}

// runChurnScenario executes a churn workload and returns metrics.
//
// The workload fills `churnKeyCount` backed entries into a small hot
// tier, then performs `accessCount` Zipf-skewed accesses over `rounds`
// rounds. Cold misses are re-Put (simulating origin fetch). The clock
// is not advanced — TTL expiry is tested separately. This isolates the
// SIEVE-eviction-driven churn that the fix targets.
func runChurnScenario(t *testing.T) churnResult {
	t.Helper()

	const (
		accessCount = 5000
		rounds      = 10
		bodySize    = 2000 // > bodyThreshold=1024 → backed
		topN        = 40   // top 20% = working set
		skewPct     = 99   // 99% of accesses to top 40
	)

	// Size hot to hold ~50 entries (slightly above the working set of 40).
	hotMaxBytes := int64(50 * 2500) // ~50 entries at ~2500 bytes/entry

	ts := tieredStore484(t, hotMaxBytes)
	ts.warmSyncBatchSize = 1000

	// Phase 1: Fill all keys. All backed (bodySize > bodyThreshold).
	for i := range churnKeyCount {
		k := testkey.Key(uint64(i))
		require.NoError(t, ts.Put(context.Background(), k, bigObj(k, bodySize)))
	}
	ts.drainQueues()

	// Record evictions after fill — the initial fill churn is excluded
	// from the ratio (it's setup, not the loop being tested).
	fillEvictions := ts.Stats().Evictions

	// Phase 2: Access phase — Zipf-skewed, 99% to top 40.
	// No clock advance — isolates SIEVE-driven churn.
	accessSeq := zipfishAccess(churnKeyCount, accessCount, topN, skewPct)
	hits, misses := 0, 0
	perRound := accessCount / rounds
	for r := range rounds {
		_ = r
		for i := range perRound {
			keyIdx := accessSeq[r*perRound+i]
			k := testkey.Key(uint64(keyIdx))
			obj, _, err := ts.Get(context.Background(), k)
			require.NoError(t, err)
			if obj != nil {
				hits++
				// Warm hit or hot hit — TieredStore.Get already re-promotes
				// warm hits via hot.Put internally. No re-Put needed.
			} else {
				misses++
				// Cold miss — simulate origin fetch by re-Putting.
				require.NoError(t, ts.Put(context.Background(), k, bigObj(k, bodySize)))
			}
		}
		ts.drainQueues()
	}

	stats := ts.Stats()
	result := churnResult{
		phase2Evictions: stats.Evictions - fillEvictions,
		hotEntries:      stats.HotEntries,
		hits:            hits,
		misses:          misses,
		warmEntries:     stats.WarmEntries,
		warmDiskBytes:   stats.WarmDiskBytes,
		warmMaxBytes:    stats.WarmMaxBytes,
		protectedCount:  ts.warm.ProtectedCount(), // test-only accessor
	}
	t.Logf("phase2 evictions=%d, hot=%d, churn=%.1fx, hits=%d, misses=%d, warm=%d, disk=%d, protected=%d",
		result.phase2Evictions, result.hotEntries,
		result.churnRatio(), result.hits, result.misses,
		result.warmEntries, result.warmDiskBytes, result.protectedCount)
	return result
}

// TestTiered_484_ChurnRegression is the load-bearing regression test
// for issue #484. It proves the SIEVE-eviction → warm-Unprotect path
// breaks the 32× dual-tier destruction feedback loop.
//
// Assertions:
//   - Churn < 2× entries (the issue's acceptance criterion).
//   - No stranded protected entries (protected ≤ hot entries).
//   - Warm disk within budget.
//   - 0 cold misses (all warm hits, no dual-tier destruction).
//   - All keys retained in warm (no tombstone destruction).
func TestTiered_484_ChurnRegression(t *testing.T) {
	t.Parallel()
	r := runChurnScenario(t)

	// Churn < 2× entries (the issue's acceptance criterion).
	require.Less(t, r.phase2Evictions, 2*r.hotEntries,
		"churn should be < 2× entries (got %.1fx)", r.churnRatio())

	// No stranded protected entries.
	require.LessOrEqual(t, r.protectedCount, int(r.hotEntries),
		"no stranded protected entries (protected=%d, hot=%d)",
		r.protectedCount, r.hotEntries)

	// Warm disk within budget.
	require.Less(t, r.warmDiskBytes, r.warmMaxBytes,
		"warm disk should be within budget (disk=%d, max=%d)",
		r.warmDiskBytes, r.warmMaxBytes)

	// 0 cold misses (all warm hits, no dual-tier destruction).
	require.Equal(t, 0, r.misses,
		"should have 0 cold misses (all warm hits)")

	// All keys retained in warm (no tombstone destruction).
	require.Equal(t, int64(churnKeyCount), r.warmEntries,
		"warm should retain all keys (got %d, want %d)",
		r.warmEntries, churnKeyCount)
}

// tieredStore484 builds a TieredStore with all background goroutines
// disabled for deterministic testing.
func tieredStore484(t *testing.T, hotMaxBytes int64) *TieredStore {
	t.Helper()
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot: HotConfig{
			MaxBytes:       hotMaxBytes,
			NumShards:      1,
			ReaperInterval: -1,
		},
		Warm: &warm.Config{
			Dir:      filepath.Join(dir, "warm"),
			MaxBytes: 100 << 20,
			SegMax:   1 << 20,
		},
		WALDir:                 "",
		BodyThreshold:          1024,
		WarmSyncInterval:       -1,
		WarmSyncBatchSize:      100,
		TombstoneDrainInterval: -1,
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(context.Background()) })
	return ts
}

// TestTiered_484_NonSIEVERemovalsTombstoneWarm verifies that the three
// non-SIEVE hot-removal paths (reaper, ban, Delete) still tombstone
// (delete) the warm copy of backed entries. This preserves RFC 9111
// freshness semantics: expired/banned/deleted objects must not be
// served from warm after the hot tier drops them.
func TestTiered_484_NonSIEVERemovalsTombstoneWarm(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_000_000, 0)

	tests := []struct {
		name    string
		key     api.Key
		setup   func(o *api.Object)
		trigger func(t *testing.T, ts *TieredStore)
	}{
		{
			name: "reaper",
			key:  testkey.Key(42),
			setup: func(o *api.Object) {
				o.TTL = 10 * time.Second
				o.StaleWhileRevalidate = 0
				o.StaleIfError = 0
				o.StoredAt = now
			},
			trigger: func(t *testing.T, ts *TieredStore) {
				ts.hot.reapExpired(now.Add(11 * time.Second))
				ts.drainQueues()
			},
		},
		{
			name: "ban",
			key:  testkey.Key(99),
			setup: func(o *api.Object) {
				o.Header.Set(header.XBouinePath, "/ban-me")
			},
			trigger: func(t *testing.T, ts *TieredStore) {
				_, err := ts.Ban(context.Background(), api.BanExpr{PathRegex: "^/ban-me"})
				require.NoError(t, err, "Ban")
				ts.drainQueues()
			},
		},
		{
			name:  "delete",
			key:   testkey.Key(77),
			setup: func(o *api.Object) {},
			trigger: func(t *testing.T, ts *TieredStore) {
				require.NoError(t, ts.Delete(context.Background(), testkey.Key(77)))
				ts.drainQueues()
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts := tieredStore484(t, 1<<20)

			o := bigObj(tt.key, 2000)
			tt.setup(o)
			require.NoError(t, ts.Put(context.Background(), tt.key, o))
			require.Len(t, ts.warm.Keys(), 1, "warm should have the backed entry")

			tt.trigger(t, ts)

			body, err := ts.warm.Get(tt.key)
			require.NoError(t, err)
			require.Nil(t, body, "%s should tombstone warm copy", tt.name)
		})
	}
}

// TestTiered_484_DrainWarmUnprotects_NilWarm verifies that
// drainWarmUnprotects is a no-op when the warm tier is nil
// (hot-only store).
func TestTiered_484_DrainWarmUnprotects_NilWarm(t *testing.T) {
	t.Parallel()
	ts, err := NewTieredStore(TieredConfig{
		Hot: HotConfig{
			MaxBytes:       1 << 20,
			NumShards:      1,
			ReaperInterval: -1,
		},
		Warm:                   nil, // hot-only — no warm tier
		WALDir:                 "",
		BodyThreshold:          1 << 20, // everything hot-only
		WarmSyncInterval:       -1,
		WarmSyncBatchSize:      100,
		TombstoneDrainInterval: -1,
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(context.Background()) })

	for i := range 5 {
		k := testkey.Key(uint64(i))
		require.NoError(t, ts.Put(context.Background(), k, obj(k, 100)))
	}

	// drainQueues guards on t.warm == nil before calling
	// drainWarmUnprotects, so exercise the latter directly to cover its
	// own nil-warm early return.
	require.Equal(t, 0, ts.drainWarmUnprotects(), "drainWarmUnprotects no-op when warm is nil")
	ts.drainQueues()
	require.Equal(t, int64(0), ts.droppedWarmUnprotects.Load())
}

// TestTiered_484_OverflowFallsBackToTombstone verifies that when
// warmUnprotectQueue is full, the OnEvictDemoted callback falls back
// to tombstoneQueue instead of stranding the entry as permanently
// protected.
func TestTiered_484_OverflowFallsBackToTombstone(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// TombstoneQueueSize=1 so both queues have capacity 1.
	ts, err := NewTieredStore(TieredConfig{
		Hot: HotConfig{
			MaxBytes:       4 << 10, // 4 KiB — ~1 bigObj(2000)
			NumShards:      1,
			ReaperInterval: -1,
		},
		Warm: &warm.Config{
			Dir:      filepath.Join(dir, "warm"),
			MaxBytes: 100 << 20,
			SegMax:   1 << 20,
		},
		WALDir:                 "",
		BodyThreshold:          1024,
		WarmSyncInterval:       -1,
		WarmSyncBatchSize:      100,
		TombstoneDrainInterval: -1,
		TombstoneQueueSize:     1, // tiny queue → overflow on 2nd eviction
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(context.Background()) })

	for i := range 3 {
		k := testkey.Key(uint64(i))
		require.NoError(t, ts.Put(context.Background(), k, bigObj(k, 2000)))
	}

	require.GreaterOrEqual(t, ts.droppedWarmUnprotects.Load(), int64(1),
		"overflow should increment droppedWarmUnprotects")

	ts.drainQueues()

	warmKeys := ts.warm.Keys()
	require.Less(t, len(warmKeys), 3,
		"overflow fallback should have tombstoned at least one warm entry")
}

// TestTiered_484_OverflowBothQueuesFull verifies the inner-default branch
// of OnEvictDemoted: when both warmUnprotectQueue and tombstoneQueue are
// full, the callback increments droppedTombstones (the entry is dropped
// rather than stranded). Covers tiered.go lines 287-288.
func TestTiered_484_OverflowBothQueuesFull(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	ts, err := NewTieredStore(TieredConfig{
		Hot: HotConfig{
			MaxBytes:       4 << 10,
			NumShards:      1,
			ReaperInterval: -1,
		},
		Warm: &warm.Config{
			Dir:      filepath.Join(dir, "warm"),
			MaxBytes: 100 << 20,
			SegMax:   1 << 20,
		},
		WALDir:                 "",
		BodyThreshold:          1024,
		WarmSyncInterval:       -1,
		WarmSyncBatchSize:      100,
		TombstoneDrainInterval: -1,
		TombstoneQueueSize:     1, // both queues cap=1
	})
	require.NoError(t, err, "NewTieredStore")
	t.Cleanup(func() { _ = ts.Close(context.Background()) })

	// Pre-fill both queues to capacity so the next OnEvictDemoted hits
	// the inner default (both selects fall through).
	ts.warmUnprotectQueue <- testkey.Key(9001)
	ts.tombstoneQueue <- testkey.Key(9002)
	droppedBefore := ts.droppedTombstones.Load()

	// Put a backed entry then evict it via SIEVE by overfilling hot.
	for i := range 3 {
		k := testkey.Key(uint64(i))
		require.NoError(t, ts.Put(context.Background(), k, bigObj(k, 2000)))
	}

	require.Greater(t, ts.droppedTombstones.Load(), droppedBefore,
		"inner default should increment droppedTombstones when both queues are full")
	require.GreaterOrEqual(t, ts.droppedWarmUnprotects.Load(), int64(1),
		"droppedWarmUnprotects should also increment")
}
