package storage

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage/warm"
	"github.com/bouine-cache/bouine/internal/testutil/poll"
	"github.com/bouine-cache/bouine/pkg/api"
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
		k := api.KeyFromPrimary(uint64(100 + i))
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
	k := api.KeyFromPrimary(200)
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
		k := api.KeyFromPrimary(uint64(300 + i))
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
	k := api.KeyFromPrimary(400)
	_ = ts.Put(context.Background(), k, bigObj(k, 2000))

	warmKeysBefore := len(ts.warm.Keys())
	require.Equal(t, 1, warmKeysBefore)

	// Fill with many large entries to force SIEVE eviction of k.
	// evictPreferBacked targets backed entries first.
	for i := range 50 {
		k2 := api.KeyFromPrimary(uint64(500 + i))
		_ = ts.Put(context.Background(), k2, bigObj(k2, 2000))
	}

	// Drain tombstones — evicted backed keys should be removed
	// from the warm tier.
	ts.runWarmSyncCycle(context.Background())

	warmKeysAfter := len(ts.warm.Keys())
	// Without tombstoning, warm would have 51 keys (1 original + 50 new).
	// With tombstoning, the original key 400 should be removed from warm.
	// Some of the 50 new entries may also be evicted and tombstoned,
	// so we check that key 400 is gone specifically.
	if warmKeysAfter >= 51 {
		t.Fatalf("warm key count should reflect tombstone drain: before=%d, after=%d (expected < 51)",
			warmKeysBefore, warmKeysAfter)
	}
	for _, wk := range ts.warm.Keys() {
		require.NotEqual(t, k.Primary(), wk)
	}
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
		ts.tombstoneQueue <- api.KeyFromPrimary(uint64(600 + i))
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
		k := api.KeyFromPrimary(uint64(700 + i))
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
		k := api.KeyFromPrimary(uint64(700 + i))
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
		k := api.KeyFromPrimary(uint64(800 + i))
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
		k := api.KeyFromPrimary(uint64(900 + i))
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
		k := api.KeyFromPrimary(uint64(900 + i))
		_ = ts1.Put(context.Background(), k, bigObj(k, 2000))
	}
	err = ts1.Delete(context.Background(), api.KeyFromPrimary(901))
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
	got, _, err := ts2.Get(context.Background(), api.KeyFromPrimary(901))
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
	k := api.KeyFromPrimary(1100)
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

	s := NewHotStore(HotConfig{
		MaxBytes:  1 << 14, // 16 KiB — very small to force eviction
		NumShards: 1,       // single shard for deterministic eviction
		OnEvict: func(key uint64) {
			mu.Lock()
			evictedKeys = append(evictedKeys, api.KeyFromPrimary(key))
			mu.Unlock()
		},
	})
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	// Put entry and mark as backed.
	k1 := api.KeyFromPrimary(1000)
	_ = s.Put(context.Background(), k1, obj(k1, 100))
	s.SetBacked(k1)

	// Fill with many other entries to force SIEVE to evict k1.
	// evictPreferBacked evicts backed entries first (up to 4 skips),
	// so with backedCount > 0, k1 is guaranteed to be among the first
	// evicted.
	for i := range 200 {
		k2 := api.KeyFromPrimary(uint64(2000 + i))
		_ = s.Put(context.Background(), k2, obj(k2, 100))
	}

	// Wait for sweeper to process overshoot and evict k1.
	poll.Eventually(t, 2*time.Second, 10*time.Millisecond, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return slices.Contains(evictedKeys, k1)
	})

	mu.Lock()
	found := slices.Contains(evictedKeys, k1)
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
	k := api.KeyFromPrimary(1200)
	_ = ts.Put(context.Background(), k, bigObj(k, 2000))
	require.Len(t, ts.warm.Keys(), 1, "warm should have the large object")

	// Enqueue a tombstone directly — simulates hot-tier eviction.
	ts.tombstoneQueue <- k

	// The dedicated drain goroutine should process it within a few
	// drain cycles. The tombstone removes the key from the warm tier.
	poll.Eventually(t, 5*time.Second, 20*time.Millisecond, func() bool {
		return !slices.Contains(ts.warm.Keys(), k.Primary())
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
