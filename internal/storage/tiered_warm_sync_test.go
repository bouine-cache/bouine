package storage

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/thylong/bouine/internal/storage/warm"
	"github.com/thylong/bouine/pkg/api"
)

func tieredStoreWithSync(t *testing.T, batchSize int) *TieredStore {
	t.Helper()
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:               HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:              &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:            filepath.Join(dir, "index.wal"),
		BodyThreshold:     1024,
		WarmSyncInterval:  -1, // disabled — we call runWarmSyncCycle manually
		WarmSyncBatchSize: batchSize,
	})
	if err != nil {
		t.Fatalf("NewTieredStore: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close(context.Background()) })
	return ts
}

func TestWarmSync_WritesHotOnlyEntriesToWarm(t *testing.T) {
	t.Parallel()
	ts := tieredStoreWithSync(t, 100) // sync disabled, we call cycle manually
	// Put small objects (below bodyThreshold=1024) so they stay hot-only.
	for i := range 10 {
		k := api.Key(100 + i)
		_ = ts.Put(context.Background(), k, obj(k, 100))
	}

	// Verify warm is empty before sync.
	if keys := ts.warm.Keys(); len(keys) != 0 {
		t.Fatalf("warm should be empty before sync, got %d keys", len(keys))
	}

	ts.runWarmSyncCycle(context.Background())

	// Verify warm now has the entries.
	if keys := ts.warm.Keys(); len(keys) != 10 {
		t.Fatalf("warm should have 10 keys after sync, got %d", len(keys))
	}
}

func TestWarmSync_SkipsWarmBackedEntries(t *testing.T) {
	t.Parallel()
	ts := tieredStoreWithSync(t, 100)

	// Put a large object (> bodyThreshold) so it goes to warm on Put.
	k := api.Key(200)
	_ = ts.Put(context.Background(), k, bigObj(k, 2000))

	warmKeysBefore := len(ts.warm.Keys())
	if warmKeysBefore != 1 {
		t.Fatalf("expected 1 warm key from large Put, got %d", warmKeysBefore)
	}

	ts.runWarmSyncCycle(context.Background())

	warmKeysAfter := len(ts.warm.Keys())
	if warmKeysAfter != warmKeysBefore {
		t.Fatalf("warm key count changed: %d → %d (should skip warm-backed)",
			warmKeysBefore, warmKeysAfter)
	}
}

func TestWarmSync_RespectsBatchSize(t *testing.T) {
	t.Parallel()
	ts := tieredStoreWithSync(t, 3) // batch size 3

	for i := range 10 {
		k := api.Key(300 + i)
		_ = ts.Put(context.Background(), k, obj(k, 100))
	}

	ts.runWarmSyncCycle(context.Background())
	if keys := ts.warm.Keys(); len(keys) != 3 {
		t.Fatalf("expected 3 warm keys (batch size), got %d", len(keys))
	}

	// Second cycle should sync more.
	ts.runWarmSyncCycle(context.Background())
	if keys := ts.warm.Keys(); len(keys) != 6 {
		t.Fatalf("expected 6 warm keys after 2 cycles, got %d", len(keys))
	}
}

func TestWarmSync_TombstonesWarmBackedEvictions(t *testing.T) {
	t.Parallel()
	// Use a very small hot tier so the warm-backed entry is evicted
	// by SIEVE when we fill with competing entries.
	dir := t.TempDir()
	ts, err := NewTieredStore(TieredConfig{
		Hot:               HotConfig{MaxBytes: 1 << 14, NumShards: 1}, // 16 KiB, single shard
		Warm:              &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:            filepath.Join(dir, "index.wal"),
		BodyThreshold:     1024,
		WarmSyncInterval:  -1, // disabled — manual cycle
		WarmSyncBatchSize: 100,
	})
	if err != nil {
		t.Fatalf("NewTieredStore: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close(context.Background()) })

	// Put a large object (goes to warm on Put, marks hasWarm).
	k := api.Key(400)
	_ = ts.Put(context.Background(), k, bigObj(k, 2000))

	warmKeysBefore := len(ts.warm.Keys())
	if warmKeysBefore != 1 {
		t.Fatalf("expected 1 warm key, got %d", warmKeysBefore)
	}

	// Fill with many large entries to force SIEVE eviction of k.
	// evictPreferWarm targets warm-backed entries first.
	for i := range 50 {
		k2 := api.Key(500 + i)
		_ = ts.Put(context.Background(), k2, bigObj(k2, 2000))
	}

	// Drain tombstones — evicted warm-backed keys should be removed
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
		if wk == uint64(k) {
			t.Fatalf("key %d should have been tombstoned but is still in warm", k)
		}
	}
}

func TestWarmSync_TombstoneQueueOverflowNonBlocking(t *testing.T) {
	t.Parallel()
	ts := tieredStoreWithSync(t, 100)

	// Fill tombstone queue to capacity (4096). The channel is buffered
	// so all sends should succeed without blocking.
	for i := range 4096 {
		ts.tombstoneQueue <- api.Key(600 + i)
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
	if err != nil {
		t.Fatalf("NewTieredStore: %v", err)
	}

	// Let it run a few cycles.
	time.Sleep(300 * time.Millisecond)

	// Close should join syncWg within timeout.
	done := make(chan struct{})
	go func() {
		_ = ts.Close(context.Background())
		close(done)
	}()

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
		Hot:               HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:              &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:            walPath,
		BodyThreshold:     1024,
		WarmSyncInterval:  -1, // disabled — we call cycle manually
		WarmSyncBatchSize: 100,
	})
	if err != nil {
		t.Fatalf("NewTieredStore: %v", err)
	}

	for i := range 5 {
		k := api.Key(700 + i)
		_ = ts1.Put(context.Background(), k, obj(k, 100))
	}
	ts1.runWarmSyncCycle(context.Background())
	_ = ts1.Close(context.Background())

	// Reopen — warm should have the entries from the previous run.
	ts2, err := NewTieredStore(TieredConfig{
		Hot:               HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:              &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:            walPath,
		BodyThreshold:     1024,
		WarmSyncInterval:  -1,
		WarmSyncBatchSize: 100,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = ts2.Close(context.Background()) })

	if keys := ts2.warm.Keys(); len(keys) != 5 {
		t.Fatalf("expected 5 warm keys after restart, got %d", len(keys))
	}

	// Verify warm hits work.
	for i := range 5 {
		k := api.Key(700 + i)
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
		Hot:              HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:             &warm.Config{Dir: filepath.Join(dir, "warm"), MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:           filepath.Join(dir, "index.wal"),
		BodyThreshold:    1024,
		WarmSyncInterval: -1, // explicitly disabled
	})
	if err != nil {
		t.Fatalf("NewTieredStore: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close(context.Background()) })

	// syncWg should be 0 — no goroutine started.
	// Put small objects and wait — warm should stay empty.
	for i := range 5 {
		k := api.Key(800 + i)
		_ = ts.Put(context.Background(), k, obj(k, 100))
	}
	time.Sleep(200 * time.Millisecond)
	if keys := ts.warm.Keys(); len(keys) != 0 {
		t.Fatalf("warm should be empty with sync disabled, got %d keys", len(keys))
	}
}

func TestWarmSync_RebuildIndexFromScan(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	walPath := filepath.Join(dir, "index.wal")

	// Create store, fill warm, close.
	ts1, err := NewTieredStore(TieredConfig{
		Hot:               HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:              &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:            walPath,
		BodyThreshold:     1024,
		WarmSyncInterval:  -1,
		WarmSyncBatchSize: 100,
	})
	if err != nil {
		t.Fatalf("NewTieredStore: %v", err)
	}

	// Put large objects (go to warm on Put).
	for i := range 3 {
		k := api.Key(900 + i)
		_ = ts1.Put(context.Background(), k, bigObj(k, 2000))
	}
	_ = ts1.Close(context.Background())

	// Delete the WAL to simulate WAL loss.
	_ = os.Remove(walPath)

	// Reopen — should rebuild index from segment scan.
	ts2, err := NewTieredStore(TieredConfig{
		Hot:               HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:              &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:            walPath,
		BodyThreshold:     1024,
		WarmSyncInterval:  -1,
		WarmSyncBatchSize: 100,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = ts2.Close(context.Background()) })

	if keys := ts2.warm.Keys(); len(keys) != 3 {
		t.Fatalf("expected 3 warm keys after segment scan rebuild, got %d", len(keys))
	}
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
		Hot:               HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:              &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:            walPath,
		BodyThreshold:     1024,
		WarmSyncInterval:  -1,
		WarmSyncBatchSize: 100,
	})
	if err != nil {
		t.Fatalf("NewTieredStore: %v", err)
	}

	// Put 3 large objects, then delete one so a tombstone exists in the
	// segment alongside the live records.
	for i := range 3 {
		k := api.Key(900 + i)
		_ = ts1.Put(context.Background(), k, bigObj(k, 2000))
	}
	if err := ts1.Delete(context.Background(), api.Key(901)); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_ = ts1.Close(context.Background())

	// Delete the WAL to force the segment-scan fallback.
	_ = os.Remove(walPath)

	ts2, err := NewTieredStore(TieredConfig{
		Hot:               HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		Warm:              &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
		WALDir:            walPath,
		BodyThreshold:     1024,
		WarmSyncInterval:  -1,
		WarmSyncBatchSize: 100,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = ts2.Close(context.Background()) })

	// Keys 900 and 902 should be live; 901 should NOT have been
	// resurrected by the segment scan.
	if keys := ts2.warm.Keys(); len(keys) != 2 {
		t.Fatalf("expected 2 warm keys after tombstone-honouring rebuild, got %d: %v", len(keys), keys)
	}
	got, _, err := ts2.Get(context.Background(), api.Key(901))
	if err != nil {
		t.Fatalf("Get(901): %v", err)
	}
	if got != nil {
		t.Fatal("Get(901) should return nil — key was tombstoned before WAL loss")
	}
}

// TestPutReplace_TombstonesOldWarmCopy verifies that replacing a
// warm-backed entry with a smaller (hot-only) entry enqueues a
// tombstone so the stale warm copy is cleaned up by the sync loop.
// After the sync cycle, the new object is also synced to warm (it's
// now hot-only), so warm should have exactly 1 key — the new object.
func TestPutReplace_TombstonesOldWarmCopy(t *testing.T) {
	t.Parallel()
	ts := tieredStoreWithSync(t, 100)

	// Put a large object — goes to warm, marks hasWarm.
	k := api.Key(1100)
	_ = ts.Put(context.Background(), k, bigObj(k, 2000))

	if keys := ts.warm.Keys(); len(keys) != 1 {
		t.Fatalf("expected 1 warm key, got %d", len(keys))
	}

	// Replace with a small object — below bodyThreshold, does not
	// write to warm. The old warm-backed entry is replaced in hot,
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
	if got.BodySize != 100 {
		t.Fatalf("expected new body size 100, got %d (stale warm copy?)", got.BodySize)
	}
}

// TestOnEvictCallback verifies that the OnEvict callback fires when a
// warm-backed entry is evicted from the hot tier. Uses a single shard
// with a tiny memory budget so the warm-backed entry is guaranteed to
// be evicted by evictPreferWarm before hot-only entries.
func TestOnEvictCallback(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var evictedKeys []api.Key

	s := NewHotStore(HotConfig{
		MaxBytes:  1 << 14, // 16 KiB — very small to force eviction
		NumShards: 1,       // single shard for deterministic eviction
		OnEvict: func(key api.Key) {
			mu.Lock()
			evictedKeys = append(evictedKeys, key)
			mu.Unlock()
		},
	})
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	// Put entry and mark as warm-backed.
	k1 := api.Key(1000)
	_ = s.Put(context.Background(), k1, obj(k1, 100))
	s.SetWarm(k1)

	// Fill with many other entries to force SIEVE to evict k1.
	// evictPreferWarm evicts warm-backed entries first (up to 4 skips),
	// so with warmCount > 0, k1 is guaranteed to be among the first
	// evicted.
	for i := range 200 {
		k2 := api.Key(2000 + i)
		_ = s.Put(context.Background(), k2, obj(k2, 100))
	}

	// Wait for sweeper to process overshoot.
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	found := slices.Contains(evictedKeys, k1)
	mu.Unlock()

	if !found {
		t.Fatalf("OnEvict did not fire for warm-backed key %d (evicted %d total)",
			k1, len(evictedKeys))
	}
}
