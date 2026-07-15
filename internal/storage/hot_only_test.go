package storage

import (
	"context"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// hotOnlyContains checks whether key is hot-only (present and not backed).
func hotOnlyContains(s *HotStore, key api.Key) bool {
	sh := &s.shards[uint64(key)&s.mask]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, ok := sh.entries[key]
	return ok && !e.hasBackup
}

// hotOnlyCount returns the total number of hot-only entries across all shards.
func hotOnlyCount(s *HotStore) int {
	var total int
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for _, e := range sh.entries {
			if !e.hasBackup {
				total++
			}
		}
		sh.mu.RUnlock()
	}
	return total
}

func TestHotOnly_PutAddsKey(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	ctx := context.Background()

	k := KeyHash([]byte("hot-put"))
	if err := s.Put(ctx, k, obj(k, 100)); err != nil {
		t.Fatal(err)
	}
	if !hotOnlyContains(s, k) {
		t.Fatal("key should be in hotOnly after Put")
	}
	if hotOnlyCount(s) != 1 {
		t.Fatalf("HotOnlyCount = %d, want 1", hotOnlyCount(s))
	}
}

func TestHotOnly_SetBackedRemovesKey(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	ctx := context.Background()

	k := KeyHash([]byte("backed-key"))
	_ = s.Put(ctx, k, obj(k, 100))
	if !hotOnlyContains(s, k) {
		t.Fatal("key should be in hotOnly before SetBacked")
	}
	s.SetBacked(k)
	if hotOnlyContains(s, k) {
		t.Fatal("key should not be in hotOnly after SetBacked")
	}
	if hotOnlyCount(s) != 0 {
		t.Fatalf("HotOnlyCount = %d, want 0", hotOnlyCount(s))
	}
}

func TestHotOnly_ClearBackedReaddsKey(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	ctx := context.Background()

	k := KeyHash([]byte("clear-backed-key"))
	_ = s.Put(ctx, k, obj(k, 100))
	s.SetBacked(k)
	if hotOnlyContains(s, k) {
		t.Fatal("key should not be in hotOnly after SetBacked")
	}
	s.ClearBacked(k)
	if !hotOnlyContains(s, k) {
		t.Fatal("key should be in hotOnly after ClearBacked")
	}
	if hotOnlyCount(s) != 1 {
		t.Fatalf("HotOnlyCount = %d, want 1", hotOnlyCount(s))
	}
}

func TestHotOnly_DeleteRemovesKey(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	ctx := context.Background()

	k := KeyHash([]byte("delete-key"))
	_ = s.Put(ctx, k, obj(k, 100))
	if !hotOnlyContains(s, k) {
		t.Fatal("key should be in hotOnly before Delete")
	}
	if err := s.Delete(ctx, k); err != nil {
		t.Fatal(err)
	}
	if hotOnlyContains(s, k) {
		t.Fatal("key should not be in hotOnly after Delete")
	}
	if hotOnlyCount(s) != 0 {
		t.Fatalf("HotOnlyCount = %d, want 0", hotOnlyCount(s))
	}
}

func TestHotOnly_BanRemovesKey(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()
	ctx := context.Background()

	k := KeyHash([]byte("ban-key"))
	o := obj(k, 50)
	o.Header.Set(header.XBouineHost, "example.com")
	o.Header.Set(header.XBouinePath, "/ban-me")
	_ = s.Put(ctx, k, o)

	if !hotOnlyContains(s, k) {
		t.Fatal("key should be in hotOnly before Ban")
	}
	count, err := s.Ban(ctx, api.BanExpr{PathRegex: "^/ban-me"})
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("ban count = %d, want 1", count)
	}
	if hotOnlyContains(s, k) {
		t.Fatal("key should not be in hotOnly after Ban")
	}
	if hotOnlyCount(s) != 0 {
		t.Fatalf("HotOnlyCount = %d, want 0", hotOnlyCount(s))
	}
}

func TestHotOnly_EvictionRemovesKey(t *testing.T) {
	t.Parallel()
	// Single shard, 2 KiB budget. Each object is ~1280 bytes, so the
	// budget holds 1 entry; the 2nd Put forces eviction.
	s := NewHotStore(HotConfig{MaxBytes: 2 << 10, NumShards: 1})
	ctx := context.Background()

	k1 := KeyHash([]byte("evict-1"))
	k2 := KeyHash([]byte("evict-2"))
	_ = s.Put(ctx, k1, obj(k1, 1024))
	_ = s.Put(ctx, k2, obj(k2, 1024))

	// One of k1/k2 was evicted; the survivor should still be in hotOnly,
	// the evicted one should not.
	total := hotOnlyCount(s)
	if total != 1 {
		t.Fatalf("HotOnlyCount = %d, want 1 (one entry evicted)", total)
	}
	if hotOnlyContains(s, k1) && hotOnlyContains(s, k2) {
		t.Fatal("both keys in hotOnly — expected one to be evicted")
	}
}

func TestHotOnly_SweeperEvictionRemovesKey(t *testing.T) {
	t.Parallel()
	// Single shard, small budget. Each object is ~1280 bytes. With
	// perShardMax = 4 KiB, 3 entries fit. Inserting 6 entries forces
	// inline eviction of 4 (inlineEvictCap), then the sweeper handles
	// the remaining overshoot.
	s := NewHotStore(HotConfig{MaxBytes: 4 << 10, NumShards: 1})
	defer func() { _ = s.Close(context.Background()) }()
	ctx := context.Background()

	var keys []api.Key
	for i := range 8 {
		k := KeyHash([]byte("sweep-" + string(rune('a'+i))))
		keys = append(keys, k)
		_ = s.Put(ctx, k, obj(k, 1024))
	}

	// Wait for the sweeper to drain the overshoot. The sweeper runs
	// asynchronously, so we poll until the shard is within budget.
	sh := &s.shards[0]
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		sh.mu.RLock()
		over := sh.bytes > 4<<10
		sh.mu.RUnlock()
		if !over {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// After the sweeper runs, no evicted key should remain in hotOnly.
	// Every key still in entries must be in hotOnly; every evicted key
	// must not be.
	for _, k := range keys {
		inEntries := false
		sh.mu.RLock()
		_, inEntries = sh.entries[k]
		sh.mu.RUnlock()
		if inEntries {
			if !hotOnlyContains(s, k) {
				t.Errorf("key %d in entries but not in hotOnly", k)
			}
		} else {
			if hotOnlyContains(s, k) {
				t.Errorf("key %d not in entries but still in hotOnly (sweeper did not clean up)", k)
			}
		}
	}
}

func TestHotOnly_ReplaceBackedWithNonBacked(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	ctx := context.Background()

	k := KeyHash([]byte("replace-key"))
	_ = s.Put(ctx, k, obj(k, 100))
	s.SetBacked(k)
	if hotOnlyContains(s, k) {
		t.Fatal("key should not be in hotOnly after SetBacked")
	}
	// Put again — replaces the backed entry with a new non-backed one.
	_ = s.Put(ctx, k, obj(k, 100))
	if !hotOnlyContains(s, k) {
		t.Fatal("key should be back in hotOnly after replacing backed entry")
	}
}

func TestHotOnly_KeysRotation(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 1})
	ctx := context.Background()

	// Insert 10 keys.
	for i := range 10 {
		k := api.Key(i + 1)
		_ = s.Put(ctx, k, obj(k, 10))
	}

	if hotOnlyCount(s) != 10 {
		t.Fatalf("HotOnlyCount = %d, want 10", hotOnlyCount(s))
	}

	// A batch with limit < total returns at most limit keys.
	batch, _ := s.HotOnlyKeys(0, 3)
	if len(batch) > 3 {
		t.Fatalf("batch len = %d, want <= 3", len(batch))
	}
	if len(batch) == 0 {
		t.Fatal("batch should not be empty")
	}

	// All returned keys must be valid hot-only entries.
	for _, k := range batch {
		if !hotOnlyContains(s, k) {
			t.Fatalf("key %d in batch but not in hotOnly", k)
		}
	}

	// A large limit returns all 10 keys.
	all, _ := s.HotOnlyKeys(0, 100)
	if len(all) != 10 {
		t.Fatalf("all len = %d, want 10", len(all))
	}

	// Verify all keys are covered by the full batch.
	seen := make(map[api.Key]struct{}, 10)
	for _, k := range all {
		seen[k] = struct{}{}
	}
	if len(seen) != 10 {
		t.Fatalf("unique keys in full batch = %d, want 10", len(seen))
	}
}

func TestHotOnly_KeysEmpty(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	if hotOnlyCount(s) != 0 {
		t.Fatalf("HotOnlyCount = %d, want 0", hotOnlyCount(s))
	}
	if keys, _ := s.HotOnlyKeys(0, 10); keys != nil {
		t.Fatalf("HotOnlyKeys = %v, want nil", keys)
	}
}

func TestHotOnly_KeysLimitZero(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	ctx := context.Background()
	_ = s.Put(ctx, KeyHash([]byte("k")), obj(KeyHash([]byte("k")), 10))
	if keys, _ := s.HotOnlyKeys(0, 0); keys != nil {
		t.Fatalf("HotOnlyKeys with limit=0 = %v, want nil", keys)
	}
}
