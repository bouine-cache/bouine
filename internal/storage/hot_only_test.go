package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/poll"
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
	{
		err := s.Put(ctx, k, obj(k, 100))
		require.NoError(t, err)
	}
	require.True(t, hotOnlyContains(s, k))
	require.Equal(t, 1, hotOnlyCount(s))
}

func TestHotOnly_SetBackedRemovesKey(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	ctx := context.Background()

	k := KeyHash([]byte("backed-key"))
	_ = s.Put(ctx, k, obj(k, 100))
	require.True(t, hotOnlyContains(s, k))
	s.SetBacked(k)
	require.False(t, hotOnlyContains(s, k))
	require.Equal(t, 0, hotOnlyCount(s))
}

func TestHotOnly_ClearBackedReaddsKey(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	ctx := context.Background()

	k := KeyHash([]byte("clear-backed-key"))
	_ = s.Put(ctx, k, obj(k, 100))
	s.SetBacked(k)
	require.False(t, hotOnlyContains(s, k))
	s.ClearBacked(k)
	require.True(t, hotOnlyContains(s, k))
	require.Equal(t, 1, hotOnlyCount(s))
}

func TestHotOnly_DeleteRemovesKey(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	ctx := context.Background()

	k := KeyHash([]byte("delete-key"))
	_ = s.Put(ctx, k, obj(k, 100))
	require.True(t, hotOnlyContains(s, k))
	{
		err := s.Delete(ctx, k)
		require.NoError(t, err)
	}
	require.False(t, hotOnlyContains(s, k))
	require.Equal(t, 0, hotOnlyCount(s))
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

	require.True(t, hotOnlyContains(s, k))
	count, err := s.Ban(ctx, api.BanExpr{PathRegex: "^/ban-me"})
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.False(t, hotOnlyContains(s, k))
	require.Equal(t, 0, hotOnlyCount(s))
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
	require.Equal(t, 1, total)
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
	poll.Eventually(t, 2*time.Second, time.Millisecond, func() bool {
		sh.mu.RLock()
		over := sh.bytes > 4<<10
		sh.mu.RUnlock()
		return !over
	})

	// After the sweeper runs, no evicted key should remain in hotOnly.
	// Every key still in entries must be in hotOnly; every evicted key
	// must not be.
	for _, k := range keys {
		inEntries := false
		sh.mu.RLock()
		_, inEntries = sh.entries[k]
		sh.mu.RUnlock()
		if inEntries {
			assert.True(t, hotOnlyContains(s, k))
		} else {
			assert.False(t, hotOnlyContains(s, k))
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
	require.False(t, hotOnlyContains(s, k))
	// Put again — replaces the backed entry with a new non-backed one.
	_ = s.Put(ctx, k, obj(k, 100))
	require.True(t, hotOnlyContains(s, k))
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

	require.Equal(t, 10, hotOnlyCount(s))

	// A batch with limit < total returns at most limit keys.
	batch, _ := s.HotOnlyKeys(0, 3)
	if len(batch) > 3 {
		t.Fatalf("batch len = %d, want <= 3", len(batch))
	}
	require.NotEqual(t, 0, len(batch))

	// All returned keys must be valid hot-only entries.
	for _, k := range batch {
		require.True(t, hotOnlyContains(s, k))
	}

	// A large limit returns all 10 keys.
	all, _ := s.HotOnlyKeys(0, 100)
	require.Len(t, all, 10)

	// Verify all keys are covered by the full batch.
	seen := make(map[api.Key]struct{}, 10)
	for _, k := range all {
		seen[k] = struct{}{}
	}
	require.Len(t, seen, 10)
}

func TestHotOnly_KeysEmpty(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	require.Equal(t, 0, hotOnlyCount(s))
	{
		keys, _ := s.HotOnlyKeys(0, 10)
		require.Nil(t, keys)
	}
}

func TestHotOnly_KeysLimitZero(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	ctx := context.Background()
	_ = s.Put(ctx, KeyHash([]byte("k")), obj(KeyHash([]byte("k")), 10))
	{
		keys, _ := s.HotOnlyKeys(0, 0)
		require.Nil(t, keys)
	}
}
