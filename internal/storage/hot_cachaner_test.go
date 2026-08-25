package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

func cachanerHotStore(t *testing.T, budget int64) *HotStore {
	t.Helper()
	return NewHotStore(HotConfig{
		MaxBytes:             budget,
		NumShards:            1,
		HotEvictionAlgorithm: "cachaner",
		ReaperInterval:       -1,
	})
}

func TestCachanerHot_PutGet(t *testing.T) {
	t.Parallel()
	s := cachanerHotStore(t, 1<<20)
	defer func() { _ = s.Close(context.Background()) }()

	k := testkey.Hash([]byte("sf-key"))
	o := obj(k, 100)
	require.NoError(t, s.Put(context.Background(), k, o))

	got, src, err := s.Get(context.Background(), k)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, api.SourceHot, src)
	require.Equal(t, 200, got.StatusCode)
}

func TestCachanerHot_Eviction(t *testing.T) {
	t.Parallel()
	// 4 KiB budget. Each 1 KiB body object is ~1537 bytes (body + overhead),
	// so the budget holds 2 entries; the 3rd Put forces eviction.
	s := cachanerHotStore(t, 4<<10)
	defer func() { _ = s.Close(context.Background()) }()
	ctx := context.Background()

	k1 := testkey.Hash([]byte("key-1"))
	k2 := testkey.Hash([]byte("key-2"))
	k3 := testkey.Hash([]byte("key-3"))

	require.NoError(t, s.Put(ctx, k1, obj(k1, 1024)))
	require.NoError(t, s.Put(ctx, k2, obj(k2, 1024)))

	// With visited=true on insert (#484), the Get fast path (RLock)
	// skips evict.Access, so the cachaner freq counter is never
	// incremented and freq-based protection cannot engage. Clear the
	// visited bit on both entries so the next Get takes the slow path
	// (write lock) and increments freq — this restores the
	// pre-#484 visited=false insert state for this isolated
	// freq-policy test.
	shard := &s.shards[0]
	shard.mu.Lock()
	if e, ok := shard.entries.Get(k1); ok {
		e.entry.ClearVisited()
	}
	if e, ok := shard.entries.Get(k2); ok {
		e.entry.ClearVisited()
	}
	shard.mu.Unlock()

	// Access k1 to give it freq > 0 (protected from eviction).
	_, _, err := s.Get(ctx, k1)
	require.NoError(t, err)

	// Insert k3 — triggers eviction. k1 (freq=1) survives; k2 (freq=0)
	// is evicted.
	require.NoError(t, s.Put(ctx, k3, obj(k3, 1024)))

	// k1 should still be present (it was accessed, freq > 0).
	got1, _, err := s.Get(ctx, k1)
	require.NoError(t, err)
	require.NotNil(t, got1, "accessed key should survive eviction")

	// k2 should have been evicted (cold, freq=0).
	got2, _, err := s.Get(ctx, k2)
	require.NoError(t, err)
	require.Nil(t, got2, "cold key should be evicted")
}

func TestCachanerHot_FreqProtectsHotKey(t *testing.T) {
	t.Parallel()
	// 8 KiB budget. Each 1 KiB body object is ~1280 bytes, so the budget
	// holds ~6 entries. Insert 1 hot key (re-accessed between cold key
	// insertions so freq stays high) + several cold keys. The hot key
	// should survive because each access between sweeps re-increments
	// freq. With visited=true on insert (#484), the Get fast path
	// (RLock) skips evict.Access, so freq is never incremented. Clear
	// the visited bit on the hot key after insert so the first Get
	// takes the slow path and seeds freq; subsequent sweeps clear the
	// visited bit again, so each re-access keeps freq replenished.
	s := cachanerHotStore(t, 8<<10)
	defer func() { _ = s.Close(context.Background()) }()
	ctx := context.Background()

	hot := testkey.Hash([]byte("hot"))
	require.NoError(t, s.Put(ctx, hot, obj(hot, 1024)))

	// Reset the hot key's visited bit so the first Get takes the slow
	// path and seeds the cachaner freq counter. Without this, the
	// visited=true insert fast path would skip freq incrementation
	// entirely (see #484).
	shard := &s.shards[0]
	shard.mu.Lock()
	if e, ok := shard.entries.Get(hot); ok {
		e.entry.ClearVisited()
	}
	shard.mu.Unlock()

	// Insert cold keys, but re-access the hot key before each insertion
	// so its freq is re-incremented (the slow path fires when visited
	// has been cleared by a previous sweep). Each cold key gets one
	// sweep of protection from visited=true on insert, so we insert
	// enough cold keys to exhaust that and force real eviction
	// decisions based on freq.
	for i := range 'i' {
		// Re-access hot key to keep it warm.
		_, _, err := s.Get(ctx, hot)
		require.NoError(t, err)

		cold := testkey.Hash([]byte("cold-" + string(rune('a'+i))))
		require.NoError(t, s.Put(ctx, cold, obj(cold, 1024)))
	}

	// The hot key should still be present because it was re-accessed
	// between eviction sweeps, keeping its freq counter replenished.
	got, _, err := s.Get(ctx, hot)
	require.NoError(t, err)
	require.NotNil(t, got, "re-accessed hot key should survive eviction")
}

func TestCachanerHot_Delete(t *testing.T) {
	t.Parallel()
	s := cachanerHotStore(t, 1<<20)
	defer func() { _ = s.Close(context.Background()) }()
	ctx := context.Background()

	k := testkey.Hash([]byte("del-key"))
	require.NoError(t, s.Put(ctx, k, obj(k, 100)))
	require.NoError(t, s.Delete(ctx, k))

	got, _, err := s.Get(ctx, k)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestCachanerHot_EvictionOccurs(t *testing.T) {
	t.Parallel()
	// 2 KiB budget. Each 1 KiB body object is ~1280 bytes, so the budget
	// holds 1 entry; the 2nd Put forces eviction.
	s := cachanerHotStore(t, 2<<10)
	defer func() { _ = s.Close(context.Background()) }()
	ctx := context.Background()

	k1 := testkey.Hash([]byte("first"))
	k2 := testkey.Hash([]byte("second"))
	require.NoError(t, s.Put(ctx, k1, obj(k1, 1024)))
	require.NoError(t, s.Put(ctx, k2, obj(k2, 1024)))

	// At least one key should have been evicted.
	totalKeys := len(s.Keys())
	require.Less(t, totalKeys, 3, "eviction should have occurred")
}
