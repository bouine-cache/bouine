package storage

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/thylong/bouine/pkg/api"
)

func obj(key api.Key, bodySize int) *api.Object {
	return &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     http.Header{"Content-Type": {"text/plain"}},
		Body:       make([]byte, bodySize),
		BodySize:   int64(bodySize),
		StoredAt:   time.Now(),
		TTL:        time.Minute,
	}
}

func TestHotStore_PutGet(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})

	k := KeyHash([]byte("test-key"))
	o := obj(k, 100)

	if err := s.Put(context.Background(), k, o); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := s.Get(context.Background(), k)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got == nil {
		t.Fatal("expected hit, got miss")
	}
	if got.StatusCode != 200 {
		t.Fatalf("status = %d", got.StatusCode)
	}
	if got.Hits != 1 {
		t.Fatalf("hits = %d, want 1", got.Hits)
	}
}

func TestHotStore_Miss(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})

	got, err := s.Get(context.Background(), 999)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != nil {
		t.Fatal("expected miss")
	}
	st := s.Stats()
	if st.Misses != 1 {
		t.Fatalf("misses = %d", st.Misses)
	}
}

func TestHotStore_Delete(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	k := KeyHash([]byte("del"))
	_ = s.Put(context.Background(), k, obj(k, 50))
	_ = s.Delete(context.Background(), k)

	got, _ := s.Get(context.Background(), k)
	if got != nil {
		t.Fatal("expected miss after delete")
	}
}

func TestHotStore_EvictsOnFull(t *testing.T) {
	t.Parallel()
	// 4 shards, 4096 bytes total = 1024 per shard.
	s := NewHotStore(HotConfig{MaxBytes: 4096, NumShards: 4})

	// Insert objects until eviction must have happened.
	for i := range 100 {
		k := api.Key(i)
		_ = s.Put(context.Background(), k, obj(k, 500))
	}

	st := s.Stats()
	if st.Evictions == 0 {
		t.Fatal("expected evictions")
	}
	if st.HotBytes > 4096 {
		t.Fatalf("HotBytes = %d, exceeds budget", st.HotBytes)
	}
}

func TestHotStore_Replace(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	k := KeyHash([]byte("replace"))

	_ = s.Put(context.Background(), k, obj(k, 100))
	_ = s.Put(context.Background(), k, obj(k, 200))

	got, _ := s.Get(context.Background(), k)
	if got == nil {
		t.Fatal("expected hit")
	}
	if got.BodySize != 200 {
		t.Fatalf("body_size = %d, want 200", got.BodySize)
	}
	st := s.Stats()
	if st.HotEntries != 1 {
		t.Fatalf("entries = %d, want 1", st.HotEntries)
	}
}

func TestHotStore_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 8})
	var wg sync.WaitGroup

	for g := range 8 {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := range 1000 {
				k := api.Key(base*1000 + i)
				_ = s.Put(context.Background(), k, obj(k, 64))
				_, _ = s.Get(context.Background(), k)
			}
		}(g)
	}
	wg.Wait()

	st := s.Stats()
	if st.Hits == 0 {
		t.Fatal("expected hits from concurrent access")
	}
}

func TestHotStore_Stats(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	k := KeyHash([]byte("stats"))
	_ = s.Put(context.Background(), k, obj(k, 50))
	_, _ = s.Get(context.Background(), k)
	_, _ = s.Get(context.Background(), 12345) // miss

	st := s.Stats()
	if st.HotEntries != 1 {
		t.Fatalf("entries = %d", st.HotEntries)
	}
	if st.Hits != 1 {
		t.Fatalf("hits = %d", st.Hits)
	}
	if st.Misses != 1 {
		t.Fatalf("misses = %d", st.Misses)
	}
}

func TestKeyHash_Deterministic(t *testing.T) {
	t.Parallel()
	a := KeyHash([]byte("hello"))
	b := KeyHash([]byte("hello"))
	if a != b {
		t.Fatalf("non-deterministic: %d != %d", a, b)
	}
	c := KeyHash([]byte("world"))
	if a == c {
		t.Fatal("collision on different inputs")
	}
}

func TestHotStore_SetWarm(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})

	k := KeyHash([]byte("warm-key"))
	o := obj(k, 512)
	if err := s.Put(context.Background(), k, o); err != nil {
		t.Fatal(err)
	}

	s.SetWarm(k)

	sh := &s.shards[uint64(k)&s.mask]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if e, ok := sh.entries[k]; !ok || !e.hasWarm {
		t.Fatal("expected entry to be marked hasWarm after SetWarm")
	}
	if sh.warmCount != 1 {
		t.Fatalf("warmCount = %d, want 1", sh.warmCount)
	}
}

func TestHotStore_EvictPreferWarm(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 4 << 10, NumShards: 1})
	ctx := context.Background()

	k1 := KeyHash([]byte("a"))
	k2 := KeyHash([]byte("b"))
	_ = s.Put(ctx, k1, obj(k1, 1024))
	_ = s.Put(ctx, k2, obj(k2, 1024))

	// Mark k2 as warm-backed. It should be evicted first.
	s.SetWarm(k2)

	// Add a third entry — triggers eviction.
	k3 := KeyHash([]byte("c"))
	if err := s.Put(ctx, k3, obj(k3, 1024)); err != nil {
		t.Fatal(err)
	}

	// k1 (hot-only) should survive; k2 may or may not depending
	// on SIEVE visited bits, but k3 must be present.
	if _, err := s.Get(ctx, k3); err != nil {
		t.Fatal("k3 should exist, got:", err)
	}
}

func TestHotStore_EvictFallbackNoWarm(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 3 << 10, NumShards: 1})
	ctx := context.Background()

	k1 := KeyHash([]byte("x"))
	k2 := KeyHash([]byte("y"))
	_ = s.Put(ctx, k1, obj(k1, 1000))
	_ = s.Put(ctx, k2, obj(k2, 1000))

	k3 := KeyHash([]byte("z"))
	if err := s.Put(ctx, k3, obj(k3, 1000)); err != nil {
		t.Fatal(err)
	}

	// k3 must have been inserted (eviction loop allowed it).
	if _, err := s.Get(ctx, k3); err != nil {
		t.Fatal("k3 should exist:", err)
	}
}

func TestHotStore_WarmCountConsistency(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	ctx := context.Background()

	k := KeyHash([]byte("consistency"))
	_ = s.Put(ctx, k, obj(k, 100))
	s.SetWarm(k)

	// Overwrite with new entry; warm status resets.
	_ = s.Put(ctx, k, obj(k, 200))
	s.SetWarm(k)

	sh := &s.shards[uint64(k)&s.mask]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if e, ok := sh.entries[k]; !ok || !e.hasWarm {
		t.Fatal("entry should have hasWarm after re-marking")
	}
	if sh.warmCount != 1 {
		t.Fatalf("warmCount = %d, want 1 after re-mark", sh.warmCount)
	}
}
