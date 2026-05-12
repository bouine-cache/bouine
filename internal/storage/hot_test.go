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
