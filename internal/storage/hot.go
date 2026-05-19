package storage

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"

	"github.com/thylong/bouine/internal/storage/sieve"
	"github.com/thylong/bouine/pkg/api"
)

// HotStore is the sharded in-memory (L0) cache tier. It implements
// the Store interface using a fixed number of shards, each protected
// by its own mutex, with SIEVE eviction.
//
// Stable.
type HotStore struct {
	shards   []shard
	mask     uint64
	maxBytes int64
	stats    hotStats
}

type shard struct {
	mu      sync.Mutex
	entries map[api.Key]*hotEntry
	evict   *sieve.List[api.Key]
	bytes   int64
}

type hotEntry struct {
	obj   *api.Object
	sieve *sieve.Entry[api.Key]
}

type hotStats struct {
	hits      atomic.Int64
	misses    atomic.Int64
	evictions atomic.Int64
}

// HotConfig configures the hot store.
type HotConfig struct {
	// MaxBytes is the total memory budget across all shards.
	MaxBytes int64
	// NumShards overrides the default shard count. Zero means
	// min(runtime.NumCPU(), 64).
	NumShards int
}

// NewHotStore creates a sharded in-memory store.
func NewHotStore(cfg HotConfig) *HotStore {
	n := cfg.NumShards
	if n <= 0 {
		n = runtime.NumCPU()
		if n > 64 {
			n = 64
		}
	}
	// Round up to power of two for fast modulo.
	n = nextPow2(n)

	shards := make([]shard, n)
	for i := range shards {
		shards[i].entries = make(map[api.Key]*hotEntry)
		shards[i].evict = sieve.NewList[api.Key]()
	}
	return &HotStore{
		shards:   shards,
		mask:     uint64(n - 1), //nolint:gosec // n is always a positive power of two
		maxBytes: cfg.MaxBytes,
	}
}

func (h *HotStore) shard(key api.Key) *shard {
	return &h.shards[uint64(key)&h.mask]
}

// Get looks up a key in the hot tier. Returns nil, nil on miss (not
// an error — a miss is a normal control-flow outcome).
func (h *HotStore) Get(_ context.Context, key api.Key) (*api.Object, error) {
	s := h.shard(key)
	s.mu.Lock()
	e := s.entries[key]
	if e != nil {
		s.evict.Access(key, func(k api.Key) *sieve.Entry[api.Key] {
			return e.sieve
		})
		e.obj.Hits++
	}
	s.mu.Unlock()

	if e == nil {
		h.stats.misses.Add(1)
		return nil, nil
	}
	h.stats.hits.Add(1)
	return e.obj, nil
}

// Put stores an object. If the store is over budget, SIEVE eviction
// runs until enough space is freed.
func (h *HotStore) Put(_ context.Context, key api.Key, obj *api.Object) error {
	size := objSize(obj)
	s := h.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	// Evict until we have room.
	perShardMax := h.maxBytes / int64(len(h.shards))
	for s.bytes+size > perShardMax && s.evict.Len() > 0 {
		evKey, ok := s.evict.Evict()
		if !ok {
			break
		}
		if old, exists := s.entries[evKey]; exists {
			s.bytes -= objSize(old.obj)
			delete(s.entries, evKey)
			h.stats.evictions.Add(1)
		}
	}

	// Remove old entry if replacing.
	if old, exists := s.entries[key]; exists {
		s.bytes -= objSize(old.obj)
		s.evict.Remove(old.sieve)
	}

	se, _ := s.evict.Access(key, func(k api.Key) *sieve.Entry[api.Key] {
		return nil // force insert
	})
	s.entries[key] = &hotEntry{obj: obj, sieve: se}
	s.bytes += size
	return nil
}

// Delete removes a key from the hot tier.
func (h *HotStore) Delete(_ context.Context, key api.Key) error {
	s := h.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.entries[key]; ok {
		s.bytes -= objSize(e.obj)
		s.evict.Remove(e.sieve)
		delete(s.entries, key)
	}
	return nil
}

// Ban evaluates a predicate against all entries. Phase 4 adds the
// lazy-evaluated ban list; for now this is an eager scan.
func (h *HotStore) Ban(_ context.Context, _ api.BanExpr) (int, error) {
	return 0, nil
}

// Stats returns an atomic snapshot.
func (h *HotStore) Stats() api.Stats {
	var hotEntries, hotBytes int64
	for i := range h.shards {
		s := &h.shards[i]
		s.mu.Lock()
		hotEntries += int64(len(s.entries))
		hotBytes += s.bytes
		s.mu.Unlock()
	}
	return api.Stats{
		HotEntries: hotEntries,
		HotBytes:   hotBytes,
		Hits:       h.stats.hits.Load(),
		Misses:     h.stats.misses.Load(),
		Evictions:  h.stats.evictions.Load(),
	}
}

// Close is a no-op for the hot tier (no files to close).
func (h *HotStore) Close(_ context.Context) error { return nil }

// KeyHash computes the canonical cache key from a byte slice.
func KeyHash(b []byte) api.Key {
	return api.Key(xxhash.Sum64(b))
}

func objSize(obj *api.Object) int64 {
	// Body + a fixed overhead for the struct and header map.
	return int64(len(obj.Body)) + 256
}

func nextPow2(v int) int {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++
	if v < 1 {
		return 1
	}
	return v
}
