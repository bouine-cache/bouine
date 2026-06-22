package storage

import (
	"context"
	"fmt"
	"regexp"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/thylong/bouine/internal/storage/sieve"
	"github.com/thylong/bouine/pkg/api"
)

// inlineEvictCap is the maximum number of entries evicted synchronously
// inside Put while holding the shard write lock. Additional over-budget
// bytes are reclaimed asynchronously by the background sweeper (Phase 2b).
// This bounds the worst-case lock hold time on the Put path.
const inlineEvictCap = 4

// HotStore is the sharded in-memory (L0) cache tier. It implements
// the Store interface using a fixed number of shards, each protected
// by its own mutex, with SIEVE eviction.
//
// Background eviction: Put performs at most inlineEvictCap evictions
// inline, then signals a per-store sweeper goroutine to drain any
// remaining overshoot asynchronously. The cache may transiently exceed
// maxBytes by at most one Put's object size until the sweeper runs.
//
// Stable.
type HotStore struct {
	shards   []shard
	mask     uint64
	maxBytes int64
	stats    hotStats
	// activeBans is the lazy ban list. Each entry was added via Ban() and
	// is checked against objects returned by Get(). Objects stored AFTER
	// the ban's CreatedAt are not subject to it (RFC 9111 §4.4 semantics).
	bansMu     sync.Mutex
	activeBans []activeBan
	// banCount mirrors len(activeBans) for a lock-free fast path in
	// matchesActiveBan. It is written only while holding bansMu but read
	// without any lock on the hot Get path, so the global bansMu is never
	// acquired on a cache hit in the common (no active bans) case.
	banCount atomic.Int64
	// evictSignal receives shard indices that need further eviction after
	// the inline cap was reached. Buffered to len(shards); non-blocking
	// sends coalesce burst signals.
	evictSignal chan int
	// done is closed by Close() to stop the background sweeper.
	done chan struct{}
}

// activeBan is a compiled, time-stamped ban predicate in the lazy list.
type activeBan struct {
	pred    banPredicate
	created time.Time
}

type shard struct {
	mu        sync.RWMutex
	entries   map[api.Key]*hotEntry
	evict     *sieve.List[api.Key]
	bytes     int64
	warmCount int64 // entries with warm backup (cheap to evict)
}

type hotEntry struct {
	obj   *api.Object
	sieve *sieve.Entry[api.Key]
	// hasWarm is true when the object also exists in the warm tier.
	// Eviction prefers these entries because they can be recovered
	// from disk.
	hasWarm bool
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

// NewHotStore creates a sharded in-memory store and starts the
// background eviction sweeper.
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
	h := &HotStore{
		shards:      shards,
		mask:        uint64(n - 1), //nolint:gosec // n is always a positive power of two
		maxBytes:    cfg.MaxBytes,
		evictSignal: make(chan int, n),
		done:        make(chan struct{}),
	}
	go h.sweeper()
	return h
}

func (h *HotStore) shard(key api.Key) *shard {
	return &h.shards[uint64(key)&h.mask]
}

// Get looks up a key in the hot tier. Returns nil, nil on miss (not
// an error — a miss is a normal control-flow outcome).
//
// Fast path (visited bit already set): acquires only a read lock,
// avoiding write-lock contention under concurrent read-heavy workloads.
// Slow path (visited=false, i.e. first access after eviction hand sweep):
// upgrades to a write lock to set the bit.
func (h *HotStore) Get(_ context.Context, key api.Key) (*api.Object, error) {
	s := h.shard(key)

	// Fast path: read lock. If the entry exists and its visited bit is
	// already set, no write is needed — just return the object.
	s.mu.RLock()
	e := s.entries[key]
	if e != nil && e.sieve.Visited() {
		obj := e.obj
		s.mu.RUnlock()
		// Lazy ban check: if any active ban matches this object, evict and
		// return nil (RFC 9111 §4.4 lazy semantics).
		if h.matchesActiveBan(obj) {
			// Upgrade to write lock to delete the entry.
			s.mu.Lock()
			if cur, ok := s.entries[key]; ok && cur.obj == obj {
				s.bytes -= objSize(obj)
				s.evict.Remove(cur.sieve)
				delete(s.entries, key)
				h.stats.evictions.Add(1)
			}
			s.mu.Unlock()
			h.stats.misses.Add(1)
			return nil, nil
		}
		h.stats.hits.Add(1)
		return obj, nil
	}
	s.mu.RUnlock()

	if e == nil {
		h.stats.misses.Add(1)
		return nil, nil
	}

	// Slow path: visited bit is false. Upgrade to write lock, re-check
	// (entry may have been evicted between RUnlock and Lock), then set it.
	s.mu.Lock()
	e = s.entries[key]
	if e != nil {
		s.evict.Access(key, func(k api.Key) *sieve.Entry[api.Key] {
			return e.sieve
		})
	}
	s.mu.Unlock()

	if e == nil {
		h.stats.misses.Add(1)
		return nil, nil
	}
	h.stats.hits.Add(1)
	return e.obj, nil
}

// Put stores an object. If the shard is over budget, SIEVE eviction
// runs inline for up to inlineEvictCap victims, then signals the
// background sweeper for any remaining overshoot. Entries with warm-tier
// backups are evicted first (cheap: recoverable from disk).
func (h *HotStore) Put(_ context.Context, key api.Key, obj *api.Object) error {
	size := objSize(obj)
	s := h.shard(key)
	shardIdx := int(uint64(key) & h.mask) //nolint:gosec // mask < len(shards) ≤ 64, never overflows int
	perShardMax := h.maxBytes / int64(len(h.shards))

	s.mu.Lock()
	stillOver := false
	for range inlineEvictCap {
		if s.bytes+size <= perShardMax || s.evict.Len() == 0 {
			break
		}
		evKey, ok := s.evictPreferWarm()
		if !ok {
			break
		}
		if old, exists := s.entries[evKey]; exists {
			s.bytes -= objSize(old.obj)
			delete(s.entries, evKey)
			h.stats.evictions.Add(1)
		}
	}
	if s.bytes+size > perShardMax {
		stillOver = true
	}

	// Remove old entry if replacing.
	if old, exists := s.entries[key]; exists {
		s.bytes -= objSize(old.obj)
		s.evict.Remove(old.sieve)
		if old.hasWarm {
			s.warmCount--
		}
	}

	se, _ := s.evict.Access(key, func(k api.Key) *sieve.Entry[api.Key] {
		return nil // force insert
	})
	s.entries[key] = &hotEntry{obj: obj, sieve: se}
	s.bytes += size
	s.mu.Unlock()

	// Signal the sweeper if the shard is still over budget after the
	// inline cap. Non-blocking: a full channel means a signal is already
	// pending for this shard.
	if stillOver {
		select {
		case h.evictSignal <- shardIdx:
		default:
		}
	}
	return nil
}

// sweeper is the background goroutine that drains overshoot evictions
// signalled by Put. It terminates when Close() is called.
func (h *HotStore) sweeper() {
	perShardMax := h.maxBytes / int64(len(h.shards))
	for {
		select {
		case <-h.done:
			return
		case idx := <-h.evictSignal:
			s := &h.shards[idx]
			s.mu.Lock()
			for s.bytes > perShardMax && s.evict.Len() > 0 {
				evKey, ok := s.evictPreferWarm()
				if !ok {
					break
				}
				if old, exists := s.entries[evKey]; exists {
					s.bytes -= objSize(old.obj)
					delete(s.entries, evKey)
					h.stats.evictions.Add(1)
				}
			}
			s.mu.Unlock()
		}
	}
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

// Ban performs an eager scan of the hot tier, deleting all entries
// whose stored object matches the ban predicate (RFC 9111 §4.4 lazy
// bans are deferred to post-v1.0; this eager scan handles the common
// case of host/path/surrogate-key invalidation on administrative APIs).
func (h *HotStore) Ban(_ context.Context, expr api.BanExpr) (int, error) {
	pred, err := compileBanPredicate(expr)
	if err != nil {
		return 0, err
	}

	total := 0
	for i := range h.shards {
		s := &h.shards[i]
		s.mu.Lock()
		for key, e := range s.entries {
			if pred(e.obj) {
				s.bytes -= objSize(e.obj)
				s.evict.Remove(e.sieve)
				delete(s.entries, key)
				h.stats.evictions.Add(1)
				total++
			}
		}
		s.mu.Unlock()
	}
	// Register in the lazy ban list so objects filled AFTER this scan
	// are also checked on next lookup (RFC 9111 §4.4 lazy semantics).
	createdAt := expr.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	h.bansMu.Lock()
	h.activeBans = append(h.activeBans, activeBan{pred: pred, created: createdAt})
	h.banCount.Store(int64(len(h.activeBans)))
	h.bansMu.Unlock()
	return total, nil
}

// banPredicate is a compiled function that returns true when an Object
// should be evicted by a ban expression.
type banPredicate func(*api.Object) bool

// compileBanPredicate pre-compiles the regexps in expr and returns a
// closure that evaluates the full predicate against a single Object.
func compileBanPredicate(expr api.BanExpr) (banPredicate, error) {
	var hostRE, pathRE *regexp.Regexp
	if expr.HostRegex != "" {
		re, err := regexp.Compile(expr.HostRegex)
		if err != nil {
			return nil, fmt.Errorf("ban: invalid host_regex: %w", err)
		}
		hostRE = re
	}
	if expr.PathRegex != "" {
		re, err := regexp.Compile(expr.PathRegex)
		if err != nil {
			return nil, fmt.Errorf("ban: invalid path_regex: %w", err)
		}
		pathRE = re
	}
	return func(obj *api.Object) bool {
		// Skip objects stored after the ban was issued.
		if !expr.CreatedAt.IsZero() && obj.StoredAt.After(expr.CreatedAt) {
			return false
		}
		if hostRE != nil && !hostRE.MatchString(obj.Header.Get("Host")) {
			return false
		}
		if pathRE != nil && !pathRE.MatchString(obj.Header.Get("X-Bouine-Path")) {
			return false
		}
		if expr.SurrogateKey != "" {
			found := false
			for _, sk := range obj.SurrogateKeys {
				if sk == expr.SurrogateKey {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}, nil
}

// matchesActiveBan reports whether obj is subject to any active lazy ban.
// Expired bans (where the ban's CreatedAt is far in the past and unlikely to
// be relevant) are pruned to bound the list size.
//
// Fast path: when no bans are active (the steady-state case), a single
// atomic load returns immediately without touching the global bansMu. This
// keeps the hot Get path free of cross-shard lock contention.
func (h *HotStore) matchesActiveBan(obj *api.Object) bool {
	if h.banCount.Load() == 0 {
		return false
	}
	h.bansMu.Lock()
	defer h.bansMu.Unlock()
	if len(h.activeBans) == 0 {
		return false
	}
	var live []activeBan
	for _, b := range h.activeBans {
		// Keep bans for 24h after creation; older bans cannot match objects
		// stored after them (StoredAt > ban.created).
		if time.Since(b.created) < 24*time.Hour {
			live = append(live, b)
		}
	}
	h.activeBans = live
	h.banCount.Store(int64(len(live)))
	for _, b := range live {
		if obj.StoredAt.After(b.created) {
			continue // object stored after ban — not subject to it
		}
		if b.pred(obj) {
			return true
		}
	}
	return false
}

// Stats returns an atomic snapshot.
func (h *HotStore) Stats() api.Stats {
	var hotEntries, hotBytes int64
	for i := range h.shards {
		s := &h.shards[i]
		s.mu.RLock()
		hotEntries += int64(len(s.entries))
		hotBytes += s.bytes
		s.mu.RUnlock()
	}
	return api.Stats{
		HotEntries: hotEntries,
		HotBytes:   hotBytes,
		Hits:       h.stats.hits.Load(),
		Misses:     h.stats.misses.Load(),
		Evictions:  h.stats.evictions.Load(),
	}
}

// Close stops the background sweeper and waits for it to exit.
func (h *HotStore) Close(_ context.Context) error {
	close(h.done)
	return nil
}

// SetWarm marks the entry for key as having a warm-tier backup. If the
// entry doesn't exist, this is a no-op. Warm-backed entries are evicted
// first under memory pressure.
func (h *HotStore) SetWarm(key api.Key) {
	s := h.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[key]; ok && !e.hasWarm {
		e.hasWarm = true
		s.warmCount++
	}
}

// evictPreferWarm selects and removes an entry from the SIEVE list,
// preferring entries with warm-tier backups. It tries up to maxSkips
// SIEVE evictions, deferring hot-only entries back into the list.
// If no warm-backed entries are found, falls back to standard eviction.
const maxEvictSkips = 4

func (s *shard) evictPreferWarm() (key api.Key, ok bool) {
	if s.warmCount == 0 {
		return s.evict.Evict()
	}
	for range maxEvictSkips {
		k, ok := s.evict.Evict()
		if !ok {
			return k, false
		}
		if he, exists := s.entries[k]; exists {
			if he.hasWarm {
				s.warmCount--
				return k, true
			}
			// Hot-only entry: put it back at the head (deferred).
			se, _ := s.evict.Access(k, func(ek api.Key) *sieve.Entry[api.Key] {
				return nil
			})
			he.sieve = se
		}
	}
	// Fall back to standard eviction after skips exhausted.
	return s.evict.Evict()
}

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
