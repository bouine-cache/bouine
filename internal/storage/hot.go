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

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/storage/sieve"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// inlineEvictCap is the maximum number of entries evicted synchronously
// inside Put while holding the shard write lock. Additional over-budget
// bytes are reclaimed asynchronously by the background sweeper (Phase 2b).
// This bounds the worst-case lock hold time on the Put path.
const inlineEvictCap = 4

// defaultReaperInterval is how often the TTL reaper scans shards for
// expired entries. Overridable via HotConfig.ReaperInterval.
const defaultReaperInterval = 30 * time.Second

// reaperShardBudget caps the wall-clock time spent holding the write
// lock on a single shard during a TTL reaper pass.
const reaperShardBudget = 10 * time.Millisecond

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
	logger   observability.Logger
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
	// reaperInterval is how often the TTL reaper wakes to scan for
	// expired entries. Zero disables background reaping.
	reaperInterval time.Duration
	// done is closed by Close() to stop all background goroutines.
	done chan struct{}
	// onEvict is called when a backed entry is evicted. Set via
	// HotConfig.OnEvict. See HotConfig.OnEvict for the constraint.
	onEvict func(key api.Key)
}

// activeBan is a compiled, time-stamped ban predicate in the lazy list.
type activeBan struct {
	pred    banPredicate
	created time.Time
}

type shard struct {
	mu          sync.RWMutex
	entries     map[api.Key]*hotEntry
	evict       *sieve.List[api.Key]
	bytes       int64
	backedCount int64 // entries with a backup (cheap to evict)
}

type hotEntry struct {
	obj   *api.Object
	sieve *sieve.Entry[api.Key]
	// hasBackup is true when the object also exists in a slower tier.
	// Eviction prefers these entries because they can be recovered
	// from disk.
	hasBackup bool
	// windowHits is a per-TTL-window hit counter, incremented on every
	// Get (both fast and slow paths). Unlike Object.Hits (which only
	// increments on the SIEVE slow path), this is a true access count.
	// Reset to 0 on every Put (new hotEntry created). Used by the cache
	// layer's refresh popularity gate.
	windowHits atomic.Int64
}

// hotEntryPool reuses hotEntry structs across Put/Delete cycles to avoid
// per-Put allocation. Reset on acquire; not returned to pool if the
// entry's windowHits counter is non-zero (shouldn't happen since Delete
// clears it, but defensive).
var hotEntryPool = sync.Pool{
	New: func() any { return new(hotEntry) },
}

// evictionLog is a deferred log record collected under the shard lock
// and flushed after the lock is released. Backed evictions are
// Info (sampled by key); evictions without a backup are Warn
// (never sampled — signals potential data loss).
type evictionLog struct {
	key       api.Key
	reason    string
	hadBackup bool
	size      int64
	varyKey   string
}

func (h *HotStore) flushEvictionLogs(logs []evictionLog) {
	for _, e := range logs {
		attrs := []any{
			"reason", e.reason,
			"had_backup", e.hadBackup,
			"size_bytes", e.size,
			"key", e.key,
		}
		if e.varyKey != "" {
			attrs = append(attrs, "vary_key", e.varyKey)
		}
		if e.hadBackup {
			h.logger.Info("evicted from hot store", attrs...)
		} else {
			h.logger.Warn("evicted from hot store", attrs...)
		}
	}
}

func recordEviction(logs *[]evictionLog, key api.Key, entry *hotEntry, reason string) {
	if entry == nil || entry.obj == nil {
		return
	}
	*logs = append(*logs, evictionLog{
		key:       key,
		reason:    reason,
		hadBackup: entry.hasBackup,
		size:      objSize(entry.obj),
		varyKey:   entry.obj.VaryKey,
	})
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
	// ReaperInterval controls how often the background TTL reaper scans
	// shards for entries past TTL + SWR + SIE. Zero means use the
	// default (30 s). A negative value disables background reaping
	// entirely (lazy expiry on Get remains).
	ReaperInterval time.Duration
	// Logger receives eviction records. Defaults to a SampledLogger
	// wrapping slog.Default().
	Logger observability.Logger
	// OnEvict is called when a backed entry (hasBackup == true) is
	// evicted from the hot tier. The key should be tombstoned in the
	// backup tier so stale unpopular objects are not served after
	// restart.
	//
	// CONSTRAINT: This callback is invoked while the shard write lock
	// is held. It MUST NOT block, perform I/O, or call back into
	// HotStore. It may only enqueue the key for async processing.
	// Violating this constraint stalls all readers and writers on the
	// shard.
	OnEvict func(key api.Key)
}

// NewHotStore creates a sharded in-memory store and starts the
// background eviction sweeper.
func NewHotStore(cfg HotConfig) *HotStore {
	cfg.Logger = observability.ResolveLogger(cfg.Logger)
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
	reaperInterval := defaultReaperInterval
	if cfg.ReaperInterval > 0 {
		reaperInterval = cfg.ReaperInterval
	}
	h := &HotStore{
		shards:         shards,
		mask:           uint64(n - 1), //nolint:gosec // n is always a positive power of two
		maxBytes:       cfg.MaxBytes,
		evictSignal:    make(chan int, n),
		reaperInterval: reaperInterval,
		done:           make(chan struct{}),
		logger:         cfg.Logger,
		onEvict:        cfg.OnEvict,
	}
	go h.sweeper()
	if cfg.ReaperInterval >= 0 {
		go h.reaperLoop()
	}
	return h
}

func (h *HotStore) shard(key api.Key) *shard {
	return &h.shards[uint64(key)&h.mask]
}

// Get looks up key in the hot tier. Returns the object and api.SourceHot
// on a hit, or nil + empty source on a miss (a miss is not an error —
// it is a normal control-flow outcome). Bans are checked lazily.
//
// Fast path (visited bit already set): acquires only a read lock,
// avoiding write-lock contention under concurrent read-heavy workloads.
// Slow path (visited=false, i.e. first access after eviction hand sweep):
// upgrades to a write lock to set the bit.
func (h *HotStore) Get(_ context.Context, key api.Key) (*api.Object, api.Source, error) {
	s := h.shard(key)

	// Fast path: read lock. If the entry exists and its visited bit is
	// already set, no write is needed — just return the object.
	s.mu.RLock()
	e := s.entries[key]
	if e != nil && e.sieve.Visited() {
		e.windowHits.Add(1)
		obj := e.obj
		s.mu.RUnlock()
		if h.matchesActiveBan(obj) {
			h.evictBanned(s, key, obj)
			h.stats.misses.Add(1)
			return nil, "", nil
		}
		h.stats.hits.Add(1)
		return obj, api.SourceHot, nil
	}
	s.mu.RUnlock()

	if e == nil {
		h.stats.misses.Add(1)
		return nil, "", nil
	}

	// Slow path: visited bit is false. Upgrade to write lock, re-check
	// (entry may have been evicted between RUnlock and Lock), then set it.
	s.mu.Lock()
	e = s.entries[key]
	var obj *api.Object
	if e != nil {
		s.evict.Access(key, func(k api.Key) *sieve.Entry[api.Key] {
			return e.sieve
		})
		e.obj.Hits++
		e.windowHits.Add(1)
		obj = e.obj
	}
	s.mu.Unlock()

	if obj == nil {
		h.stats.misses.Add(1)
		return nil, "", nil
	}
	if h.matchesActiveBan(obj) {
		h.evictBanned(s, key, obj)
		h.stats.misses.Add(1)
		return nil, "", nil
	}
	h.stats.hits.Add(1)
	return obj, api.SourceHot, nil
}

// notifyEvict fires the OnEvict callback for a backed entry being
// evicted. Called while the shard write lock is held — the callback
// MUST NOT block or perform I/O.
func (h *HotStore) notifyEvict(key api.Key, entry *hotEntry) {
	if h.onEvict != nil && entry.hasBackup {
		h.onEvict(key)
	}
}

// evictBanned removes a ban-matching entry from the shard. It re-checks
// that the current entry is still the same object pointer to avoid
// evicting a replacement that arrived between the unlock and re-lock.
func (h *HotStore) evictBanned(s *shard, key api.Key, obj *api.Object) {
	s.mu.Lock()
	if cur, ok := s.entries[key]; ok && cur.obj == obj {
		h.notifyEvict(key, cur)
		s.bytes -= objSize(obj)
		s.evict.Remove(cur.sieve)
		delete(s.entries, key)
		h.stats.evictions.Add(1)
		cur.obj = nil
		cur.sieve = nil
		cur.hasBackup = false
		cur.windowHits.Store(0)
		hotEntryPool.Put(cur)
	}
	s.mu.Unlock()
}

// Put stores an object. If the shard is over budget, SIEVE eviction
// runs inline for up to inlineEvictCap victims, then signals the
// background sweeper for any remaining overshoot. Entries with a backup
// are evicted first (cheap: recoverable from disk).
func (h *HotStore) Put(_ context.Context, key api.Key, obj *api.Object) error {
	size := objSize(obj)
	s := h.shard(key)
	shardIdx := int(uint64(key) & h.mask) //nolint:gosec // mask < len(shards) ≤ 64, never overflows int
	perShardMax := h.maxBytes / int64(len(h.shards))

	var logs []evictionLog
	s.mu.Lock()
	stillOver := false
	for range inlineEvictCap {
		if s.bytes+size <= perShardMax || s.evict.Len() == 0 {
			break
		}
		evKey, ok := s.evictPreferBacked()
		if !ok {
			break
		}
		if old, exists := s.entries[evKey]; exists {
			recordEviction(&logs, evKey, old, "inline_overshoot")
			h.notifyEvict(evKey, old)
			s.bytes -= objSize(old.obj)
			delete(s.entries, evKey)
			h.stats.evictions.Add(1)
			old.obj = nil
			old.sieve = nil
			old.hasBackup = false
			old.windowHits.Store(0)
			hotEntryPool.Put(old)
		}
	}
	if s.bytes+size > perShardMax {
		stillOver = true
	}

	// Remove old entry if replacing, return to pool.
	if old, exists := s.entries[key]; exists {
		h.notifyEvict(key, old)
		s.bytes -= objSize(old.obj)
		s.evict.Remove(old.sieve)
		if old.hasBackup {
			s.backedCount--
		}
		old.obj = nil
		old.sieve = nil
		old.hasBackup = false
		old.windowHits.Store(0)
		hotEntryPool.Put(old)
	}

	se, _ := s.evict.Access(key, func(k api.Key) *sieve.Entry[api.Key] {
		return nil // force insert
	})
	e := hotEntryPool.Get().(*hotEntry)
	e.obj = obj
	e.sieve = se
	s.entries[key] = e
	s.bytes += size
	s.mu.Unlock()
	h.flushEvictionLogs(logs)

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

// reaperLoop periodically scans all shards for entries that have
// exceeded TTL + SWR + SIE and removes them. This prevents dead entries
// from accumulating indefinitely when they are never accessed again.
func (h *HotStore) reaperLoop() {
	ticker := time.NewTicker(h.reaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-h.done:
			return
		case <-ticker.C:
			h.reapExpired(time.Now())
		}
	}
}

// reapExpired scans all shards and removes entries whose TTL + SWR + SIE
// has elapsed. Each shard is locked individually and for at most
// reaperShardBudget to avoid blocking readers.
func (h *HotStore) reapExpired(now time.Time) {
	for i := range h.shards {
		h.reapShard(i, now)
	}
}

func (h *HotStore) reapShard(idx int, now time.Time) {
	s := &h.shards[idx]
	var logs []evictionLog
	s.mu.Lock()

	deadline := time.Now().Add(reaperShardBudget)
	for key, e := range s.entries {
		if time.Now().After(deadline) {
			break
		}
		expiry := e.obj.StoredAt.Add(e.obj.TTL + e.obj.StaleWhileRevalidate + e.obj.StaleIfError)
		if now.After(expiry) {
			recordEviction(&logs, key, e, "expired")
			h.notifyEvict(key, e)
			s.bytes -= objSize(e.obj)
			s.evict.Remove(e.sieve)
			delete(s.entries, key)
			h.stats.evictions.Add(1)
			e.obj = nil
			e.sieve = nil
			e.hasBackup = false
			e.windowHits.Store(0)
			hotEntryPool.Put(e)
		}
	}
	s.mu.Unlock()
	h.flushEvictionLogs(logs)
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
			var logs []evictionLog
			s.mu.Lock()
			for s.bytes > perShardMax && s.evict.Len() > 0 {
				evKey, ok := s.evictPreferBacked()
				if !ok {
					break
				}
				if old, exists := s.entries[evKey]; exists {
					recordEviction(&logs, evKey, old, "sweeper_overshoot")
					h.notifyEvict(evKey, old)
					s.bytes -= objSize(old.obj)
					delete(s.entries, evKey)
					h.stats.evictions.Add(1)
				}
			}
			s.mu.Unlock()
			h.flushEvictionLogs(logs)
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
		e.obj = nil
		e.sieve = nil
		e.hasBackup = false
		e.windowHits.Store(0)
		hotEntryPool.Put(e)
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
				e.obj = nil
				e.sieve = nil
				e.hasBackup = false
				e.windowHits.Store(0)
				hotEntryPool.Put(e)
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
		if hostRE != nil && !hostRE.MatchString(obj.Header.Get(header.XBouineHost)) {
			return false
		}
		if pathRE != nil && !pathRE.MatchString(obj.Header.Get(header.XBouinePath)) {
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

// MatchesActiveBan reports whether obj is subject to any active lazy
// ban. It is exported so the tiered store can check warm-tier hits
// against the same ban list used by the hot tier.
func (h *HotStore) MatchesActiveBan(obj *api.Object) bool {
	return h.matchesActiveBan(obj)
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

// WindowHits returns the per-window hit count for key, or 0 if the key
// is not in the hot tier. Called by the cache layer during refresh to
// evaluate the popularity gate. Objects in the warm tier only (not in
// hot) return 0 — they are not being actively served from the hot tier.
func (h *HotStore) WindowHits(key api.Key) int64 {
	s := h.shard(key)
	s.mu.RLock()
	e := s.entries[key]
	var n int64
	if e != nil {
		n = e.windowHits.Load()
	}
	s.mu.RUnlock()
	return n
}

// Close stops the background sweeper and waits for it to exit.
func (h *HotStore) Close(_ context.Context) error {
	close(h.done)
	return nil
}

// OverBudget reports whether the hot tier exceeds its configured byte
// budget. Used by TieredStore.OverBudget to skip warm→hot promotion
// under memory pressure (#175).
func (h *HotStore) OverBudget() bool {
	return h.Stats().HotBytes > h.maxBytes
}

// SetBacked marks the entry for key as having a backup in a slower tier.
// If the entry doesn't exist, this is a no-op. Backed entries are evicted
// first under memory pressure.
func (h *HotStore) SetBacked(key api.Key) {
	s := h.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[key]; ok && !e.hasBackup {
		e.hasBackup = true
		s.backedCount++
	}
}

// ClearBacked unmarks the entry for key as having a backup. Called
// when the backup tier evicts the key so the hot tier stops preferring it
// for eviction (it's no longer cheap to evict — there's no disk backup).
func (h *HotStore) ClearBacked(key api.Key) {
	s := h.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[key]; ok && e.hasBackup {
		e.hasBackup = false
		s.backedCount--
	}
}

// evictPreferBacked selects and removes an entry from the SIEVE list,
// preferring entries with a backup. It tries up to maxSkips
// SIEVE evictions, deferring hot-only entries back into the list.
// If no backed entries are found, falls back to standard eviction.
const maxEvictSkips = 4

func (s *shard) evictPreferBacked() (key api.Key, ok bool) {
	if s.backedCount == 0 {
		return s.evict.Evict()
	}
	for range maxEvictSkips {
		k, ok := s.evict.Evict()
		if !ok {
			return k, false
		}
		if he, exists := s.entries[k]; exists {
			if he.hasBackup {
				s.backedCount--
				return k, true
			}
			// Hot-only entry: defer it back to the head without
			// resetting the visited bit.
			s.evict.Defer(he.sieve)
		}
	}
	// Fall back to standard eviction after skips exhausted.
	return s.evict.Evict()
}

// Keys returns all cache keys currently stored in the hot tier.
// The returned slice is unsorted; callers that need determinism must
// sort it.
func (h *HotStore) Keys() []api.Key {
	var totalEntries int
	for i := range h.shards {
		s := &h.shards[i]
		s.mu.RLock()
		totalEntries += len(s.entries)
		s.mu.RUnlock()
	}
	keys := make([]api.Key, 0, totalEntries)
	for i := range h.shards {
		s := &h.shards[i]
		s.mu.RLock()
		for k := range s.entries {
			keys = append(keys, k)
		}
		s.mu.RUnlock()
	}
	return keys
}

// KeyHash computes the canonical cache key from a byte slice.
func KeyHash(b []byte) api.Key {
	return api.Key(xxhash.Sum64(b))
}

const (
	objectStructSize    int64 = 256 // unsafe.Sizeof(api.Object{}) — update when fields are added
	hotEntrySize        int64 = 32
	sieveEntrySize      int64 = 32
	mapPerEntryOverhead int64 = 22 // 8-slot bucket = 144 B at load factor 6.5 → ~22 B/entry. hmap header (~96 B) negligible at 1M+ entries.
	// Map has two slice headers: entries ([]headerEntry) and values ([]string).
	headerEntriesSlice int64 = 24 // []headerEntry slice header
	headerValuesSlice  int64 = 24 // []string slice header
	headerEntrySize    int64 = 24 // headerEntry struct (key string header 16B + off int 8B)
	headerValueHeader  int64 = 16 // string header in values slice (ptr + len)
)

func objSize(obj *api.Object) int64 {
	size := int64(len(obj.Body)) +
		objectStructSize + hotEntrySize + sieveEntrySize + mapPerEntryOverhead

	// Map: two slice headers + per-entry overhead (headerEntry 24B + value
	// string header 16B) + value data bytes. Footprint counts orphaned value
	// slots from Del so objSize accounts for their heap cost.
	entries, valueSlots, valueBytes := obj.Header.Footprint()
	size += headerEntriesSlice + headerValuesSlice +
		headerEntrySize*int64(entries) +
		headerValueHeader*int64(valueSlots) +
		int64(valueBytes)

	size += int64(len(obj.VaryKey))
	size += int64(len(obj.ETag))
	size += int64(len(obj.CacheControl))
	for _, sk := range obj.SurrogateKeys {
		size += int64(len(sk))
	}

	return size
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
