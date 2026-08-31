package storage

import (
	"context"
	"fmt"
	"regexp"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/storage/cachaner"
	"github.com/bouine-cache/bouine/internal/storage/evictor"
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
	logger observability.Logger
	// done is closed by Close() to stop all background goroutines.
	done chan struct{}
	// evictSignal receives shard indices that need further eviction after
	// the inline cap was reached. Buffered to len(shards); non-blocking
	// sends coalesce burst signals.
	evictSignal chan int
	// onEvict is called when a backed entry is removed by a non-SIEVE
	// path. Set via HotConfig.OnEvict. See HotConfig.OnEvict for the
	// constraint.
	onEvict func(key api.Key)
	// onEvictDemoted is called when a backed entry is SIEVE-evicted.
	// Set via HotConfig.OnEvictDemoted. The TieredStore uses it to
	// Unprotect (not Delete) the warm copy — demoting it to
	// SIEVE-managed without destroying it (#484).
	onEvictDemoted func(key api.Key)
	// slab allocates body bytes from mmap'd regions to reduce GC
	// pressure. nil means use Go heap (default, backward compatible).
	slab *SlabAllocator
	// bans is the lazy ban list, stored as an atomic pointer to an
	// immutable snapshot. Ban() appends + prunes under bansMu, then
	// publishes a new slice via atomic.Store. The read path
	// (matchesActiveBan) does a lock-free atomic.Load and iterates the
	// snapshot — no lock, no allocation, no mutation on the hit path.
	// Objects stored AFTER a ban's CreatedAt are not subject to it
	// (RFC 9111 §4.4 semantics).
	bans   atomic.Pointer[[]activeBan]
	shards []shard
	stats  hotStats
	// wg tracks the sweeper and reaper goroutines so Close can wait
	// for them to fully exit before munmapping slab regions. Without
	// this, a goroutine mid-flushSlabFrees could access munmap'd
	// memory and segfault.
	wg       sync.WaitGroup
	maxBytes int64
	mask     uint64
	// reaperInterval is how often the TTL reaper wakes to scan for
	// expired entries. Zero disables background reaping.
	reaperInterval time.Duration
	bansMu         sync.Mutex
}

// activeBan is a compiled, time-stamped ban predicate in the lazy list.
type activeBan struct {
	pred    banPredicate
	created time.Time
}

// banTTL is how long a lazy ban stays in the active list. Bans older
// than this cannot match any live object: objects stored after the ban
// are exempt (StoredAt > ban.created), and objects stored before it
// have either been re-cached or expired by now. This is a bouine policy
// constant, not an RFC requirement.
const banTTL = 24 * time.Hour

type shard struct {
	entries     map[api.Key]*hotEntry
	evict       evictor.List[api.Key]
	bytes       int64
	backedCount int64 // entries with a backup (cheap to evict)
	mu          sync.RWMutex
}

type hotEntry struct {
	obj   *api.Object
	entry *evictor.Entry[api.Key]
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

// newEvictList builds a per-shard eviction list from the HotConfig's
// algorithm selection. SIEVE is the default (zero-value config). When
// HotEvictionAlgorithm == "cachaner" the list is a cachaner list that
// uses a 3-bit freq counter packed into ioBits to give hot objects up
// to 7 second chances (vs SIEVE's 1). Both implementations satisfy the
// evictor.List interface so the rest of the hot tier is agnostic to the
// active policy. The warm tier has an identical dispatch function.
func newEvictList(cfg HotConfig) evictor.List[api.Key] {
	if cfg.HotEvictionAlgorithm == "cachaner" {
		return cachaner.NewList[api.Key]()
	}
	return sieve.NewList[api.Key]()
}

// evictionLog is a deferred log record collected under the shard lock
// and flushed after the lock is released. Only unbacked evictions are
// recorded — backed evictions are benign (the warm tier retains the
// data) and are already counted by the hot_store_evictions_total
// metric, so logging them wastes ~80% of the eviction-log allocation
// budget for no operational value. Unbacked evictions signal potential
// data loss and are always logged at Warn.
type evictionLog struct {
	varyKey string
	reason  string
	size    int64
	key     api.Key
}

func (h *HotStore) flushEvictionLogs(logs []evictionLog) {
	for _, e := range logs {
		attrs := []any{
			"reason", e.reason,
			"size_bytes", e.size,
			"key", e.key,
		}
		if e.varyKey != "" {
			attrs = append(attrs, "vary_key", e.varyKey)
		}
		h.logger.Warn("evicted from hot store", attrs...)
	}
}

func recordEviction(logs *[]evictionLog, key api.Key, entry *hotEntry, reason string) {
	if entry == nil || entry.obj == nil || entry.hasBackup {
		return
	}
	*logs = append(*logs, evictionLog{
		key:     key,
		reason:  reason,
		size:    objSize(entry.obj),
		varyKey: entry.obj.VaryKey,
	})
}

type hotStats struct {
	hits      atomic.Int64
	misses    atomic.Int64
	evictions atomic.Int64
}

// evictReason classifies the caller of notifyEvict so the OnEvict
// callback can be suppressed for SIEVE-eviction-only paths (Fix A
// for #484) while remaining active for reaper/ban/Delete paths.
type evictReason int

const (
	evictReasonSIEVE  evictReason = iota // inline Put, background sweeper
	evictReasonReaper                    // reapShard — TTL expiry
	evictReasonBan                       // evictBanned, banShard
	evictReasonDelete                    // Delete, Put overwrite-replace
)

// HotConfig configures the hot store.
type HotConfig struct {
	// Logger receives eviction records. Defaults to a SampledLogger
	// wrapping slog.Default().
	Logger observability.Logger
	// OnEvict is called when a backed entry (hasBackup == true) is
	// removed from the hot tier by a non-SIEVE path (reaper, ban,
	// Delete, Put-overwrite). The key should be tombstoned in the
	// backup tier so stale objects are not served after the hot tier
	// drops them.
	//
	// OnEvict is NOT called for evictReasonSIEVE — instead
	// OnEvictDemoted is called so the TieredStore can Unprotect (not
	// Delete) the warm copy, keeping it live and SIEVE-evictable (#484).
	//
	// CONSTRAINT: This callback is invoked while the shard write lock
	// is held. It MUST NOT block, perform I/O, or call back into
	// HotStore. It may only enqueue the key for async processing.
	// Violating this constraint stalls all readers and writers on the
	// shard.
	OnEvict func(key api.Key)
	// OnEvictDemoted is called when a backed entry is SIEVE-evicted
	// from the hot tier. Instead of tombstoning the warm copy
	// (OnEvict), the TieredStore uses this callback to Unprotect the
	// warm copy — demoting it to SIEVE-managed without deleting it
	// (#484).
	//
	// CONSTRAINT: Same as OnEvict — invoked under the shard write lock.
	// Must only enqueue for async processing (the TieredStore enqueues
	// to a warmUnprotectQueue drained outside the hot lock to avoid a
	// lock-ordering cycle with warm.idxMu).
	OnEvictDemoted func(key api.Key)
	// HotEvictionAlgorithm selects the eviction policy for the hot tier.
	// "" and "sieve" (the default) use the SIEVE visited-bit sweep.
	// "cachaner" uses SIEVE with a 3-bit frequency counter that gives
	// hot objects up to 7 second chances (vs SIEVE's 1) before eviction.
	//
	// This is the resolved per-tier value: builders copy either
	// config.Storage.HotEvictionAlgorithm (when set) or the shared
	// config.Storage.EvictionAlgorithm into this field. The distinct
	// name from the shared config field keeps `grep EvictionAlgorithm`
	// unambiguous.
	HotEvictionAlgorithm string
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
	// Slab enables the mmap'd slab allocator for body bytes. When
	// true, bodies are allocated from mmap'd regions instead of Go
	// heap, reducing GC pressure. Default false (Go heap).
	Slab bool
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
		shards[i].evict = newEvictList(cfg)
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
		onEvictDemoted: cfg.OnEvictDemoted,
	}
	if cfg.Slab {
		slab, err := NewSlabAllocator()
		if err != nil {
			cfg.Logger.Warn("slab allocator init failed, falling back to Go heap", "error", err)
		} else {
			h.slab = slab
		}
	}
	h.wg.Add(2)
	go h.sweeper()
	if cfg.ReaperInterval >= 0 {
		go h.reaperLoop()
	} else {
		// Balance the Add(2) above when reaping is disabled.
		h.wg.Done()
	}
	return h
}

func (h *HotStore) shard(key api.Key) *shard {
	return &h.shards[key.Hash64()&h.mask]
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
	if e != nil && e.entry.Visited() {
		e.windowHits.Add(1)
		stored := e.obj
		ret := h.detachBody(stored)
		s.mu.RUnlock()
		if h.matchesActiveBan(ret) {
			h.evictBanned(s, key, stored)
			h.stats.misses.Add(1)
			return nil, "", nil
		}
		h.stats.hits.Add(1)
		return ret, api.SourceHot, nil
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
	var stored *api.Object
	if e != nil {
		s.evict.Access(key, func(k api.Key) *evictor.Entry[api.Key] {
			return e.entry
		})
		e.obj.Hits++
		e.windowHits.Add(1)
		stored = e.obj
	}
	var ret *api.Object
	if stored != nil {
		ret = h.detachBody(stored)
	}
	s.mu.Unlock()

	if ret == nil {
		h.stats.misses.Add(1)
		return nil, "", nil
	}
	if h.matchesActiveBan(ret) {
		h.evictBanned(s, key, stored)
		h.stats.misses.Add(1)
		return nil, "", nil
	}
	h.stats.hits.Add(1)
	return ret, api.SourceHot, nil
}

// detachBody returns a copy of obj with Body on the Go heap, safe for
// the caller to use after the shard lock is released. When slab is
// disabled (the default), it returns obj as-is — zero allocations on
// the hit path. When slab is enabled, it copies Body to the heap and
// returns a new Object to avoid use-after-free: the stored entry's
// slab-backed Body can be freed by concurrent eviction after the lock
// is released.
//
// MUST be called while holding at least a read lock on the shard —
// the body copy reads slab memory that eviction (which holds the write
// lock) could otherwise free mid-copy.
func (h *HotStore) detachBody(obj *api.Object) *api.Object {
	if h.slab == nil || len(obj.Body) == 0 {
		return obj
	}
	heapBody := make([]byte, len(obj.Body))
	copy(heapBody, obj.Body)
	return obj.CloneForReturn(heapBody)
}

// notifyEvict fires the OnEvict callback for a backed entry being
// evicted. Called while the shard write lock is held — the callback
// MUST NOT block or perform I/O. slabFrees collects slab-allocated
// bodies that must be returned to the free list after the shard lock
// is released (slab.Free takes a per-region mutex and must not be
// called under the shard lock).
func (h *HotStore) notifyEvict(key api.Key, entry *hotEntry, slabFrees *[][]byte, reason evictReason) {
	if h.slab != nil && entry.obj != nil {
		*slabFrees = append(*slabFrees, entry.obj.Body)
	}
	if !entry.hasBackup {
		return
	}
	// SIEVE evictions demote the warm copy (Unprotect) instead of
	// tombstoning it, so the warm entry stays live and SIEVE-evictable
	// under warm pressure. Non-SIEVE removals (reaper, ban, Delete,
	// Put-overwrite) still tombstone — see tiered.go's
	// warmUnprotectQueue drain path (#484).
	if reason == evictReasonSIEVE {
		if h.onEvictDemoted != nil {
			h.onEvictDemoted(key)
		}
		return
	}
	if h.onEvict != nil {
		h.onEvict(key)
	}
}

func (h *HotStore) flushSlabFrees(bodies [][]byte) {
	if h.slab == nil {
		return
	}
	h.slab.FreeBatch(bodies)
}

// evictBanned removes a ban-matching entry from the shard. It re-checks
// that the current entry is still the same object pointer to avoid
// evicting a replacement that arrived between the unlock and re-lock.
func (h *HotStore) evictBanned(s *shard, key api.Key, obj *api.Object) {
	var slabFrees [][]byte
	s.mu.Lock()
	if cur, ok := s.entries[key]; ok && cur.obj == obj {
		h.notifyEvict(key, cur, &slabFrees, evictReasonBan)
		s.bytes -= objSize(obj)
		s.evict.Remove(cur.entry)
		delete(s.entries, key)
		h.stats.evictions.Add(1)
		cur.obj = nil
		cur.entry = nil
		cur.hasBackup = false
		cur.windowHits.Store(0)
		hotEntryPool.Put(cur)
	}
	s.mu.Unlock()
	h.flushSlabFrees(slabFrees)
}

// Put stores an object. If the shard is over budget, SIEVE eviction
// runs inline for up to inlineEvictCap victims, then signals the
// background sweeper for any remaining overshoot. Entries with a backup
// are evicted first (cheap: recoverable from disk).
//
//nolint:funlen // 52 statements: eviction loop is inherently linear; extracting would harm readability
func (h *HotStore) Put(_ context.Context, key api.Key, obj *api.Object) error {
	size := objSize(obj)
	s := h.shard(key)
	shardIdx := int(key.Hash64() & h.mask) //nolint:gosec // mask < len(shards) ≤ 64, never overflows int
	perShardMax := h.maxBytes / int64(len(h.shards))

	// Move the body off the Go heap before acquiring the shard lock so
	// the per-region mutex in slab.Alloc doesn't extend shard lock hold
	// time. If the slab is full or unavailable, the stored entry keeps
	// the Go-heap body — no crash, just no GC optimization.
	//
	// We clone obj so the caller's *api.Object is not mutated: the
	// caller (TieredStore.Put) may still read obj.Body for warm-tier
	// encoding, and mutating it in place would cause a use-after-free
	// if the slab slot is later evicted before encoding finishes.
	stored := obj
	if h.slab != nil && len(obj.Body) > 0 {
		slabBuf := h.slab.Alloc(len(obj.Body))
		if slabBuf != nil {
			copy(slabBuf, obj.Body)
			stored = obj.CloneForReturn(slabBuf)
		}
	}

	var logs []evictionLog
	var slabFrees [][]byte
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
			h.notifyEvict(evKey, old, &slabFrees, evictReasonSIEVE)
			s.bytes -= objSize(old.obj)
			delete(s.entries, evKey)
			h.stats.evictions.Add(1)
			old.obj = nil
			old.entry = nil
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
		h.notifyEvict(key, old, &slabFrees, evictReasonDelete)
		s.bytes -= objSize(old.obj)
		s.evict.Remove(old.entry)
		if old.hasBackup {
			s.backedCount--
		}
		old.obj = nil
		old.entry = nil
		old.hasBackup = false
		old.windowHits.Store(0)
		hotEntryPool.Put(old)
	}

	se, _ := s.evict.Access(key, func(k api.Key) *evictor.Entry[api.Key] {
		return nil // force insert
	})
	se.MarkVisited() // one sweep of protection on insert (#484)
	e := hotEntryPool.Get().(*hotEntry)
	e.obj = stored
	e.entry = se
	s.entries[key] = e
	s.bytes += size
	s.mu.Unlock()
	h.flushEvictionLogs(logs)
	h.flushSlabFrees(slabFrees)

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
	defer h.wg.Done()
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
	var slabFrees [][]byte
	s.mu.Lock()

	deadline := time.Now().Add(reaperShardBudget)
	for key, e := range s.entries {
		if time.Now().After(deadline) {
			break
		}
		expiry := e.obj.StoredAt.Add(e.obj.TTL + e.obj.StaleWhileRevalidate + e.obj.StaleIfError)
		if now.After(expiry) {
			recordEviction(&logs, key, e, "expired")
			h.notifyEvict(key, e, &slabFrees, evictReasonReaper)
			s.bytes -= objSize(e.obj)
			s.evict.Remove(e.entry)
			delete(s.entries, key)
			h.stats.evictions.Add(1)
			e.obj = nil
			e.entry = nil
			e.hasBackup = false
			e.windowHits.Store(0)
			hotEntryPool.Put(e)
		}
	}
	s.mu.Unlock()
	h.flushEvictionLogs(logs)
	h.flushSlabFrees(slabFrees)
}

// sweeper is the background goroutine that drains overshoot evictions
// signalled by Put. It terminates when Close() is called.
func (h *HotStore) sweeper() {
	defer h.wg.Done()
	perShardMax := h.maxBytes / int64(len(h.shards))
	for {
		select {
		case <-h.done:
			return
		case idx := <-h.evictSignal:
			s := &h.shards[idx]
			var logs []evictionLog
			var slabFrees [][]byte
			s.mu.Lock()
			for s.bytes > perShardMax && s.evict.Len() > 0 {
				evKey, ok := s.evictPreferBacked()
				if !ok {
					break
				}
				if old, exists := s.entries[evKey]; exists {
					recordEviction(&logs, evKey, old, "sweeper_overshoot")
					h.notifyEvict(evKey, old, &slabFrees, evictReasonSIEVE)
					s.bytes -= objSize(old.obj)
					delete(s.entries, evKey)
					h.stats.evictions.Add(1)
					old.obj = nil
					old.entry = nil
					old.hasBackup = false
					old.windowHits.Store(0)
					hotEntryPool.Put(old)
				}
			}
			s.mu.Unlock()
			h.flushEvictionLogs(logs)
			h.flushSlabFrees(slabFrees)
		}
	}
}

// Has reports whether key is present in the hot tier, without side
// effects. Unlike Get, it does not increment hit counters, mark SIEVE
// entries as visited, or touch windowHits.
func (h *HotStore) Has(key api.Key) bool {
	s := h.shard(key)
	s.mu.RLock()
	_, ok := s.entries[key]
	s.mu.RUnlock()
	return ok
}

// Delete removes a key from the hot tier.
func (h *HotStore) Delete(_ context.Context, key api.Key) error {
	s := h.shard(key)
	var slabFrees [][]byte
	s.mu.Lock()
	if e, ok := s.entries[key]; ok {
		s.bytes -= objSize(e.obj)
		s.evict.Remove(e.entry)
		h.notifyEvict(key, e, &slabFrees, evictReasonDelete)
		delete(s.entries, key)
		e.obj = nil
		e.entry = nil
		e.hasBackup = false
		e.windowHits.Store(0)
		hotEntryPool.Put(e)
	}
	s.mu.Unlock()
	h.flushSlabFrees(slabFrees)
	return nil
}

// Ban performs an eager scan of the hot tier, deleting all entries
// whose stored object matches the ban predicate. After the eager
// scan, the predicate is registered in the lazy ban list so objects
// filled after this scan are also checked on next lookup (RFC 9111
// §4.4 lazy semantics). This handles the common case of
// host/path/surrogate-key invalidation on administrative APIs.
func (h *HotStore) Ban(_ context.Context, expr api.BanExpr) (int, error) {
	pred, err := compileBanPredicate(expr)
	if err != nil {
		return 0, err
	}

	var total atomic.Int64
	var g errgroup.Group
	g.SetLimit(min(len(h.shards), runtime.NumCPU()))
	for i := range h.shards {
		g.Go(func() error {
			n, err := h.banShard(i, pred)
			if err != nil {
				return err
			}
			total.Add(int64(n))
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return 0, err
	}

	// Register in the lazy ban list so objects filled AFTER this scan
	// are also checked on next lookup (RFC 9111 §4.4 lazy semantics).
	createdAt := expr.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	h.bansMu.Lock()
	cur := h.bans.Load()
	var base []activeBan
	if cur != nil {
		base = *cur
	}
	updated := make([]activeBan, 0, len(base)+1)
	now := time.Now()
	for _, b := range base {
		if now.Sub(b.created) < banTTL {
			updated = append(updated, b)
		}
	}
	updated = append(updated, activeBan{pred: pred, created: createdAt})
	h.bans.Store(&updated)
	h.bansMu.Unlock()
	return int(total.Load()), nil
}

// banShard locks one shard, evicts all entries matching pred, and
// returns the count. Each worker scans exactly one shard — no
// cross-shard lock ordering, no deadlock risk. Fires notifyEvict for
// each evicted entry, matching the inline eviction and reaper paths.
// The error return is always nil today but is plumbed through
// errgroup so future fallible operations propagate correctly.
func (h *HotStore) banShard(idx int, pred banPredicate) (int, error) { //nolint:unparam // error return reserved for future fallible ops
	s := &h.shards[idx]
	n := 0
	var slabFrees [][]byte
	s.mu.Lock()
	for key, e := range s.entries {
		if pred(e.obj) {
			h.notifyEvict(key, e, &slabFrees, evictReasonBan)
			s.bytes -= objSize(e.obj)
			s.evict.Remove(e.entry)
			delete(s.entries, key)
			h.stats.evictions.Add(1)
			n++
			e.obj = nil
			e.entry = nil
			e.hasBackup = false
			e.windowHits.Store(0)
			hotEntryPool.Put(e)
		}
	}
	s.mu.Unlock()
	h.flushSlabFrees(slabFrees)
	return n, nil
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

// matchesActiveBan reports whether obj is subject to any active lazy
// ban. It loads the immutable ban snapshot via atomic.Pointer (lock-
// free, zero allocation) and iterates it. Pruning of expired bans
// happens in Ban(), not on the read path.
func (h *HotStore) matchesActiveBan(obj *api.Object) bool {
	cur := h.bans.Load()
	if cur == nil || len(*cur) == 0 {
		return false
	}
	for _, b := range *cur {
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
	h.wg.Wait()
	if h.slab != nil {
		_ = h.slab.Close()
	}
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
// SIEVE evictions, re-inserting hot-only entries at the head for a
// second chance (via Access + MarkVisited).
// If no backed entries are found, falls back to standard eviction.
const maxEvictSkips = 4

// maxSweepProbes caps the number of SIEVE entries scanned per Evict
// call in the hot tier. Under heavy read load all entries have
// visited=true, making the unbounded sweep O(N). The cap bounds the
// worst case at ~128 pointer chases (~50 us) instead of 2N (~2+ ms
// at 1 M entries). See ADR 0026.
const maxSweepProbes = 128

func (s *shard) evictPreferBacked() (key api.Key, ok bool) {
	if s.backedCount == 0 {
		return s.evict.EvictBounded(maxSweepProbes)
	}
	for range maxEvictSkips {
		k, ok := s.evict.EvictBounded(maxSweepProbes)
		if !ok {
			return k, false
		}
		if he, exists := s.entries[k]; exists {
			if he.hasBackup {
				s.backedCount--
				return k, true
			}
			// Hot-only entry: re-insert at the head to give it another
			// chance. EvictBounded already removed the entry from the
			// list and returned it to the pool, so we must re-insert via
			// Access (not Defer, which assumes the entry is still linked
			// in the list — calling Defer on an unlinked entry corrupts
			// head/tail and orphans every other entry).
			//
			// Access pulls a fresh entry from the pool via Reset, which
			// clears visited to false. Mark it visited so the re-inserted
			// entry gets a full second chance (one sweep to clear visited,
			// a second to evict) — matching the semantic the old Defer
			// path provided by preserving the visited bit.
			he.entry, _ = s.evict.Access(k, func(api.Key) *evictor.Entry[api.Key] { return nil })
			he.entry.MarkVisited()
		}
	}
	// Fall back to standard eviction after skips exhausted.
	return s.evict.EvictBounded(maxSweepProbes)
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

// HotOnlyKeys returns up to limit hot-tier keys that are not backed by
// a slower tier, starting at offset % total. Also returns the total
// hot-only count so callers can advance their rotation offset without a
// separate scan. The returned keys are unsorted.
//
// This iterates s.entries and filters by !hasBackup, trading O(N) scan
// cost on the cold warm-sync path for zero map overhead on the hot Put
// path. Each shard is locked individually with a read lock.
func (h *HotStore) HotOnlyKeys(offset, limit int) ([]api.Key, int) {
	if limit <= 0 {
		return nil, 0
	}

	// First pass: count total hot-only entries across shards.
	var total int
	for i := range h.shards {
		s := &h.shards[i]
		s.mu.RLock()
		for _, e := range s.entries {
			if !e.hasBackup {
				total++
			}
		}
		s.mu.RUnlock()
	}
	if total == 0 {
		return nil, 0
	}

	// Normalize offset.
	offset %= total
	if offset < 0 {
		offset += total
	}

	// Second pass: collect up to limit keys, starting at offset.
	keys := make([]api.Key, 0, min(limit, total))
	skipped := 0
	needed := limit
	for i := range h.shards {
		if needed <= 0 {
			break
		}
		s := &h.shards[i]
		s.mu.RLock()
		for k, e := range s.entries {
			if e.hasBackup {
				continue
			}
			if skipped < offset {
				skipped++
				continue
			}
			keys = append(keys, k)
			needed--
			if needed <= 0 {
				break
			}
		}
		s.mu.RUnlock()
	}

	return keys, total
}

const (
	objectStructSize    int64 = 320 // unsafe.Sizeof(api.Object{}) — 312 → 320: composedHeadPtr, the per-second fast-path response-head cache (PR #567). Update when fields are added.
	hotEntrySize        int64 = 32
	sieveEntrySize      int64 = 40 // unsafe.Sizeof(evictor.Entry[api.Key]{}): 16B key + 4B atomic.Bool + 4B pad + 8B prev + 8B next
	mapPerEntryOverhead int64 = 32 // 8-slot bucket = 208 B at load factor 6.5 (16B keys) → ~32 B/entry. hmap header negligible at 1M+ entries.
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
	size += int64(len(obj.VaryValue))
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
