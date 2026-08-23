// Package warm implements the L1 warm-tier disk storage for bouine.
//
// The warm tier stores cache objects in append-only segmented files.
// Each segment is a fixed-size file (default 64 MiB) containing a
// sequence of records. Segment files are read via mmap during scans
// (startup, compaction); writes use regular file I/O. Each record has
// a CRC32C footer for integrity validation.
//
// Record layout (little-endian):
//
//	[4]  magic      0x424F5549 ("BOUI")
//	[16] key        api.Key (128-bit)
//	[4]  body_len   uint32
//	[N]  body       []byte
//	[4]  crc32c     checksum of (magic + key + body_len + body)
//
// Tombstones are written as records with body_len=0 and a special
// magic 0x44454144 ("DEAD"). Compaction rewrites live records into a
// new segment, skipping tombstones and keys that have been superseded.
//
// The warm tier serializes concurrent access internally: s.mu protects the
// segment list and s.idxMu protects the index. Compact holds s.mu.Lock for
// its entire duration to prevent concurrent Put/Delete from creating segments
// or index entries that the swap would silently drop (issue #280).
// CompactSegment (per-segment incremental compaction, issue #499) scans
// non-active segments under s.mu.RLock and only holds s.mu.Lock for the
// millisecond-scale swap phase, allowing concurrent Get/Put/Delete during
// the scan.
package warm

import (
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"

	"github.com/bouine-cache/bouine/internal/platform"
	"github.com/bouine-cache/bouine/internal/storage/cachaner"
	"github.com/bouine-cache/bouine/internal/storage/evictor"
	"github.com/bouine-cache/bouine/internal/storage/sieve"
	"github.com/bouine-cache/bouine/pkg/api"
)

const (
	magicLive uint32 = 0x424F5549 // "BOUI"
	magicDead uint32 = 0x44454144 // "DEAD"
	// HeaderLen is the on-disk warm record header size in bytes
	// (magic 4 + key 16 + body_len 4). It is part of the wire format
	// and exported so tests and tooling can compute record sizes
	// without duplicating the layout.
	HeaderLen = 4 + 16 + 4
	// FooterLen is the on-disk warm record footer size in bytes
	// (crc32c). Exported alongside HeaderLen for the same reason.
	FooterLen = 4
	segExt    = ".seg"
	// maxWarmEvictSkips bounds the number of protected entries SIEVE
	// may skip while searching for a non-protected victim. This keeps
	// eviction O(1) worst case under idxMu even when most entries are
	// protected (the steady state once the warm sync loop has marked
	// the majority of entries). Mirrors the hot tier's maxEvictSkips.
	maxWarmEvictSkips = 16
	// maxSweepProbes caps the number of SIEVE entries scanned per
	// Evict call in the warm tier. Under heavy read load all entries
	// have visited=true, making the unbounded sweep O(N). The cap
	// bounds the worst case at ~256 pointer chases instead of 2N.
	// See ADR 0026.
	maxSweepProbes = 256
)

// EstimatedWarmLocHeapBytes is the approximate Go heap cost per warm
// index entry: warmLoc struct (segID int + offset int64 + size int64 +
// entry pointer 8B + protected bool padded to 8B = ~40B, unchanged — the
// Key lives in the map, not warmLoc) + map overhead (~58B at load factor
// 6.5 with 16-byte keys) + evictor.Entry[api.Key] (40B: 16B key + 4B
// atomic.Bool + 4B pad + 8B prev + 8B next) = ~138B. Rounded to 160 for
// alignment and safety margin.
// Update if warmLoc or evictor.Entry struct layout changes.
// The same value is inlined in config/loader.go ResolveWarmMaxEntries
// to avoid a circular import.
const EstimatedWarmLocHeapBytes = 160

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// pwritevFn is the scatter-gather write function used by writeRecordAt.
// It defaults to platform.Pwritev; tests override it to simulate short
// writes and I/O errors without a real pwritev syscall. Package-level
// mutable state (AGENTS.md §2.3 exception): matches the existing
// sync.Pool pattern; only written by tests, never at runtime.
var pwritevFn = platform.Pwritev

// recordHdrPool pools the fixed-size 24-byte header buffer used by
// readRecordAt. The header is read, parsed, and discarded on every
// warm-tier read; without pooling this is a per-call heap allocation
// on the peer-fetch hot path (issue #187, fix #4).
//
// Package-level mutable state (AGENTS.md §2.3 exception): sync.Pool is
// concurrency-safe and matches the existing crcTable pattern. A per-store
// pool would add indirection without benefit since all stores share the
// same fixed buffer sizes.
var recordHdrPool = sync.Pool{
	New: func() any {
		buf := make([]byte, HeaderLen)
		return &buf
	},
}

// recordFootPool pools the fixed-size 4-byte footer buffer used by
// readRecordAt. Same rationale as recordHdrPool.
var recordFootPool = sync.Pool{
	New: func() any {
		buf := make([]byte, FooterLen)
		return &buf
	},
}

// ErrTornRecord indicates a partial trailing record — the segment was
// truncated mid-write (torn write). Callers should treat the affected
// index entry as stale (drop it, return a miss) rather than surfacing
// a hard error, mirroring wal.Replay's handling of partial records.
var ErrTornRecord = errors.New("warm: torn trailing record")

// ErrOverBudget indicates that appending a record would push total
// live entry bytes across the configured MaxBytes limit. Callers can
// handle this by evicting objects, triggering compaction, or simply
// skipping the warm-tier write (the acceleration tier already holds the
// object).
var ErrOverBudget = errors.New("warm: over maxBytes budget")

// errSegFull is returned by activeSegRLocked when the current active
// segment is full and a new one must be created. The caller releases
// s.mu.RLock, calls newSegment (which acquires s.mu.Lock), then
// re-acquires s.mu.RLock and retries.
var errSegFull = errors.New("warm: active segment full")

// Record is a single warm-tier entry read from a segment.
type Record struct {
	Key    api.Key
	Body   []byte
	IsTomb bool
	Offset int64
	SegID  int
}

// mmapRef wraps the mmap'd []byte so it can be stored in an atomic.Pointer.
// Allocated once per segment (in tryMmap), never mutated after creation.
type mmapRef struct {
	//nolint:unused // accessed only in warm_mmap_linux.go (build-tag split)
	data []byte
}

// Segment is an append-only file on disk.
type Segment struct {
	ID       int
	Path     string
	mu       sync.Mutex
	f        *os.File
	size     int64
	maxBytes int64
	opened   atomic.Bool
	// readers counts in-flight read operations using this segment's fd.
	// The fdCache checks this before evicting an open segment — a
	// non-zero count means a read is in progress and the fd cannot be
	// closed safely.
	readers atomic.Int32
	// mmap is a persistent MAP_SHARED mapping of the segment file used
	// for zero-syscall point reads on inactive (sealed) segments. It is
	// an atomic.Pointer for race-free access from multiple Get goroutines
	// holding s.mu.RLock. The mapping stays valid after the fd is closed
	// by fdCache eviction (POSIX guarantee); only Compact and Close munmap
	// (under s.mu.Lock, which excludes all RLock holders). nil on non-Linux,
	// before initialization, or while the segment is active.
	mmap atomic.Pointer[mmapRef]
}

// ensureOpen opens the segment file if not already open. Must be called
// while s.mu (Store-level) is held to prevent Compact from swapping the
// segment set mid-open.
func (seg *Segment) ensureOpen() error {
	if seg.opened.Load() {
		return nil
	}
	seg.mu.Lock()
	defer seg.mu.Unlock()
	return seg.openLocked()
}

// openLocked opens the segment file if not already open. The caller
// MUST hold seg.mu.
func (seg *Segment) openLocked() error {
	if seg.f != nil {
		return nil
	}
	f, err := os.OpenFile(seg.Path, os.O_RDWR, 0o600) //nolint:gosec // operator-configured path
	if err != nil {
		return fmt.Errorf("warm: open %s: %w", seg.Path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("warm: stat %s: %w", seg.Path, err)
	}
	_ = platform.FadviseRandom(int(f.Fd()), 0, 0) //nolint:gosec // fd from os.OpenFile; hint kernel for random-access reads
	seg.f = f
	seg.size = info.Size()
	seg.opened.Store(true)
	return nil
}

// closeIfIdle closes the segment fd if no readers are in flight.
// Returns true if the fd was closed (or already nil). Returns false
// if a reader started between the fdCache's lock-free readers check
// and the seg.mu acquisition — the caller leaves the fd open and
// the entry stays out of the cache until the next touch re-adds it.
func (seg *Segment) closeIfIdle() bool {
	seg.mu.Lock()
	defer seg.mu.Unlock()
	if seg.f == nil {
		return true
	}
	if seg.readers.Load() > 0 {
		return false
	}
	_ = seg.f.Close()
	seg.f = nil
	seg.opened.Store(false)
	return true
}

// Close closes the segment file if open. Safe to call on unopened segments.
func (seg *Segment) Close() error {
	seg.mu.Lock()
	defer seg.mu.Unlock()
	if seg.f == nil {
		return nil
	}
	err := seg.f.Close()
	seg.f = nil
	seg.opened.Store(false)
	return err
}

// fdCache is a bounded LRU cache of open segment file descriptors.
// It prevents unbounded FD growth when the warm tier has many segments.
// Eviction skips segments with in-flight readers (readers > 0) by moving
// them to the front of the LRU. If a reader starts between the lock-free
// readers check and the seg.mu acquisition inside closeIfIdle, the entry
// is removed from the cache but the fd is left open — it will be re-added
// on the next touch. The cache is protected by its own mutex, separate
// from s.mu, so eviction does not block normal segment lookups.
type fdCache struct {
	mu       sync.Mutex
	capacity int
	entries  map[int]*list.Element
	lru      *list.List
}

// newFDCache creates an FD cache with the given capacity. capacity <= 0
// means unlimited (no eviction).
func newFDCache(capacity int) *fdCache {
	if capacity < 0 {
		capacity = 0
	}
	return &fdCache{
		capacity: capacity,
		entries:  make(map[int]*list.Element),
		lru:      list.New(),
	}
}

// touch moves seg to the front of the LRU list and adds it if not
// present. If the cache is over capacity after insertion, it evicts
// LRU entries with zero readers until under capacity or no evictable
// entries remain. Must be called after seg.ensureOpen() succeeds.
func (c *fdCache) touch(seg *Segment) {
	if c == nil || c.capacity == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[seg.ID]; ok {
		c.lru.MoveToFront(el)
		return
	}
	el := c.lru.PushFront(seg)
	c.entries[seg.ID] = el
	moved := 0
	for c.lru.Len() > c.capacity {
		if moved >= c.lru.Len() {
			break // all entries have active readers
		}
		back := c.lru.Back()
		if back == nil {
			break
		}
		candidate := back.Value.(*Segment)
		if candidate.readers.Load() > 0 {
			c.lru.MoveToFront(back)
			moved++
			continue
		}
		c.lru.Remove(back)
		delete(c.entries, candidate.ID)
		moved = 0
		// closeIfIdle rechecks readers under seg.mu. If a reader
		// started between the lock-free check above and the lock
		// acquisition, the fd stays open and the entry stays out
		// of the cache until the next touch re-adds it.
		if !candidate.closeIfIdle() {
			continue
		}
	}
}

// clear removes all entries from the cache without closing them.
// Called during Compact and Close where the caller handles closing.
func (c *fdCache) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[int]*list.Element)
	c.lru = list.New()
}

// remove drops a segment from the cache without closing its fd.
func (c *fdCache) remove(segID int) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[segID]; ok {
		c.lru.Remove(el)
		delete(c.entries, segID)
	}
}

// Len returns the number of entries in the cache.
func (c *fdCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Len()
}

// warmLoc is the in-memory index entry for a warm-tier object. The
// size field stores the on-disk record size (HeaderLen + body + FooterLen)
// so Delete can subtract it from stats.bytes without re-reading the record.
//
// entry points to the entry's node in the SIEVE eviction list. It is
// set on Put/SetIndex and cleared on Delete/eviction. The visited bit
// on the SIEVE entry is set atomically by Get under idxMu.RLock — no
// write lock needed for access tracking.
//
// protected marks entries that also live in a faster tier. Evict
// skips these to avoid evicting a warm copy that the acceleration
// tier would immediately re-sync — wasting I/O on both tiers.
type warmLoc struct {
	segID     int
	offset    int64
	size      int64
	entry     *evictor.Entry[api.Key]
	protected bool
}

// Store is the warm-tier disk store.
type Store struct {
	dir          string
	maxBytes     int64
	maxEntries   int64
	maxDiskBytes int64
	minFreeDisk  int64
	preallocate  int64
	segMax       int64
	mu           sync.RWMutex
	segs         []*Segment
	segByID      map[int]*Segment // O(1) segment lookup by ID, kept in sync with segs
	nextID       atomic.Int32
	stats        warmStats
	idxMu        sync.RWMutex
	index        map[api.Key]warmLoc
	// protectedCount tracks the number of warm entries currently marked
	// protected (backed by a live hot entry). Maintained as an atomic
	// so ProtectedCount is O(1) — no index scan, no lock contention.
	// Incremented by Protect (false→true), decremented by Unprotect
	// (true→false), Delete (if the removed entry was protected), and
	// Put overwrite (if the old entry was protected and the new one is
	// not, which is the default — callers re-Protect if needed).
	protectedCount atomic.Int64
	// evictList is the SIEVE eviction list. O(1) eviction and access
	// tracking — Get sets the visited bit atomically under RLock, Put
	// inserts at head, Evict sweeps from the hand.
	evictList evictor.List[api.Key]
	// evictionAlgorithm records the configured policy so compact can
	// rebuild the correct list type. Stored separately from evictList
	// because evictList is replaced during compaction.
	evictionAlgorithm string
	// compactKeysBuf is a reusable buffer for collecting keys in append
	// order during compaction. Compaction runs on a single goroutine
	// (compactLoop), so no synchronization is needed. The buffer grows
	// to fit the steady-state key count and is reused across compaction
	// cycles, avoiding a per-compaction allocation of ~32 MB at 2M keys.
	compactKeysBuf []api.Key
	// OnEvict is called with the evicted key after evictOne removes the
	// entry from the warm index and writes the tombstone. The acceleration
	// tier uses this to clear hasBackup on the corresponding hot entry so
	// it is no longer preferred for hot-tier eviction.
	//
	// CONSTRAINT: MUST NOT block or perform I/O. The callback runs UNDER
	// idxMu (and seg.mu) — firing it under the locks closes the race where
	// a concurrent Put for the victim key re-inserts a live record and
	// sets hasBackup=true, only to have a late OnEvict clobber hasBackup
	// back to false and permanently strand the warm entry as protected.
	// ClearBacked (the only current callback) is O(1) and lock-only, so
	// holding idxMu across it adds no I/O latency.
	OnEvict func(key api.Key)
	// metrics receives warm-tier Prometheus collectors. Nil when the
	// store is constructed without a registry (tests, single-node).
	metrics *Metrics
	// fdCache bounds the number of open segment file descriptors.
	// Nil when SegmentCacheSize is -1 (unlimited). ensureOpen calls
	// fdCache.touch after opening a segment so the cache can evict the
	// LRU entry when over capacity.
	fdCache *fdCache
}

// newEvictList builds the warm-tier eviction list from the Config's
// algorithm selection. SIEVE is the default (zero-value config). When
// WarmEvictionAlgorithm == "cachaner" the list is a cachaner list,
// mirroring the hot tier's dispatch.
func newEvictList(cfg Config) evictor.List[api.Key] {
	if cfg.WarmEvictionAlgorithm == "cachaner" {
		return cachaner.NewList[api.Key]()
	}
	return sieve.NewList[api.Key]()
}

// rebuildSegByID updates the segByID index from the current segs slice.
// Must be called under s.mu.Lock whenever segs is modified.
func (s *Store) rebuildSegByID() {
	if len(s.segs) == 0 {
		s.segByID = nil
		return
	}
	if s.segByID == nil {
		s.segByID = make(map[int]*Segment, len(s.segs))
	} else {
		clear(s.segByID)
	}
	for _, seg := range s.segs {
		s.segByID[seg.ID] = seg
	}
}

type warmStats struct {
	entries        atomic.Int64
	bytes          atomic.Int64
	staleSelfHeals atomic.Int64
}

// Config configures the warm store.
type Config struct {
	Dir      string
	MaxBytes int64
	SegMax   int64 // per-segment max, default 64 MiB
	// SegmentCacheSize caps the number of concurrently open segment
	// file descriptors. 0 means auto (min(segCount, 256)). -1 means
	// unlimited (no eviction). When the cache is full and a new segment
	// is opened, the least-recently-accessed segment with zero in-flight
	// readers is closed.
	SegmentCacheSize int
	// MaxEntries caps the warm-tier index size in entries. Zero means
	// unlimited (backward compatible). A positive value derived from
	// GOMEMLIMIT bounds the Go heap cost of the index map. When the cap
	// is exceeded, Put rejects with ErrOverBudget and the warm sync loop
	// skips promotion.
	MaxEntries int64
	// MaxDiskBytes caps the total physical disk usage of segment files.
	// When exceeded, DiskOverBudget returns true and the caller should
	// trigger compaction. Zero means unlimited.
	MaxDiskBytes int64
	// MinFreeDisk is the minimum free disk space to maintain on the
	// warm-tier filesystem. When free space drops below this,
	// DiskOverBudget returns true. Zero means no filesystem monitoring.
	MinFreeDisk int64
	// Preallocate, when non-zero, pre-creates segment files at startup
	// totaling this size. The store operates as a circular buffer.
	// Zero means create segments on demand.
	Preallocate int64
	// Metrics receives warm-tier Prometheus collectors. Nil disables
	// metric collection (single-node mode without a registry).
	Metrics *Metrics
	// WarmEvictionAlgorithm selects the eviction policy for the warm tier.
	// "" and "sieve" (the default) use the SIEVE visited-bit sweep.
	// "cachaner" uses SIEVE with a 3-bit frequency counter that gives
	// hot objects up to 7 second chances (vs SIEVE's 1) before
	// eviction.
	//
	// This is the resolved per-tier value: builders copy either
	// config.Storage.WarmEvictionAlgorithm (when set) or the shared
	// config.Storage.EvictionAlgorithm into this field. The distinct
	// name from the shared config field keeps `grep EvictionAlgorithm`
	// unambiguous.
	WarmEvictionAlgorithm string
}

// NewStore creates or opens a warm store in dir.
func NewStore(cfg Config) (*Store, error) {
	if cfg.Dir == "" {
		return nil, errors.New("warm: dir is required")
	}
	if cfg.SegMax <= 0 {
		cfg.SegMax = 64 << 20
	}
	if err := os.MkdirAll(cfg.Dir, 0o750); err != nil {
		return nil, fmt.Errorf("warm: mkdir %s: %w", cfg.Dir, err)
	}

	s := &Store{
		dir:               cfg.Dir,
		maxBytes:          cfg.MaxBytes,
		maxEntries:        cfg.MaxEntries,
		maxDiskBytes:      cfg.MaxDiskBytes,
		minFreeDisk:       cfg.MinFreeDisk,
		preallocate:       cfg.Preallocate,
		segMax:            cfg.SegMax,
		index:             make(map[api.Key]warmLoc),
		evictList:         newEvictList(cfg),
		evictionAlgorithm: cfg.WarmEvictionAlgorithm,
		metrics:           cfg.Metrics,
	}
	if cfg.SegmentCacheSize != -1 {
		cacheSize := cfg.SegmentCacheSize
		if cacheSize == 0 {
			cacheSize = 256 // auto default, clamped to segCount after openExisting
		}
		s.fdCache = newFDCache(cacheSize)
	}
	if err := s.openExisting(); err != nil {
		return nil, err
	}
	// Pre-allocate segment files if configured. This creates segment
	// files at startup totaling the preallocate size, so the warm store
	// operates as a circular buffer instead of growing unboundedly.
	if s.preallocate > 0 && len(s.segs) == 0 {
		if err := s.preallocateSegments(); err != nil {
			return nil, fmt.Errorf("warm: preallocate: %w", err)
		}
	}
	// Clamp the FD cache to the actual segment count so we don't
	// reserve capacity for segments that don't exist.
	if s.fdCache != nil && s.fdCache.capacity > len(s.segs) && len(s.segs) > 0 {
		s.fdCache.capacity = len(s.segs)
	}
	// Set the max_bytes gauge once at construction. 0 means unlimited.
	if s.metrics != nil {
		s.metrics.SetMaxBytes(cfg.MaxBytes)
	}
	return s, nil
}

func (s *Store) openExisting() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("warm: readdir: %w", err)
	}
	var ids []int
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		// Clean up stale .tmp files from aborted compaction runs.
		if strings.HasSuffix(e.Name(), segExt+".tmp") {
			_ = os.Remove(filepath.Join(s.dir, e.Name()))
			continue
		}
		if !strings.HasSuffix(e.Name(), segExt) {
			continue
		}
		var id int
		if _, err := fmt.Sscanf(e.Name(), "%d"+segExt, &id); err != nil {
			continue
		}
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		path := filepath.Join(s.dir, segName(id))
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("warm: stat %s: %w", path, err)
		}
		seg := &Segment{
			ID:       id,
			Path:     path,
			size:     info.Size(),
			maxBytes: s.segMax,
		}
		s.segs = append(s.segs, seg)
		if int32(id) >= s.nextID.Load() { //nolint:gosec // segment IDs bounded
			s.nextID.Store(int32(id + 1)) //nolint:gosec // bounded
		}
	}
	s.rebuildSegByID()
	return nil
}

// preallocateSegments creates segment files at startup totaling the
// preallocate size. Each file is truncated to segMax (64 MiB default).
// This reserves disk space upfront so the warm store cannot grow
// beyond the pre-allocated size. When all segments are full, the
// activeSeg logic will reuse the oldest sealed segment (circular buffer).
func (s *Store) preallocateSegments() error {
	numSegs := int(s.preallocate / s.segMax)
	if numSegs <= 0 {
		numSegs = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for range numSegs {
		id := int(s.nextID.Add(1)) - 1
		segPath := filepath.Join(s.dir, segName(id))
		f, err := os.OpenFile(segPath, os.O_CREATE|os.O_RDWR, 0o600) //nolint:gosec // operator-configured warm dir
		if err != nil {
			return fmt.Errorf("create segment %d: %w", id, err)
		}
		if err := f.Truncate(s.segMax); err != nil {
			_ = f.Close()
			return fmt.Errorf("truncate segment %d: %w", id, err)
		}
		_ = f.Close()
		seg := &Segment{
			ID:   id,
			f:    nil, // opened on first access
			size: 0,
		}
		seg.mu = sync.Mutex{}
		s.segs = append(s.segs, seg)
	}
	s.rebuildSegByID()
	return nil
}

// Put appends a record to the active segment. Returns the segment ID
// and offset.
//
// Both the live-bytes budget (maxBytes) and the entry-count budget
// (maxEntries) are enforced on Put. Delete appends tombstones without
// a budget check; tombstones are reclaimed by compaction. When either
// budget is exceeded, Put attempts to evict the least-recently-
// accessed entries before rejecting with ErrOverBudget.
//
// The byte budget is checked against live entry bytes (stats.bytes), not
// total segment file sizes (diskBytes). Tombstones increase diskBytes
// but not stats.bytes — only compaction can shrink the files. Using
// stats.bytes keeps the gate and the eviction loop on the same metric
// so operators see a consistent story: if Put succeeds, live bytes fit.
// The entry budget bounds the Go heap cost of the warm index map.
//
// Put holds s.mu.RLock for its entire duration so that Compact's
// s.mu.Lock cannot take the index snapshot or swap segments while a Put is
// in flight (issue #280). If the active segment is full, Put releases
// s.mu.RLock, calls newSegment (which acquires s.mu.Lock), then re-acquires
// s.mu.RLock and retries. The retry loop converges because newSegment
// double-checks whether the last segment is still full under s.mu.Lock —
// so N goroutines that hit errSegFull simultaneously create exactly one
// new segment, not N.
//
// ensureBudgetLocked checks whether recSize fits within the configured
// budgets, attempting eviction if not. Returns nil if the record fits
// (either directly or after eviction), errSegFull if the active segment
// is full and the caller should create a new one, or ErrOverBudget if
// the record cannot fit even after eviction.
func (s *Store) ensureBudgetLocked(recSize int64) error {
	if s.maxBytes <= 0 && s.maxEntries <= 0 {
		return nil
	}
	if s.fitsBudgets(recSize) {
		return nil
	}
	evictErr := s.evictToFitBatchLocked(recSize)
	if evictErr != nil && !errors.Is(evictErr, errSegFull) {
		s.metrics.IncOverBudget()
		return fmt.Errorf("warm: put %d bytes: %w", recSize, ErrOverBudget)
	}
	return evictErr
}

// Put appends a live record for key to the active segment and updates the
// in-memory index. See the comment above ensureBudgetLocked for budget and
// lock-ordering details.
func (s *Store) Put(key api.Key, body []byte) (segID int, offset int64, err error) {
	recSize := int64(HeaderLen + len(body) + FooterLen)

	s.mu.RLock()
	defer s.mu.RUnlock()

	for {
		// Enforce both budgets before appending. A zero budget means
		// no limit (backward compatible with the default). When over
		// either budget, attempt eviction to free space before rejecting.
		if budgetErr := s.ensureBudgetLocked(recSize); budgetErr != nil {
			if errors.Is(budgetErr, errSegFull) {
				// Active segment is full; create a new one and retry.
				s.mu.RUnlock()
				if _, nErr := s.newSegment(); nErr != nil {
					s.mu.RLock()
					return 0, 0, nErr
				}
				s.mu.RLock()
				continue
			}
			return 0, 0, budgetErr
		}

		seg, segErr := s.activeSegRLocked()
		if segErr != nil {
			if errors.Is(segErr, errSegFull) {
				// Release RLock so newSegment can acquire Lock,
				// then re-acquire RLock and retry.
				s.mu.RUnlock()
				if _, nErr := s.newSegment(); nErr != nil {
					s.mu.RLock() // re-acquire so deferred unlock doesn't panic
					return 0, 0, nErr
				}
				s.mu.RLock()
				continue
			}
			return 0, 0, segErr
		}

		seg.mu.Lock()
		if seg.f == nil {
			if err := seg.openLocked(); err != nil {
				seg.mu.Unlock()
				return 0, 0, err
			}
		}

		off := seg.size
		if err := writeRecordAt(seg.f, off, magicLive, key, body); err != nil {
			seg.mu.Unlock()
			return 0, 0, fmt.Errorf("warm: write: %w", err)
		}
		seg.size += recSize
		seg.mu.Unlock()

		s.idxMu.Lock()
		// Subtract the old entry's bytes on overwrite so stats.bytes
		// reflects only live records. Without this, repeated overwrites
		// inflate the counter and evictToFit evicts unnecessarily.
		if old, ok := s.index[key]; ok {
			if old.size > 0 {
				s.stats.bytes.Add(-old.size)
			}
			// Overwrite replaces the index entry with protected=false (zero
			// value). If the old entry was protected, decrement now — the
			// caller (TieredStore.Put) re-Protects afterwards if needed.
			if old.protected {
				s.protectedCount.Add(-1)
			}
		} else {
			s.stats.entries.Add(1)
		}
		s.stats.bytes.Add(recSize)
		// Insert into the SIEVE list. If the key already exists (overwrite),
		// reuse the existing entry and mark it visited. Otherwise, insert a
		// new entry at head with visited=false. The bool return (newly
		// inserted) is discarded — the entry pointer is sufficient.
		e, _ := s.evictList.Access(key, func(k api.Key) *evictor.Entry[api.Key] {
			if loc, ok := s.index[k]; ok {
				return loc.entry
			}
			return nil
		})
		s.index[key] = warmLoc{segID: seg.ID, offset: off, size: recSize, entry: e}
		s.idxMu.Unlock()

		return seg.ID, off, nil
	}
}

// Delete writes a tombstone for the key and removes it from the index.
// Returns the segment ID the tombstone was written to, so callers can
// sync that segment before appending to the WAL.
//
// Like Put, Delete holds s.mu.RLock for its entire duration to prevent
// Compact from swapping segments mid-operation (issue #280).
func (s *Store) Delete(key api.Key) (segID int, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for {
		seg, segErr := s.activeSegRLocked()
		if segErr != nil {
			if errors.Is(segErr, errSegFull) {
				s.mu.RUnlock()
				if _, nErr := s.newSegment(); nErr != nil {
					s.mu.RLock()
					return 0, nErr
				}
				s.mu.RLock()
				continue
			}
			return 0, segErr
		}

		seg.mu.Lock()
		if seg.f == nil {
			if err := seg.openLocked(); err != nil {
				seg.mu.Unlock()
				return 0, err
			}
		}

		off := seg.size
		if err := writeRecordAt(seg.f, off, magicDead, key, nil); err != nil {
			seg.mu.Unlock()
			return 0, fmt.Errorf("warm: tombstone: %w", err)
		}
		recSize := int64(HeaderLen + FooterLen)
		seg.size += recSize
		seg.mu.Unlock()

		// Decrement stats counters for the deleted entry. The index entry
		// holds the on-disk record size so we can subtract it without
		// re-reading the record from disk.
		s.idxMu.Lock()
		loc, existed := s.index[key]
		if existed {
			if loc.protected {
				s.protectedCount.Add(-1)
			}
			if loc.entry != nil {
				s.evictList.Remove(loc.entry)
			}
			delete(s.index, key)
			s.stats.entries.Add(-1)
			if loc.size > 0 {
				s.stats.bytes.Add(-loc.size)
			}
		}
		s.idxMu.Unlock()
		return seg.ID, nil
	}
}

// Get returns the raw body bytes stored for key, or nil if the key
// is not in the warm tier.
//
// Lock ordering: s.mu.RLock (held for the full operation to prevent
// Compact from swapping segments) → idxMu.RLock (read index) →
// seg.mu.Lock (read record) → idxMu.RLock (set SIEVE visited bit).
// The visited bit is an atomic.Bool, so it is set under RLock without
// a lock upgrade. The re-check of segID + offset prevents marking a
// stale entry that was evicted and reused during the TOCTOU window.
func (s *Store) Get(key api.Key) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.idxMu.RLock()
	loc, ok := s.index[key]
	s.idxMu.RUnlock()
	if !ok {
		return nil, nil
	}

	// O(1) segment lookup by ID via the segByID index.
	var seg *Segment
	if s.segByID != nil {
		seg = s.segByID[loc.segID]
	} else {
		for _, ss := range s.segs {
			if ss.ID == loc.segID {
				seg = ss
				break
			}
		}
	}
	if seg == nil {
		// The index entry points at a segment that is no longer in
		// s.segs. This happens when a Put writes to a segment and
		// updates the index after Compact has swapped the segment set
		// (issue #193): the record is gone (unlinked inode) and the
		// index entry is stale. Self-heal the same way the torn-record
		// path below does — drop the entry and treat it as a miss so
		// the caller refetches instead of spamming WARN logs forever.
		s.dropStaleIndex(key, loc)
		return nil, nil
	}

	// readers.Add(1) before ensureOpen to prevent fdCache eviction from
	// closing the fd between ensureOpen and the actual read. Also needed
	// for ensureMmapOpened, which calls tryMmap using seg.f.Fd().
	seg.readers.Add(1)
	defer seg.readers.Add(-1)
	if err := seg.ensureOpen(); err != nil {
		return nil, fmt.Errorf("warm: open segment %d: %w", seg.ID, err)
	}
	// Lazy mmap init for segments that did not go through newSegment
	// (e.g., created by compaction or startup WAL replay). The active
	// segment is never mmap'd — it is being written to via pwritev and
	// mremap on growth would be complex and unnecessary.
	isActive := len(s.segs) > 0 && s.segs[len(s.segs)-1].ID == seg.ID
	seg.ensureMmapOpened(isActive)
	s.fdCache.touch(seg)
	rec, err := readRecordAt(seg, loc.offset, loc.size)
	if err != nil {
		if errors.Is(err, ErrTornRecord) {
			s.dropStaleIndex(key, loc)
			return nil, nil
		}
		return nil, err
	}
	if rec.IsTomb {
		return nil, nil
	}

	// Mark the entry as visited for SIEVE eviction. The visited bit is
	// an atomic.Bool, safe to set under RLock — no write lock needed.
	// Re-check identity (segID + offset) to avoid marking a stale entry
	// that was evicted and reused between the initial RLock and now.
	s.idxMu.RLock()
	defer s.idxMu.RUnlock()
	if cur, ok := s.index[key]; ok && cur.segID == loc.segID && cur.offset == loc.offset && cur.entry != nil {
		cur.entry.MarkVisited()
	}

	return rec.Body, nil
}

// SetIndex adds or replaces an index entry. Called during WAL replay
// on startup to rebuild the index from persisted write history. The size
// is set to 0 (unknown) for v1 WAL entries; RecomputeStats fills it in by
// scanning segments after replay. For v2 WAL entries, use SetIndexWithSize
// instead to set the size directly and avoid the segment scan.
//
// The SIEVE Access call mirrors Put: the bool return (newly inserted) is
// discarded — the entry pointer is sufficient for the index.
func (s *Store) SetIndex(key api.Key, segID int, offset int64) {
	s.idxMu.Lock()
	defer s.idxMu.Unlock()
	e, _ := s.evictList.Access(key, func(k api.Key) *evictor.Entry[api.Key] {
		if loc, ok := s.index[k]; ok {
			return loc.entry
		}
		return nil
	})
	s.index[key] = warmLoc{segID: segID, offset: offset, entry: e}
}

// SetIndexWithSize is like SetIndex but also sets the on-disk record
// size. Used during WAL v2 replay to populate the index with size
// information directly, avoiding the need for RecomputeStats to scan
// all segments on startup.
func (s *Store) SetIndexWithSize(key api.Key, segID int, offset, size int64) {
	s.idxMu.Lock()
	defer s.idxMu.Unlock()
	e, _ := s.evictList.Access(key, func(k api.Key) *evictor.Entry[api.Key] {
		if loc, ok := s.index[k]; ok {
			return loc.entry
		}
		return nil
	})
	s.index[key] = warmLoc{segID: segID, offset: offset, size: size, entry: e}
}

// IndexLen returns the number of entries currently in the warm-tier
// index. Unlike Stats(), which returns atomic counters that are only
// updated by Put/Delete/RecomputeStats, IndexLen reads the actual map
// size, so it reflects the true index state immediately after WAL
// replay (where SetIndex populates the map without touching counters).
func (s *Store) IndexLen() int {
	s.idxMu.RLock()
	defer s.idxMu.RUnlock()
	return len(s.index)
}

// Lookup returns the segment ID and offset for a key in the warm-tier
// index, or ok=false if the key is absent. Used by TieredStore to
// rewrite the WAL after warm-tier compaction without scanning segments.
func (s *Store) Lookup(key api.Key) (segID int, offset int64, ok bool) {
	s.idxMu.RLock()
	loc, exists := s.index[key]
	s.idxMu.RUnlock()
	if !exists {
		return 0, 0, false
	}
	return loc.segID, loc.offset, true
}

// LookupWithSize is like Lookup but also returns the on-disk record
// size. Used by rewriteWAL to write v2 WAL entries that include the
// size, so the next startup can skip RecomputeStats.
func (s *Store) LookupWithSize(key api.Key) (segID int, offset, size int64, ok bool) {
	s.idxMu.RLock()
	loc, exists := s.index[key]
	s.idxMu.RUnlock()
	if !exists {
		return 0, 0, 0, false
	}
	return loc.segID, loc.offset, loc.size, true
}

// DelIndex removes a key from the index. Called during WAL replay for
// delete entries so keys deleted before the last checkpoint are not
// served from the warm tier. Does not update stats counters; callers
// must run RecomputeStats after replay to restore accurate entries and
// bytes. Using DelIndex outside of replay will cause stats drift.
func (s *Store) DelIndex(key api.Key) {
	s.idxMu.Lock()
	if loc, ok := s.index[key]; ok {
		if loc.protected {
			s.protectedCount.Add(-1)
		}
		if loc.entry != nil {
			s.evictList.Remove(loc.entry)
		}
		delete(s.index, key)
	}
	s.idxMu.Unlock()
}

// RecomputeStats scans all segments and recounts live entries and bytes
// from the current index. Called after WAL v1 replay (or when some
// entries lack size) to restore the stats counters that are not
// persisted in the WAL. It also backfills the size field in each warmLoc
// so that subsequent Delete calls can subtract the correct record size
// from stats.bytes. On scan error the stats counters are not updated
// and the index backfill is skipped, so callers do not act on partial
// data.
//
// This function uses per-key RLock lookups instead of copying the entire
// index under RLock. This avoids the O(N) memory allocation that caused
// GC pressure and startup slowdown with millions of keys. RecomputeStats
// is only called on startup (before serving) or after compaction, so
// concurrent index modifications are not a concern.
func (s *Store) RecomputeStats() error {
	sizeUpdates := make(map[api.Key]int64)
	var entries, bytes int64
	if err := s.Scan(func(r Record) error {
		if r.IsTomb {
			return nil
		}
		s.idxMu.RLock()
		loc, ok := s.index[r.Key]
		s.idxMu.RUnlock()
		if !ok || loc.segID != r.SegID || loc.offset != r.Offset {
			return nil
		}
		recSize := int64(HeaderLen + len(r.Body) + FooterLen)
		entries++
		bytes += recSize
		if loc.size == 0 {
			sizeUpdates[r.Key] = recSize
		}
		return nil
	}); err != nil {
		return fmt.Errorf("warm: recompute stats: %w", err)
	}
	s.stats.entries.Store(entries)
	s.stats.bytes.Store(bytes)

	// Backfill size into index entries that were created by SetIndex
	// (WAL replay) with size=0 so future Delete calls subtract the
	// correct byte count.
	if len(sizeUpdates) > 0 {
		s.idxMu.Lock()
		for key, sz := range sizeUpdates {
			if loc, ok := s.index[key]; ok && loc.size == 0 {
				loc.size = sz
				s.index[key] = loc
			}
		}
		s.idxMu.Unlock()
	}
	return nil
}

// RecomputeStatsFromIndex computes stats counters from the in-memory
// index without scanning segments. Used after WAL v2 replay where all
// index entries already have their on-disk record size. This avoids the
// multi-second segment scan that RecomputeStats performs, reducing
// startup time from seconds to milliseconds with millions of keys.
// Entries with size=0 (from v1 WAL or SetIndex without size) are
// counted as zero bytes — callers should only use this when all entries
// have size (allHaveSize == true from initWAL).
func (s *Store) RecomputeStatsFromIndex() {
	s.idxMu.RLock()
	var entries, bytes int64
	for _, loc := range s.index {
		entries++
		bytes += loc.size
	}
	s.idxMu.RUnlock()
	s.stats.entries.Store(entries)
	s.stats.bytes.Store(bytes)
}

// ReadRecord reads a record at the given offset in the given segment.
//
// s.mu.RLock is held across the full operation (segment lookup + file
// read) so that Compact cannot close file handles while a read is in
// flight. The per-segment mutex is not needed: readRecordAt uses
// os.File.ReadAt (pread), which does not mutate the shared file offset.
func (s *Store) ReadRecord(segID int, offset int64) (*Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var seg *Segment
	for _, ss := range s.segs {
		if ss.ID == segID {
			seg = ss
			break
		}
	}
	if seg == nil {
		return nil, fmt.Errorf("warm: segment %d not found", segID)
	}

	seg.readers.Add(1)
	defer seg.readers.Add(-1)
	if err := seg.ensureOpen(); err != nil {
		return nil, fmt.Errorf("warm: open segment %d: %w", segID, err)
	}
	s.fdCache.touch(seg)
	rec, err := readRecordAt(seg, offset, 0) // size unknown for ReadRecord API
	if err != nil {
		if errors.Is(err, ErrTornRecord) {
			return nil, nil
		}
		return nil, err
	}
	return rec, nil
}

// Scan reads all live records from all segments. Used for index
// rebuilds after crash recovery.
func (s *Store) Scan(fn func(Record) error) error {
	s.mu.RLock()
	segs := make([]*Segment, len(s.segs))
	copy(segs, s.segs)
	s.mu.RUnlock()

	for _, seg := range segs {
		seg.readers.Add(1)
		if err := seg.ensureOpen(); err != nil {
			seg.readers.Add(-1)
			return err
		}
		s.fdCache.touch(seg)
		seg.mu.Lock()
		err := scanSegment(seg.f, seg.ID, fn)
		seg.mu.Unlock()
		seg.readers.Add(-1)
		if err != nil {
			return err
		}
	}
	return nil
}

// Stats returns warm-tier counters.
func (s *Store) Stats() (entries, bytes int64) {
	return s.stats.entries.Load(), s.stats.bytes.Load()
}

// SelfHeals returns the number of stale index entries dropped by the
// Get self-heal paths (segment-not-found and torn-record). Operators
// can poll this to detect segment-management bugs or disk faults that
// drop segments for reasons other than Compact — the self-heal turns
// those into silent misses, so this counter is the only signal.
func (s *Store) SelfHeals() int64 {
	return s.stats.staleSelfHeals.Load()
}

// dropStaleIndex removes the index entry for key iff it still points
// at the stale location (segID, offset). This is a compare-and-delete:
// re-reading under the write lock prevents a concurrent Put from having
// its valid entry nuked by a self-heal that read a stale entry before
// the Put landed (the TOCTOU window between idxMu.RUnlock and idxMu.Lock
// in Get).
//
// Only segID and offset are compared — not the full struct — because
// the SIEVE visited bit and protected may have changed between the
// caller's RLock read and this Lock write (a concurrent Get sets visited,
// a concurrent Protect updates protected). Those changes are
// benign: the entry is stale because the segment is gone, and identity
// is fully determined by (segID, offset).
func (s *Store) dropStaleIndex(key api.Key, stale warmLoc) {
	s.idxMu.Lock()
	if cur, ok := s.index[key]; ok && cur.segID == stale.segID && cur.offset == stale.offset {
		if cur.protected {
			s.protectedCount.Add(-1)
		}
		if cur.entry != nil {
			s.evictList.Remove(cur.entry)
		}
		delete(s.index, key)
		s.stats.entries.Add(-1)
		if cur.size > 0 {
			s.stats.bytes.Add(-cur.size)
		}
		s.stats.staleSelfHeals.Add(1)
	}
	s.idxMu.Unlock()
}

// Keys returns all keys present in the warm-tier index. The returned
// slice is unsorted; callers that need determinism must sort it. Used by
// TieredStore.Keys() to report the union of hot + warm keys so that
// TieredStore.Keys() reports the complete set of keys the node owns.
func (s *Store) Keys() []api.Key {
	s.idxMu.RLock()
	defer s.idxMu.RUnlock()
	keys := make([]api.Key, 0, len(s.index))
	for k := range s.index {
		keys = append(keys, k)
	}
	return keys
}

// Protect marks a warm-tier entry as also living in a faster tier.
// Evict skips protected entries to avoid evicting a warm copy that the
// acceleration tier would immediately re-sync. If the key is not in
// the warm index this is a no-op.
//
// Protect is always called together with hot.SetBacked (see
// TieredStore.Put and writeHotOnlyToWarm in tiered.go). The lifecycle
// of a protected warm entry is:
//
//   - Protect(key) + hot.SetBacked(key) are paired on insert/promotion.
//   - Hot SIEVE-eviction of a backed entry → warm.Unprotect(key) (demote
//     to SIEVE-managed) when experimental.storage.hot_evict_no_warm_tomb
//     is on, or warm.Delete(key) (tombstone) when it is off. The warm
//     copy either stays live and SIEVE-evictable (Unprotect) or is
//     removed entirely (Delete, clears protected as a side effect).
//   - Hot reaper / ban / Delete / Put-overwrite → warm.Delete(key)
//     (tombstone) regardless of the flag — the warm copy must be
//     removed for freshness/ban correctness.
//
// A protected warm entry is always backed by a live hot entry. When
// the hot entry is SIEVE-evicted, Unprotect demotes the warm copy so
// warm SIEVE can reclaim it under pressure. When the hot entry is
// removed by reaper/ban/Delete, the warm copy is deleted. Either way,
// no protected entry is stranded (protected without a hot backing).
func (s *Store) Protect(key api.Key) {
	s.idxMu.Lock()
	if loc, ok := s.index[key]; ok {
		if !loc.protected {
			loc.protected = true
			s.index[key] = loc
			s.protectedCount.Add(1)
		}
	}
	s.idxMu.Unlock()
}

// Unprotect clears the protected flag on a warm-tier entry, allowing
// warm SIEVE to evict it under pressure. The warm copy stays live and
// readable until warm SIEVE evicts it (or an explicit Delete removes
// it). Used by the hot tier's SIEVE-eviction path to demote a backed
// hot entry to warm-managed without destroying the warm copy (Fix A
// for #484).
//
// Unlike Delete, Unprotect does NOT write a tombstone: the on-disk
// record stays live. This is safe because warm SIEVE will write its
// own tombstone when it eventually evicts the now-unprotected entry
// (evictOneLocked at warm.go:evictOneLocked), and warmSyncLoop drains
// the warmEvictQueue for the WAL delete at that point.
//
// Idempotent: no-op if the key is absent or already unprotected.
// Must NOT be called under a hot shard write lock — see the
// lock-ordering note in tiered.go's warmUnprotectQueue drain path.
func (s *Store) Unprotect(key api.Key) {
	s.idxMu.Lock()
	if loc, ok := s.index[key]; ok && loc.protected {
		loc.protected = false
		s.index[key] = loc
		s.protectedCount.Add(-1)
	}
	s.idxMu.Unlock()
}

// ProtectedCount returns the number of warm-tier entries currently
// marked protected (backed by a live hot entry). O(1) — reads an
// atomic counter maintained by Protect/Unprotect/Delete/Put. Used
// for observability (a future bouine_warm_protected gauge can read
// this directly) and test assertions (the §4.1 regression test
// verifies no protected entries are stranded after Fix A).
func (s *Store) ProtectedCount() int {
	return int(s.protectedCount.Load())
}

// writeTombstoneLocked writes a tombstone record for key to the active
// segment. Both seg.mu and idxMu must be held by the caller. On failure,
// the SIEVE entry and index entry for the victim are restored so the
// victim is not lost.
func (s *Store) writeTombstoneLocked(seg *Segment, key api.Key, loc warmLoc) error {
	off := seg.size
	if err := writeRecordAt(seg.f, off, magicDead, key, nil); err != nil {
		s.restoreSIEVEEntry(key, loc)
		return err
	}
	seg.size += int64(HeaderLen + FooterLen)
	return nil
}

// evictOneLocked performs a single eviction assuming seg.mu and idxMu
// are already held. Returns the evicted key and true on success, or
// (0, false) if no victim is available or the tombstone write fails.
func (s *Store) evictOneLocked(seg *Segment) (api.Key, bool) {
	if s.evictList.Len() == 0 {
		return api.Key{}, false
	}

	victimKey, victimLoc, found := s.pickEvictVictim()
	if !found {
		return api.Key{}, false
	}

	// Write the tombstone to the active segment. Both seg.mu and idxMu
	// are held, so a concurrent Put for victimKey cannot interleave a
	// new live record between this tombstone and the index removal
	// below — the on-disk order is tombstone-then-future-live, which
	// rebuildIndexFromScan honors (tombstone applied first, then the
	// later live record wins). On failure, restore the SIEVE entry and
	// index entry so the victim is not lost.
	if err := s.writeTombstoneLocked(seg, victimKey, victimLoc); err != nil {
		return api.Key{}, false
	}

	// Remove from index and decrement stats. The SIEVE entry was
	// already removed by EvictBounded and returned to the pool.
	delete(s.index, victimKey)
	s.stats.entries.Add(-1)
	if victimLoc.size > 0 {
		s.stats.bytes.Add(-victimLoc.size)
	}
	// Fire OnEvict under idxMu (and seg.mu) so a concurrent Put for
	// victimKey cannot re-insert a live record and set hasBackup=true
	// before ClearBacked runs — that would clobber hasBackup on a genuinely
	// backed hot entry, stranding the warm copy as permanently
	// protected and un-evictable. ClearBacked is O(1) lock-only.
	if callback := s.OnEvict; callback != nil {
		callback(victimKey)
	}
	s.metrics.IncEvictions()
	return victimKey, true
}

// evictOne picks a non-protected victim via SIEVE, writes a tombstone
// for it, removes it from the index, and fires OnEvict. Protected
// entries are given a second chance (re-inserted at the SIEVE list head
// with visited=true). The scan is bounded by maxWarmEvictSkips so the
// worst case is O(1) under idxMu even when most entries are protected.
// Returns the evicted key and true, or 0 and false if no non-protected
// victim was found within the skip budget or the index is empty.
//
// Lock ordering matches Put/Delete: activeSeg → seg.mu → idxMu. Both
// seg.mu and idxMu are held across the tombstone write, the index
// removal, AND the OnEvict callback. Firing the callback under the
// locks closes the race where a concurrent Put for the victim key
// re-inserts a live record and sets hasBackup=true on the hot entry,
// only to have the late OnEvict clobber hasBackup back to false —
// permanently stranding the warm entry as protected and making it
// un-evictable. ClearBacked is O(1) and lock-only, so the extra hold
// time is negligible.
//
// For batch eviction (multiple victims in a single lock acquisition),
// use evictToFitBatchLocked instead.
//
// evictOne holds s.mu.RLock for its entire duration to prevent Compact
// from swapping segments during eviction (issue #280).
func (s *Store) evictOne() (api.Key, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seg, err := s.activeSegRLocked()
	if err != nil {
		return api.Key{}, false
	}

	seg.mu.Lock()
	defer seg.mu.Unlock()
	s.idxMu.Lock()
	defer s.idxMu.Unlock()

	return s.evictOneLocked(seg)
}

// fitsBudgets reports whether recSize bytes plus one new entry would
// fit under both the byte and entry-count budgets. A zero budget means
// unlimited, so a zero budget always fits.
func (s *Store) fitsBudgets(recSize int64) bool {
	bytesOK := s.maxBytes <= 0 || s.stats.bytes.Load()+recSize <= s.maxBytes
	entriesOK := s.maxEntries <= 0 || s.stats.entries.Load()+1 <= s.maxEntries
	return bytesOK && entriesOK
}

// evictToFitBatchLocked is the core eviction loop. The caller MUST hold
// s.mu.RLock so Compact cannot swap segments during eviction. It uses
// activeSegRLocked instead of activeSeg to avoid a re-entrant s.mu.RLock.
// If the active segment is full, it returns errSegFull so the caller (Put)
// can release s.mu.RLock, create a new segment, and retry.
func (s *Store) evictToFitBatchLocked(recSize int64) error {
	if s.maxBytes <= 0 && s.maxEntries <= 0 {
		return nil
	}
	if s.maxBytes > 0 && recSize > s.maxBytes {
		return ErrOverBudget
	}

	// Fast path: already fits both budgets.
	if s.fitsBudgets(recSize) {
		return nil
	}

	seg, err := s.activeSegRLocked()
	if err != nil {
		return err
	}

	seg.mu.Lock()
	defer seg.mu.Unlock()
	if seg.f == nil {
		if err := seg.openLocked(); err != nil {
			return err
		}
	}
	s.idxMu.Lock()
	defer s.idxMu.Unlock()

	for {
		if s.fitsBudgets(recSize) {
			return nil
		}
		if _, ok := s.evictOneLocked(seg); !ok {
			return ErrOverBudget
		}
	}
}

// pickEvictVictim sweeps the SIEVE list (bounded by maxWarmEvictSkips)
// for a non-protected victim. Protected entries are re-inserted at the
// head with visited=true (second chance). idxMu must be held.
func (s *Store) pickEvictVictim() (key api.Key, loc warmLoc, found bool) {
	for range maxWarmEvictSkips {
		cand, ok := s.evictList.EvictBounded(maxSweepProbes)
		if !ok {
			return api.Key{}, warmLoc{}, false
		}
		candLoc, exists := s.index[cand]
		if !exists {
			// Defensive: Delete, DelIndex, dropStaleIndex, and evictOne
			// all remove the SIEVE entry under idxMu before deleting the
			// index map entry, so an orphaned SIEVE entry should not be
			// reachable while we hold idxMu. Skip it and continue rather
			// than panicking — a bug here degrades eviction efficiency, not correctness.
			continue
		}
		if candLoc.protected {
			// Give the protected entry a second chance: re-insert
			// at head with visited=true. The hand will clear visited
			// on a future sweep and reconsider it.
			e, _ := s.evictList.Access(cand, func(api.Key) *evictor.Entry[api.Key] { return nil })
			e.MarkVisited()
			candLoc.entry = e
			s.index[cand] = candLoc
			continue
		}
		return cand, candLoc, true
	}
	return api.Key{}, warmLoc{}, false
}

// restoreSIEVEEntry re-inserts a victim into the SIEVE list and restores
// its index entry after a failed tombstone write. idxMu must be held.
// The SIEVE entry was removed by EvictBounded, so a fresh entry is inserted
// at the head with visited=false; the next Get will re-mark it.
func (s *Store) restoreSIEVEEntry(key api.Key, loc warmLoc) {
	e, _ := s.evictList.Access(key, func(api.Key) *evictor.Entry[api.Key] { return nil })
	loc.entry = e
	s.index[key] = loc
}

// Sync flushes all open segment files to stable storage. Used by Close
// and available for checkpoint-style callers. Hot-path callers (Put,
// Delete) should use SyncSegment instead to avoid syncing unrelated
// segments.
func (s *Store) Sync() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var firstErr error
	for _, seg := range s.segs {
		seg.mu.Lock()
		if seg.f != nil {
			if err := seg.f.Sync(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		seg.mu.Unlock()
	}
	return firstErr
}

// SyncSegment flushes a single segment to stable storage. Callers that
// also append to the WAL must invoke this before wal.Append so the
// segment data is durable before the WAL pointer to it is durable.
func (s *Store) SyncSegment(segID int) error {
	s.mu.RLock()
	var seg *Segment
	for _, ss := range s.segs {
		if ss.ID == segID {
			seg = ss
			break
		}
	}
	s.mu.RUnlock()
	if seg == nil {
		return fmt.Errorf("warm: sync: segment %d not found", segID)
	}
	if err := seg.ensureOpen(); err != nil {
		return err
	}
	s.fdCache.touch(seg)
	seg.mu.Lock()
	defer seg.mu.Unlock()
	if seg.f == nil {
		if err := seg.openLocked(); err != nil {
			return err
		}
	}
	return seg.f.Sync()
}

// Close syncs and closes all segment files. Writes a final index
// snapshot first so the next startup can use the fast path.
func (s *Store) Close() error {
	var firstErr error
	if err := s.WriteSnapshot(); err != nil {
		firstErr = fmt.Errorf("warm: snapshot on close: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Munmap all segments before closing fds. The mappings reference
	// the same files as the fds; munmap first to avoid accessing freed
	// mappings after fd close (defensive — POSIX allows it, but this
	// makes the lifecycle explicit and race-detector-clean).
	munmapAll(s.segs)
	s.fdCache.clear()
	for _, seg := range s.segs {
		seg.mu.Lock()
		if seg.f != nil {
			if err := seg.f.Sync(); err != nil && firstErr == nil {
				firstErr = err
			}
			if err := seg.f.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			seg.f = nil
			seg.opened.Store(false)
		}
		seg.mu.Unlock()
	}
	s.segs = nil
	s.segByID = nil
	return firstErr
}

// diskBytes returns the total on-disk size of all segment files. This
// includes live records, tombstones, and superseded (overwritten) keys.
// Put does NOT gate on diskBytes — it gates on stats.bytes (live record
// bytes) so eviction can free space before the Put. diskBytes is used by
// NeedsCompaction to compute the dead-space ratio and by tests to verify
// tombstones push the on-disk size past the budget.
func (s *Store) diskBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var total int64
	for _, seg := range s.segs {
		seg.mu.Lock()
		total += seg.size
		seg.mu.Unlock()
	}
	return total
}

// DiskBytes returns the total on-disk size of all warm-tier segment
// files (live records + tombstones + superseded entries). Exposed for
// the bouine_warm_disk_bytes Prometheus gauge. Unlike Stats() which
// returns live bytes, this reflects actual disk usage.
func (s *Store) DiskBytes() int64 {
	return s.diskBytes()
}

// MaxBytes returns the configured warm-tier byte budget. 0 means
// unlimited (no enforcement). Exposed for the bouine_warm_max_bytes
// Prometheus gauge.
func (s *Store) MaxBytes() int64 {
	return s.maxBytes
}

// OverBudget reports whether the warm index has reached either the
// configured maxBytes or maxEntries budget. The warm sync loop
// consults this before attempting hot→warm promotion to avoid
// wasting I/O on Put calls that will return ErrOverBudget (#205).
// A zero budget means unlimited, so only non-zero budgets are checked.
//
// This is an advisory, best-effort check: stats.bytes and stats.entries
// are read atomically without holding any lock, so a concurrent
// eviction or self-heal may drop the counts below the thresholds
// between this call and the subsequent Put. The Put path re-checks
// both budgets under the index lock, so a false-positive skip only
// delays promotion by one cycle.
func (s *Store) OverBudget() bool {
	if s.maxBytes > 0 && s.stats.bytes.Load() >= s.maxBytes {
		return true
	}
	if s.maxEntries > 0 && s.stats.entries.Load() >= s.maxEntries {
		return true
	}
	return false
}

// DiskOverBudget reports whether the physical disk usage exceeds
// warm_max_disk_bytes or the filesystem free space drops below
// min_free_disk. Unlike OverBudget (which checks logical bytes),
// this checks actual disk consumption. Used by the compactLoop to
// trigger immediate compaction when the disk is filling up.
func (s *Store) DiskOverBudget() bool {
	if s.maxDiskBytes > 0 && s.diskBytes() > s.maxDiskBytes {
		return true
	}
	if s.minFreeDisk > 0 {
		var stat unix.Statfs_t
		if err := unix.Statfs(s.dir, &stat); err == nil {
			freeBytes := int64(stat.Bavail) * int64(stat.Bsize) //nolint:gosec // filesystem sizes fit int64 on all supported platforms
			if freeBytes < s.minFreeDisk {
				return true
			}
		}
	}
	return false
}

// activeSegRLocked returns the active segment assuming the caller already
// holds s.mu.RLock. It does NOT acquire s.mu. If the active segment is full,
// it returns errSegFull — the caller must release s.mu.RLock, call
// newSegment (which acquires s.mu.Lock), then re-acquire s.mu.RLock and
// retry. This avoids a re-entrant s.mu.RLock → s.mu.Lock deadlock.
func (s *Store) activeSegRLocked() (*Segment, error) {
	if len(s.segs) > 0 {
		last := s.segs[len(s.segs)-1]
		last.mu.Lock()
		full := last.size >= last.maxBytes
		last.mu.Unlock()
		if !full {
			if err := last.ensureOpen(); err != nil {
				return nil, err
			}
			s.fdCache.touch(last)
			return last, nil
		}
	}
	return nil, errSegFull
}

func (s *Store) newSegment() (*Segment, error) { //nolint:unparam // result used in some callers
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after lock upgrade.
	if len(s.segs) > 0 {
		last := s.segs[len(s.segs)-1]
		last.mu.Lock()
		full := last.size >= last.maxBytes
		last.mu.Unlock()
		if !full {
			if err := last.ensureOpen(); err != nil {
				return nil, err
			}
			s.fdCache.touch(last)
			return last, nil
		}
	}

	id := int(s.nextID.Add(1) - 1)
	seg, err := openSegment(filepath.Join(s.dir, segName(id)), id, s.segMax)
	if err != nil {
		return nil, err
	}
	// Mmap the old (now-sealed) segment for zero-syscall reads. The old
	// segment's fd may have been evicted by fdCache; ensureOpen reopens
	// it so tryMmap can map the file. If either fails, seg.mmap stays nil
	// and Get's lazy init will retry later.
	if len(s.segs) > 0 {
		old := s.segs[len(s.segs)-1]
		_ = old.ensureOpen()
		old.tryMmap()
	}
	s.segs = append(s.segs, seg)
	s.rebuildSegByID()
	s.fdCache.touch(seg)
	return seg, nil
}

func openSegment(path string, id int, maxBytes int64) (*Segment, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600) //nolint:gosec // operator-configured path
	if err != nil {
		return nil, fmt.Errorf("warm: open %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("warm: stat %s: %w", path, err)
	}
	seg := &Segment{
		ID:       id,
		Path:     path,
		f:        f,
		size:     info.Size(),
		maxBytes: maxBytes,
	}
	seg.opened.Store(true)
	return seg, nil
}

func segName(id int) string {
	return fmt.Sprintf("%06d%s", id, segExt)
}

func writeRecordAt(f *os.File, offset int64, magic uint32, key api.Key, body []byte) error {
	hdrPtr := recordHdrPool.Get().(*[]byte)
	hdr := *hdrPtr
	defer recordHdrPool.Put(hdrPtr)
	binary.LittleEndian.PutUint32(hdr[0:4], magic)
	copy(hdr[4:20], key[:])
	binary.LittleEndian.PutUint32(hdr[20:24], uint32(len(body))) //nolint:gosec // body capped by segment size

	crc := crc32.Update(0, crcTable, hdr)
	if len(body) > 0 {
		crc = crc32.Update(crc, crcTable, body)
	}
	footPtr := recordFootPool.Get().(*[]byte)
	foot := *footPtr
	defer recordFootPool.Put(footPtr)
	binary.LittleEndian.PutUint32(foot, crc)

	var iov [3][]byte
	iov[0] = hdr
	nbuf := 1
	if len(body) > 0 {
		iov[nbuf] = body
		nbuf++
	}
	iov[nbuf] = foot
	nbuf++
	expected := len(hdr) + len(body) + len(foot)
	n, err := pwritevFn(int(f.Fd()), iov[:nbuf], offset) //nolint:gosec // fd from os.OpenFile; small non-negative integer
	if err == nil && n == expected {
		return nil
	}
	// n > 0: pwritev reported partial or full progress. On Linux a
	// real short write leaves n bytes committed at offset; a torn record
	// on disk is a data-integrity risk, so always complete the remaining
	// expected-n bytes via sequential WriteAt starting at offset+n. The
	// syscall cannot return n == expected with a non-nil err, but a
	// test-injected pwritevFn could; in that case writeRemaining is a
	// no-op (all buffers skipped) and returns nil. Once the record is
	// fully on disk, return nil — the caller must advance seg.size
	// regardless of what pwritev reported. Returning an error after a
	// completed write would cause the caller to skip the seg.size
	// advance, and the next Put would overwrite the completed record.
	if n > 0 {
		return writeRemaining(f.WriteAt, offset+int64(n), iov[:nbuf], n)
	}
	// n <= 0: ErrPwritevUnsupported (non-Linux, benign fallback), a real
	// Linux pwritev error (returns -1, zero bytes committed), or an
	// unexpected (0, nil) with non-empty buffers. In all cases, write the
	// full record sequentially from offset. If writeRemaining succeeds the
	// record is on disk; return nil so the caller advances seg.size.
	return writeRemaining(f.WriteAt, offset, iov[:nbuf], 0)
}

// writeRemaining writes the buffers that were not consumed by a prior
// Pwritev call. alreadyWritten is the number of bytes already committed
// at baseOffset; the function walks the iovec list, skipping fully
// consumed buffers and partial-writing the first partially-written one,
// then writes every remaining buffer sequentially via writeAt.
//
// writeAt is injected (callers pass *os.File.WriteAt) so tests can
// simulate short writes, I/O errors, and the (0, nil) guard without a
// real file descriptor.
func writeRemaining(writeAt func([]byte, int64) (int, error), baseOffset int64, iov [][]byte, alreadyWritten int) error {
	off := baseOffset
	remaining := alreadyWritten
	for _, buf := range iov {
		if remaining >= len(buf) {
			// This buffer was fully written by Pwritev; skip it.
			remaining -= len(buf)
			continue
		}
		// Write the unwritten tail of this buffer, then all of the rest.
		w := buf[remaining:]
		remaining = 0
		// writeAt can return a short write (n < len(w), nil). Retry
		// until the full buffer is on disk — this is the data-integrity
		// completion path and must not leave gaps. A (0, nil) return
		// would loop forever; guard against it explicitly.
		for len(w) > 0 {
			nw, werr := writeAt(w, off)
			if werr != nil {
				return werr
			}
			if nw == 0 {
				return fmt.Errorf("warm: writeRemaining: WriteAt returned 0 bytes without error at offset %d", off)
			}
			off += int64(nw)
			w = w[nw:]
		}
	}
	return nil
}

func readRecordAt(seg *Segment, offset int64, size int64) (*Record, error) {
	// Fastest path: mmap read (zero syscalls) for sealed segments with
	// a persistent MAP_SHARED mapping. Returns nil,nil if not mmap'd,
	// signaling the caller to fall through to the pread path.
	if rec, err := readRecordAtMmap(seg, offset, size); rec != nil || err != nil {
		return rec, err
	}
	// Fast path: single pread when the total record size is known from
	// the index (v2 WAL entries). This cuts 3 syscalls to 1.
	if size > 0 {
		return readRecordAtSingle(seg.f, seg.ID, offset, size)
	}
	// Fallback: 3-pread path for v1 WAL entries where size is unknown.
	return readRecordAtLegacy(seg.f, seg.ID, offset)
}

// readRecordAtSingle reads an entire record in one pread syscall using the
// total on-disk record size from the index. The body aliases the read buffer
// (no separate body allocation). The full buffer stays alive until the body
// is consumed by decode+promote; the overhead is HeaderLen+FooterLen = 28 bytes
// per record, temporary. Do NOT copy the body out — that adds an allocation
// and defeats the purpose.
func readRecordAtSingle(f *os.File, segID int, offset int64, size int64) (*Record, error) {
	if size < int64(HeaderLen+FooterLen) {
		return nil, ErrTornRecord
	}
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, offset); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, ErrTornRecord
		}
		return nil, fmt.Errorf("warm: read record at %d: %w", offset, err)
	}

	magic := binary.LittleEndian.Uint32(buf[0:4])
	var key api.Key
	copy(key[:], buf[4:20])
	bodyLen := binary.LittleEndian.Uint32(buf[20:24])

	// Validate bodyLen against the buffer to prevent slice out of range
	// on corrupted or stale index entries.
	if uint64(bodyLen) > uint64(len(buf))-uint64(HeaderLen+FooterLen) {
		return nil, ErrTornRecord
	}
	body := buf[HeaderLen : HeaderLen+bodyLen] // aliases buf, no separate alloc

	storedCRC := binary.LittleEndian.Uint32(buf[len(buf)-FooterLen:])
	if crc32.Checksum(buf[:len(buf)-FooterLen], crcTable) != storedCRC {
		return nil, fmt.Errorf("warm: CRC mismatch at seg %d offset %d", segID, offset)
	}

	return &Record{
		Key:    key,
		Body:   body,
		IsTomb: magic == magicDead,
		Offset: offset,
		SegID:  segID,
	}, nil
}

// readRecordAtLegacy is the 3-pread fallback for base WAL entries where the
// total record size is unknown. Reads header (24B), body (N bytes), and
// footer (4B) in separate pread syscalls.
func readRecordAtLegacy(f *os.File, segID int, offset int64) (*Record, error) {
	hdrPtr := recordHdrPool.Get().(*[]byte)
	hdr := *hdrPtr
	defer recordHdrPool.Put(hdrPtr)
	if _, err := f.ReadAt(hdr, offset); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, ErrTornRecord
		}
		return nil, fmt.Errorf("warm: read header at %d: %w", offset, err)
	}

	magic := binary.LittleEndian.Uint32(hdr[0:4])
	var key api.Key
	copy(key[:], hdr[4:20])
	bodyLen := binary.LittleEndian.Uint32(hdr[20:24])

	var body []byte
	if bodyLen > 0 {
		body = make([]byte, bodyLen)
		if _, err := f.ReadAt(body, offset+HeaderLen); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, ErrTornRecord
			}
			return nil, fmt.Errorf("warm: read body at %d: %w", offset, err)
		}
	}

	footPtr := recordFootPool.Get().(*[]byte)
	footBuf := *footPtr
	defer recordFootPool.Put(footPtr)
	if _, err := f.ReadAt(footBuf, offset+HeaderLen+int64(bodyLen)); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, ErrTornRecord
		}
		return nil, fmt.Errorf("warm: read footer at %d: %w", offset, err)
	}
	storedCRC := binary.LittleEndian.Uint32(footBuf)

	crc := crc32.New(crcTable)
	_, _ = crc.Write(hdr)
	if len(body) > 0 {
		_, _ = crc.Write(body)
	}
	if crc.Sum32() != storedCRC {
		return nil, fmt.Errorf("warm: CRC mismatch at seg %d offset %d", segID, offset)
	}

	return &Record{
		Key:    key,
		Body:   body,
		IsTomb: magic == magicDead,
		Offset: offset,
		SegID:  segID,
	}, nil
}

func scanSegment(f *os.File, segID int, fn func(Record) error) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	if size == 0 {
		return nil
	}

	// mmap the segment file for sequential scan. This eliminates
	// per-record pread syscalls (3 per record with the old ReadAt
	// approach) and turns the scan into a memory traversal at
	// ~10-20 GiB/s instead of ~130 MiB/s.
	data, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ, unix.MAP_SHARED|platform.MmapPopulate) //nolint:gosec // fd is a small non-negative integer
	if err != nil {
		// Fall back to ReadAt if mmap fails (e.g. unsupported FS).
		return scanSegmentReadAt(f, segID, size, fn)
	}
	defer func() { _ = unix.Munmap(data) }()
	// Hint the kernel that we will access this region sequentially
	// so it can aggressively read-ahead and drop pages behind the scan.
	_ = platform.MadviseSequential(data)

	offset := 0
	for offset < len(data) {
		if offset+HeaderLen > len(data) {
			return nil // torn trailing header
		}
		magic := binary.LittleEndian.Uint32(data[offset : offset+4])
		var key api.Key
		copy(key[:], data[offset+4:offset+20])
		bodyLen := int(binary.LittleEndian.Uint32(data[offset+20 : offset+24]))

		recEnd := offset + HeaderLen + bodyLen + FooterLen
		if recEnd > len(data) {
			return nil // torn trailing record
		}

		// CRC is computed over the mmap data directly — no copy needed
		// for the checksum itself.
		storedCRC := binary.LittleEndian.Uint32(data[offset+HeaderLen+bodyLen : recEnd])
		crc := crc32.New(crcTable)
		_, _ = crc.Write(data[offset : offset+HeaderLen])
		if bodyLen > 0 {
			_, _ = crc.Write(data[offset+HeaderLen : offset+HeaderLen+bodyLen])
		}
		if crc.Sum32() != storedCRC {
			return fmt.Errorf("warm: CRC mismatch at seg %d offset %d", segID, offset)
		}

		// Copy body out of the mmap region. The callback may retain
		// the body beyond the scan (e.g. Compact writes it to a new
		// segment), so we cannot reference the mmap data directly —
		// it is unmapped after scanSegment returns.
		var body []byte
		if bodyLen > 0 {
			body = make([]byte, bodyLen)
			copy(body, data[offset+HeaderLen:offset+HeaderLen+bodyLen])
		}

		rec := Record{
			Key:    key,
			Body:   body,
			IsTomb: magic == magicDead,
			Offset: int64(offset),
			SegID:  segID,
		}
		if err := fn(rec); err != nil {
			return err
		}
		offset = recEnd
	}
	return nil
}

// scanSegmentReadAt is the fallback scan implementation using ReadAt,
// used when mmap is not available or fails.
func scanSegmentReadAt(f *os.File, segID int, size int64, fn func(Record) error) error {
	offset := int64(0)
	for offset < size {
		rec, err := readRecordAtLegacy(f, segID, offset)
		if err != nil {
			if errors.Is(err, ErrTornRecord) {
				return nil
			}
			return err
		}
		if err := fn(*rec); err != nil {
			return err
		}
		offset += int64(HeaderLen + len(rec.Body) + FooterLen)
	}
	return nil
}

// CompactionThreshold is the fraction of dead/tombstoned bytes that triggers
// compaction. Default 0.3 (compact when ≥30% of stored bytes are stale).
const CompactionThreshold = 0.3

// NeedsCompaction reports whether the store has enough waste to warrant a
// compaction pass.
func (s *Store) NeedsCompaction() bool {
	entries, total := s.Stats()
	if total == 0 || entries == 0 {
		return false
	}
	// Estimate dead bytes as (total disk bytes) minus (live entry sizes).
	// A rough proxy: if live entries are < 70% of total segment capacity,
	// compaction is beneficial.
	disk := s.diskBytes()
	if disk == 0 {
		return false
	}
	liveFraction := float64(total) / float64(disk)
	return liveFraction < (1 - CompactionThreshold)
}

// Compact rewrites all live records to a fresh segment, dropping tombstones
// and overwritten keys. The old segments are deleted after a successful
// compaction.
//
// Compact holds s.mu.Lock for its entire duration. This blocks concurrent
// Put/Delete/Get for the compaction window — the scan, the SIEVE rebuild,
// and the segment-file swap all run under the lock. Without the full-method
// lock, a concurrent Put could create a new segment and write an index entry
// that the swap silently drops, causing data loss (issue #280). The lock is
// also necessary because swapSegmentFiles removes every *.seg file in the
// directory, including any a concurrent Put just created.
//
// Records are streamed per-segment: live records from one segment are
// collected, the segment lock is released, and only then written to the
// temp store. Peak heap is O(1 segment) instead of O(total live bytes).
//
//nolint:funlen // munmapAll adds 1 statement over the limit; extracting would harm readability
func (s *Store) Compact() error {
	s.metrics.IncCompactionTriggered()

	// Hold s.mu.Lock for the entire compaction to prevent concurrent
	// Put/Delete from creating segments or index entries that the swap
	// would silently drop. sync.RWMutex is not re-entrant, so
	// compactSegments receives the segment slice directly instead of
	// acquiring s.mu.RLock internally.
	s.mu.Lock()
	defer s.mu.Unlock()

	s.idxMu.RLock()
	idxSnap := make(map[api.Key]warmLoc, len(s.index))
	for k, v := range s.index {
		idxSnap[k] = v
	}
	s.idxMu.RUnlock()

	dir := s.dir
	compactDir := filepath.Join(dir, ".compact")
	_ = os.RemoveAll(compactDir) // stale dir from a previous failed run
	tmp, err := NewStore(Config{Dir: compactDir, MaxBytes: 0, SegMax: s.segMax})
	if err != nil {
		return fmt.Errorf("compact: create temp store: %w", err)
	}
	newIndex, orderedKeys, written, err := s.compactSegments(tmp, s.segs, idxSnap, compactDir, s.compactKeysBuf)
	if err != nil {
		return err
	}
	if written == 0 {
		_ = tmp.Close()
		_ = os.RemoveAll(compactDir)
		return nil
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("compact: close temp: %w", err)
	}

	// Build the eviction list and compute live bytes. This is O(N) but
	// runs under s.mu.Lock so the index cannot change concurrently.
	freshEvictList := newEvictList(Config{WarmEvictionAlgorithm: s.evictionAlgorithm})
	for _, key := range orderedKeys {
		e, _ := freshEvictList.Access(key, func(api.Key) *evictor.Entry[api.Key] { return nil })
		loc := newIndex[key]
		loc.entry = e
		newIndex[key] = loc
	}
	var liveBytes int64
	for _, loc := range newIndex {
		liveBytes += loc.size
	}

	// Munmap all old segments before swapping files. The old mappings
	// point at file offsets that will be replaced by swapSegmentFiles.
	// POSIX guarantees mappings are valid after fd close, but the files
	// themselves are about to be deleted — munmap first for cleanliness.
	munmapAll(s.segs)

	s.fdCache.clear()

	if err := swapSegmentFiles(dir, compactDir); err != nil {
		return err
	}

	fresh, err := NewStore(Config{Dir: dir, MaxBytes: s.maxBytes, SegMax: s.segMax})
	if err != nil {
		return fmt.Errorf("compact: reopen: %w", err)
	}

	for _, seg := range s.segs {
		_ = seg.Close()
	}
	s.segs = fresh.segs
	s.segByID = fresh.segByID

	// Replace the index and SIEVE list. s.mu.Lock is already held, so
	// no concurrent Put/Delete can interleave. idxMu.Lock is still
	// needed to serialize with Get, which acquires idxMu.RLock under
	// s.mu.RLock.
	s.idxMu.Lock()
	s.index = newIndex
	s.evictList = freshEvictList
	// Recompute protectedCount from newIndex under idxMu — Protect
	// and Unprotect only acquire idxMu (not s.mu), so they can race
	// with a Store outside the lock. Holding idxMu here serializes
	// the recompute against any concurrent Protect/Unprotect that
	// slipped in via a non-Put path (e.g. TieredStore callbacks).
	var protected int64
	for _, loc := range newIndex {
		if loc.protected {
			protected++
		}
	}
	s.protectedCount.Store(protected)
	s.idxMu.Unlock()

	s.stats.entries.Store(int64(len(newIndex)))
	s.stats.bytes.Store(liveBytes)
	s.compactKeysBuf = orderedKeys
	return nil
}

// swapSegmentFiles removes old segment files from dir, moves compacted
// segment files from compactDir into dir, then removes compactDir.
// On Unix, removing an open file does not invalidate existing file handles
// (the inode lives until the last fd closes), so callers can defer closing
// old segment handles until after the swap completes.
func swapSegmentFiles(dir, compactDir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("compact: readdir %s: %w", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), segExt) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
			return fmt.Errorf("compact: remove old segment %s: %w", e.Name(), err)
		}
	}

	compactEntries, err := os.ReadDir(compactDir)
	if err != nil {
		return fmt.Errorf("compact: readdir compact: %w", err)
	}
	for _, e := range compactEntries {
		if e.IsDir() {
			continue
		}
		if err := os.Rename(filepath.Join(compactDir, e.Name()), filepath.Join(dir, e.Name())); err != nil {
			return fmt.Errorf("compact: move compacted segment %s: %w", e.Name(), err)
		}
	}
	_ = os.RemoveAll(compactDir)
	return nil
}

type pendingRec struct {
	key  api.Key
	body []byte
}

// compactSegments scans each source segment for live records and writes
// them to tmp, returning the new index, the keys in append order (by
// segID then offset — the order records were written to the new store),
// and the count. Segment locks are held only during the scan, not
// during cross-store writes. The ordered keys are appended to keysBuf
// (reset to zero length on entry), which the caller provides — typically
// a reusable buffer on the Store — to avoid a per-compaction allocation.
//
// The caller MUST hold s.mu.Lock: segs is passed in to avoid a re-entrant
// s.mu.RLock (sync.RWMutex is not re-entrant).
func (s *Store) compactSegments(tmp *Store, segs []*Segment, idxSnap map[api.Key]warmLoc, compactDir string, keysBuf []api.Key) (map[api.Key]warmLoc, []api.Key, int, error) {
	orderedKeys := keysBuf[:0]
	newIndex := make(map[api.Key]warmLoc, len(idxSnap))
	written := 0
	for _, seg := range segs {
		seg.mu.Lock()
		var pending []pendingRec
		scanErr := scanSegment(seg.f, seg.ID, func(r Record) error {
			if r.IsTomb {
				return nil
			}
			loc, ok := idxSnap[r.Key]
			if !ok || loc.segID != r.SegID || loc.offset != r.Offset {
				return nil
			}
			pending = append(pending, pendingRec{key: r.Key, body: r.Body})
			return nil
		})
		seg.mu.Unlock()
		if scanErr != nil {
			_ = tmp.Close()
			_ = os.RemoveAll(compactDir)
			return nil, nil, 0, fmt.Errorf("compact: scan: %w", scanErr)
		}
		for _, p := range pending {
			segID, offset, wErr := tmp.Put(p.key, p.body)
			if wErr != nil {
				_ = tmp.Close()
				_ = os.RemoveAll(compactDir)
				return nil, nil, 0, fmt.Errorf("compact: write: %w", wErr)
			}
			recSize := int64(HeaderLen + len(p.body) + FooterLen)
			// Preserve protected from the pre-compaction index. The SIEVE
			// list is rebuilt in Compact from orderedKeys, so the sieve
			// pointer is left nil here and set during rebuild.
			old := idxSnap[p.key]
			newIndex[p.key] = warmLoc{
				segID:     segID,
				offset:    offset,
				size:      recSize,
				protected: old.protected,
			}
			orderedKeys = append(orderedKeys, p.key)
			written++
		}
	}
	return newIndex, orderedKeys, written, nil
}

// ErrSegmentNotFound is returned by CompactSegment when the target
// segment is not found or is the active segment (which cannot be
// compacted individually because Put writes to it).
var ErrSegmentNotFound = errors.New("warm: segment not found or is active")

// compactRec is a record written to the temp segment file during
// per-segment compaction, used by the swap phase to update the index.
type compactRec struct {
	key    api.Key
	segID  int
	offset int64
	size   int64
}

// CompactSegment compacts a single non-active segment. Returns
// ErrSegmentNotFound for the active segment or a missing segID.
func (s *Store) CompactSegment(segID int) error {
	s.metrics.IncCompactionTriggered()

	s.mu.RLock()
	var target *Segment
	for _, seg := range s.segs {
		if seg.ID == segID {
			target = seg
			break
		}
	}
	if target == nil {
		s.mu.RUnlock()
		return ErrSegmentNotFound
	}
	if len(s.segs) > 0 && s.segs[len(s.segs)-1].ID == segID {
		s.mu.RUnlock()
		return ErrSegmentNotFound
	}

	s.idxMu.RLock()
	idxSnap := make(map[api.Key]warmLoc)
	for k, v := range s.index {
		if v.segID == segID {
			idxSnap[k] = v
		}
	}
	s.idxMu.RUnlock()

	if err := target.ensureOpen(); err != nil {
		s.mu.RUnlock()
		return fmt.Errorf("compact segment: open: %w", err)
	}
	s.fdCache.touch(target)

	target.mu.Lock()
	var pending []pendingRec
	scanErr := scanSegment(target.f, target.ID, func(r Record) error {
		if r.IsTomb {
			return nil
		}
		loc, ok := idxSnap[r.Key]
		if !ok || loc.segID != r.SegID || loc.offset != r.Offset {
			return nil
		}
		pending = append(pending, pendingRec{key: r.Key, body: r.Body})
		return nil
	})
	target.mu.Unlock()
	s.mu.RUnlock()
	if scanErr != nil {
		return fmt.Errorf("compact segment %d: scan: %w", segID, scanErr)
	}

	newSegID, recs, err := s.writeCompactTemp(segID, pending)
	if err != nil {
		return err
	}

	return s.swapCompactSegment(segID, newSegID, recs)
}

// writeCompactTemp writes live records to a temp segment file and
// returns the new segID plus record metadata for the swap phase.
func (s *Store) writeCompactTemp(segID int, pending []pendingRec) (int, []compactRec, error) {
	newSegID := int(s.nextID.Add(1) - 1)
	tmpPath := filepath.Join(s.dir, segName(newSegID)+".tmp")
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600) //nolint:gosec // operator-configured path
	if err != nil {
		return 0, nil, fmt.Errorf("compact segment %d: create temp: %w", segID, err)
	}

	var recs []compactRec
	var off int64
	for _, p := range pending {
		recSize := int64(HeaderLen + len(p.body) + FooterLen)
		if err := writeRecordAt(tmpFile, off, magicLive, p.key, p.body); err != nil {
			_ = tmpFile.Close()
			_ = os.Remove(tmpPath)
			return 0, nil, fmt.Errorf("compact segment %d: write: %w", segID, err)
		}
		recs = append(recs, compactRec{
			key:    p.key,
			segID:  newSegID,
			offset: off,
			size:   recSize,
		})
		off += recSize
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return 0, nil, fmt.Errorf("compact segment %d: sync: %w", segID, err)
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return 0, nil, fmt.Errorf("compact segment %d: close temp: %w", segID, err)
	}
	return newSegID, recs, nil
}

// swapCompactSegment atomically replaces the old segment with the new
// one under s.mu.Lock and updates index entries still pointing at the
// old segID.
func (s *Store) swapCompactSegment(segID, newSegID int, recs []compactRec) error {
	tmpPath := filepath.Join(s.dir, segName(newSegID)+".tmp")

	s.mu.Lock()
	defer s.mu.Unlock()

	segIdx := -1
	for i, seg := range s.segs {
		if seg.ID == segID {
			segIdx = i
			break
		}
	}
	if segIdx < 0 {
		_ = os.Remove(tmpPath)
		return ErrSegmentNotFound
	}
	if segIdx == len(s.segs)-1 {
		_ = os.Remove(tmpPath)
		return ErrSegmentNotFound
	}

	finalPath := filepath.Join(s.dir, segName(newSegID))
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("compact segment %d: rename: %w", segID, err)
	}

	newSeg, err := openSegment(finalPath, newSegID, s.segMax)
	if err != nil {
		_ = os.Remove(finalPath)
		return fmt.Errorf("compact segment %d: open new: %w", segID, err)
	}
	newSeg.tryMmap()

	oldSeg := s.segs[segIdx]
	oldSeg.munmap()
	s.fdCache.remove(oldSeg.ID)
	_ = oldSeg.Close()
	_ = os.Remove(oldSeg.Path)

	s.segs[segIdx] = newSeg
	s.rebuildSegByID()
	s.fdCache.touch(newSeg)

	s.idxMu.Lock()
	defer s.idxMu.Unlock()
	for _, nr := range recs {
		cur, ok := s.index[nr.key]
		if !ok || cur.segID != segID {
			// Overwritten or deleted since the scan — leave the
			// orphaned record as dead space for the next compaction.
			continue
		}
		cur.segID = nr.segID
		cur.offset = nr.offset
		cur.size = nr.size
		s.index[nr.key] = cur
	}
	return nil
}

// SegmentCount returns the number of segments including the active one.
func (s *Store) SegmentCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.segs)
}

// NeedsSegmentCompaction returns the segID of the first non-active
// segment whose dead-byte ratio exceeds CompactionThreshold, or 0
// and false if none qualify.
func (s *Store) NeedsSegmentCompaction() (int, bool) {
	s.mu.RLock()
	if len(s.segs) <= 1 {
		s.mu.RUnlock()
		return 0, false
	}
	segs := make([]*Segment, len(s.segs))
	copy(segs, s.segs)
	s.mu.RUnlock()

	s.idxMu.RLock()
	livePerSeg := make(map[int]int64)
	for _, loc := range s.index {
		livePerSeg[loc.segID] += loc.size
	}
	s.idxMu.RUnlock()

	// Check each non-active segment.
	for i := range len(segs) - 1 {
		seg := segs[i]
		seg.mu.Lock()
		segSize := seg.size
		seg.mu.Unlock()
		if segSize == 0 {
			continue
		}
		live := livePerSeg[seg.ID]
		deadRatio := 1.0 - float64(live)/float64(segSize)
		if deadRatio >= CompactionThreshold {
			return seg.ID, true
		}
	}
	return 0, false
}
