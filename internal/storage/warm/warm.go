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
//	[8]  key        uint64
//	[4]  body_len   uint32
//	[N]  body       []byte
//	[4]  crc32c     checksum of (magic + key + body_len + body)
//
// Tombstones are written as records with body_len=0 and a special
// magic 0x44454144 ("DEAD"). Compaction rewrites live records into a
// new segment, skipping tombstones and keys that have been superseded.
//
// The warm tier is NOT goroutine-safe by itself; the TieredStore
// serializes writes through the hot-tier shard lock and the WAL.
package warm

import (
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
	"github.com/bouine-cache/bouine/internal/storage/sieve"
)

const (
	magicLive uint32 = 0x424F5549 // "BOUI"
	magicDead uint32 = 0x44454144 // "DEAD"
	headerLen        = 4 + 8 + 4  // magic + key + body_len
	footerLen        = 4          // crc32c
	segExt           = ".seg"
	// maxWarmEvictSkips bounds the number of protected entries SIEVE
	// may skip while searching for a non-protected victim. This keeps
	// eviction O(1) worst case under idxMu even when most entries are
	// protected (the steady state once the warm sync loop has marked
	// the majority of entries). Mirrors the hot tier's maxEvictSkips.
	maxWarmEvictSkips = 16
)

// RecordSize returns the on-disk byte footprint of a warm-tier record with
// the given body length: headerLen + bodyLen + footerLen. Exported so callers
// (e.g. tests) can compute exact record sizes without hardcoding the internal
// header/footer layout.
func RecordSize(bodyLen int) int {
	return headerLen + bodyLen + footerLen
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// recordHdrPool pools the fixed-size 16-byte header buffer used by
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
		buf := make([]byte, headerLen)
		return &buf
	},
}

// recordFootPool pools the fixed-size 4-byte footer buffer used by
// readRecordAt. Same rationale as recordHdrPool.
var recordFootPool = sync.Pool{
	New: func() any {
		buf := make([]byte, footerLen)
		return &buf
	},
}

// ErrTornRecord indicates a partial trailing record — the segment was
// truncated mid-write (torn write). Callers should treat the affected
// index entry as stale (drop it, return a miss) rather than surfacing
// a hard error, mirroring wal.Replay's handling of partial records.
var ErrTornRecord = errors.New("warm: torn trailing record")

// ErrOverBudget indicates that appending a record would push total
// disk bytes across the configured MaxBytes limit. Callers can handle
// this by evicting objects, triggering compaction, or simply skipping
// the warm-tier write (the acceleration tier already holds the object).
var ErrOverBudget = errors.New("warm: over maxBytes budget")

// Record is a single warm-tier entry read from a segment.
type Record struct {
	Key    uint64
	Body   []byte
	IsTomb bool
	Offset int64
	SegID  int
}

// Segment is an append-only file on disk.
type Segment struct {
	ID       int
	Path     string
	mu       sync.Mutex
	f        *os.File
	size     int64
	maxBytes int64
}

// warmLoc is the in-memory index entry for a warm-tier object. The
// size field stores the on-disk record size (headerLen + body + footerLen)
// so Delete can subtract it from stats.bytes without re-reading the record.
//
// sieve points to the entry's node in the SIEVE eviction list. It is
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
	sieve     *sieve.Entry[uint64]
	protected bool
}

// Store is the warm-tier disk store.
type Store struct {
	dir      string
	maxBytes int64
	segMax   int64
	mu       sync.RWMutex
	segs     []*Segment
	segByID  map[int]*Segment // O(1) segment lookup by ID, kept in sync with segs
	nextID   atomic.Int32
	stats    warmStats
	idxMu    sync.RWMutex
	index    map[uint64]warmLoc
	// evictList is the SIEVE eviction list. O(1) eviction and access
	// tracking — Get sets the visited bit atomically under RLock, Put
	// inserts at head, Evict sweeps from the hand.
	evictList *sieve.List[uint64]
	// compactKeysBuf is a reusable buffer for collecting keys in append
	// order during compaction. Compaction runs on a single goroutine
	// (compactLoop), so no synchronization is needed. The buffer grows
	// to fit the steady-state key count and is reused across compaction
	// cycles, avoiding a per-compaction allocation of ~16 MB at 2M keys.
	compactKeysBuf []uint64
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
	OnEvict func(key uint64)
	// metrics receives warm-tier Prometheus collectors. Nil when the
	// store is constructed without a registry (tests, single-node).
	metrics *Metrics
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
	// Metrics receives warm-tier Prometheus collectors. Nil disables
	// metric collection (single-node mode without a registry).
	Metrics *Metrics
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
		dir:       cfg.Dir,
		maxBytes:  cfg.MaxBytes,
		segMax:    cfg.SegMax,
		index:     make(map[uint64]warmLoc),
		evictList: sieve.NewList[uint64](),
		metrics:   cfg.Metrics,
	}
	if err := s.openExisting(); err != nil {
		return nil, err
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
		if e.IsDir() || !strings.HasSuffix(e.Name(), segExt) {
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
		seg, err := openSegment(filepath.Join(s.dir, segName(id)), id, s.segMax)
		if err != nil {
			return err
		}
		s.segs = append(s.segs, seg)
		if int32(id) >= s.nextID.Load() { //nolint:gosec // segment IDs bounded
			s.nextID.Store(int32(id + 1)) //nolint:gosec // bounded
		}
	}
	return nil
}

// Put appends a record to the active segment. Returns the segment ID
// and offset.
//
// maxBytes is enforced on Put only. Delete appends tombstones without
// a budget check; tombstones are reclaimed by compaction. When the live
// bytes exceed maxBytes, Put attempts to evict the least-recently-
// accessed entries before rejecting with ErrOverBudget.
//
// The budget is checked against live entry bytes (stats.bytes), not
// total segment file sizes (diskBytes). Tombstones increase diskBytes
// but not stats.bytes — only compaction can shrink the files. Using
// stats.bytes keeps the gate and the eviction loop on the same metric
// so operators see a consistent story: if Put succeeds, live bytes fit.
func (s *Store) Put(key uint64, body []byte) (segID int, offset int64, err error) {
	recSize := int64(headerLen + len(body) + footerLen)

	// Enforce live-bytes budget before appending. maxBytes == 0 means
	// no limit (backward compatible with the default). When over budget,
	// attempt eviction to free space before rejecting.
	if s.maxBytes > 0 {
		if s.stats.bytes.Load()+recSize > s.maxBytes {
			if evictErr := s.evictToFit(recSize); evictErr != nil {
				s.metrics.IncOverBudget()
				return 0, 0, fmt.Errorf("warm: put %d bytes: %w", recSize, ErrOverBudget)
			}
		}
	}

	seg, err := s.activeSeg()
	if err != nil {
		return 0, 0, err
	}

	seg.mu.Lock()
	defer seg.mu.Unlock()

	off := seg.size
	if _, err := seg.f.Seek(off, io.SeekStart); err != nil {
		return 0, 0, fmt.Errorf("warm: seek: %w", err)
	}
	if err := writeRecord(seg.f, magicLive, key, body); err != nil {
		return 0, 0, fmt.Errorf("warm: write: %w", err)
	}
	seg.size += recSize

	s.idxMu.Lock()
	// Subtract the old entry's bytes on overwrite so stats.bytes
	// reflects only live records. Without this, repeated overwrites
	// inflate the counter and evictToFit evicts unnecessarily.
	if old, ok := s.index[key]; ok {
		if old.size > 0 {
			s.stats.bytes.Add(-old.size)
		}
	} else {
		s.stats.entries.Add(1)
	}
	s.stats.bytes.Add(recSize)
	// Insert into the SIEVE list. If the key already exists (overwrite),
	// reuse the existing entry and mark it visited. Otherwise, insert a
	// new entry at head with visited=false. The bool return (newly
	// inserted) is discarded — the entry pointer is sufficient.
	e, _ := s.evictList.Access(key, func(k uint64) *sieve.Entry[uint64] {
		if loc, ok := s.index[k]; ok {
			return loc.sieve
		}
		return nil
	})
	s.index[key] = warmLoc{segID: seg.ID, offset: off, size: recSize, sieve: e}
	s.idxMu.Unlock()

	return seg.ID, off, nil
}

// Delete writes a tombstone for the key and removes it from the index.
// Returns the segment ID the tombstone was written to, so callers can
// sync that segment before appending to the WAL.
func (s *Store) Delete(key uint64) (segID int, err error) {
	seg, err := s.activeSeg()
	if err != nil {
		return 0, err
	}
	seg.mu.Lock()
	defer seg.mu.Unlock()

	if _, err := seg.f.Seek(seg.size, io.SeekStart); err != nil {
		return 0, fmt.Errorf("warm: seek: %w", err)
	}
	if err := writeRecord(seg.f, magicDead, key, nil); err != nil {
		return 0, fmt.Errorf("warm: tombstone: %w", err)
	}
	recSize := int64(headerLen + footerLen)
	seg.size += recSize

	// Decrement stats counters for the deleted entry. The index entry
	// holds the on-disk record size so we can subtract it without
	// re-reading the record from disk.
	s.idxMu.Lock()
	loc, existed := s.index[key]
	if existed {
		if loc.sieve != nil {
			s.evictList.Remove(loc.sieve)
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

// Get returns the raw body bytes stored for key, or nil if the key
// is not in the warm tier.
//
// Lock ordering: s.mu.RLock (held for the full operation to prevent
// Compact from swapping segments) → idxMu.RLock (read index) →
// seg.mu.Lock (read record) → idxMu.RLock (set SIEVE visited bit).
// The visited bit is an atomic.Bool, so it is set under RLock without
// a lock upgrade. The re-check of segID + offset prevents marking a
// stale entry that was evicted and reused during the TOCTOU window.
func (s *Store) Get(key uint64) ([]byte, error) {
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

	seg.mu.Lock()
	defer seg.mu.Unlock()

	rec, err := readRecordAt(seg.f, loc.offset, loc.segID)
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
	if cur, ok := s.index[key]; ok && cur.segID == loc.segID && cur.offset == loc.offset && cur.sieve != nil {
		cur.sieve.MarkVisited()
	}

	return rec.Body, nil
}

// SetIndex adds or replaces an index entry. Called during WAL replay
// on startup to rebuild the index from persisted write history. The
// size is set to 0 because the WAL does not record record sizes;
// RecomputeStats fills it in by scanning segments after replay.
//
// The SIEVE Access call mirrors Put: the bool return (newly inserted) is
// discarded — the entry pointer is sufficient for the index.
func (s *Store) SetIndex(key uint64, segID int, offset int64) {
	s.idxMu.Lock()
	defer s.idxMu.Unlock()
	e, _ := s.evictList.Access(key, func(k uint64) *sieve.Entry[uint64] {
		if loc, ok := s.index[k]; ok {
			return loc.sieve
		}
		return nil
	})
	s.index[key] = warmLoc{segID: segID, offset: offset, sieve: e}
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
func (s *Store) Lookup(key uint64) (segID int, offset int64, ok bool) {
	s.idxMu.RLock()
	loc, exists := s.index[key]
	s.idxMu.RUnlock()
	if !exists {
		return 0, 0, false
	}
	return loc.segID, loc.offset, true
}

// DelIndex removes a key from the index. Called during WAL replay for
// delete entries so keys deleted before the last checkpoint are not
// served from the warm tier. Does not update stats counters; callers
// must run RecomputeStats after replay to restore accurate entries and
// bytes. Using DelIndex outside of replay will cause stats drift.
func (s *Store) DelIndex(key uint64) {
	s.idxMu.Lock()
	if loc, ok := s.index[key]; ok {
		if loc.sieve != nil {
			s.evictList.Remove(loc.sieve)
		}
		delete(s.index, key)
	}
	s.idxMu.Unlock()
}

// RecomputeStats scans all segments and recounts live entries and bytes
// from the current index. Called after WAL replay to restore the stats
// counters that are not persisted in the WAL. It also backfills the
// size field in each warmLoc so that subsequent Delete calls can
// subtract the correct record size from stats.bytes. On scan error the
// stats counters are not updated and the index backfill is skipped, so
// callers do not act on partial data.
func (s *Store) RecomputeStats() error {
	s.idxMu.RLock()
	idxSnap := make(map[uint64]warmLoc, len(s.index))
	for k, v := range s.index {
		idxSnap[k] = v
	}
	s.idxMu.RUnlock()

	sizeUpdates := make(map[uint64]int64)
	var entries, bytes int64
	if err := s.Scan(func(r Record) error {
		if r.IsTomb {
			return nil
		}
		loc, ok := idxSnap[r.Key]
		if !ok || loc.segID != r.SegID || loc.offset != r.Offset {
			return nil
		}
		recSize := int64(headerLen + len(r.Body) + footerLen)
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

// ReadRecord reads a record at the given offset in the given segment.
//
// s.mu.RLock is held across the full operation (segment lookup + file
// read) so that Compact cannot close file handles while a read is in
// flight.
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

	seg.mu.Lock()
	defer seg.mu.Unlock()

	rec, err := readRecordAt(seg.f, offset, segID)
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
		seg.mu.Lock()
		err := scanSegment(seg.f, seg.ID, fn)
		seg.mu.Unlock()
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
func (s *Store) dropStaleIndex(key uint64, stale warmLoc) {
	s.idxMu.Lock()
	if cur, ok := s.index[key]; ok && cur.segID == stale.segID && cur.offset == stale.offset {
		if cur.sieve != nil {
			s.evictList.Remove(cur.sieve)
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
func (s *Store) Keys() []uint64 {
	s.idxMu.RLock()
	defer s.idxMu.RUnlock()
	keys := make([]uint64, 0, len(s.index))
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
// There is deliberately no Unprotect. The invariant that prevents
// stranded protected entries is: Protect is always called together
// with hot.SetBacked (see TieredStore.Put and writeHotOnlyToWarm in
// tiered.go). When the hot tier evicts a backed entry, its OnEvict
// callback enqueues a tombstone, and drainTombstones calls warm.Delete
// — which removes the warm entry entirely, clearing protected as a
// side effect. So a protected warm entry is always backed by a live
// hot entry, and the hot entry's eviction path always deletes the
// warm entry. A standalone Unprotect would need to be wired into
// every hot-tier removal path to match this lifecycle; the paired
// Protect+SetBacked + Delete-on-hot-eviction lifecycle is simpler
// and already correct.
func (s *Store) Protect(key uint64) {
	s.idxMu.Lock()
	if loc, ok := s.index[key]; ok {
		loc.protected = true
		s.index[key] = loc
	}
	s.idxMu.Unlock()
}

// evictToFit attempts to evict entries until the given record size fits
// within the live-bytes budget (stats.bytes). Returns nil if space was
// freed, or ErrOverBudget if eviction cannot free enough space (empty
// index or all visible entries protected within the skip budget).
// Evicted entries are tombstoned; actual disk space is reclaimed at the
// next compaction. The budget is checked against live bytes, not
// diskBytes (total segment file sizes), because tombstones increase
// diskBytes — only compaction can shrink the files.
//
// Only non-protected entries are evicted on the Put path: evicting a
// protected warm entry is wasteful because the acceleration tier will
// re-sync it on the next warm sync cycle. If no non-protected victim is
// found within maxWarmEvictSkips SIEVE probes, the Put is rejected with
// ErrOverBudget rather than scanning the whole list under idxMu.
func (s *Store) evictToFit(recSize int64) error {
	if s.maxBytes <= 0 {
		return nil
	}
	// A record larger than the entire budget can never fit, even with
	// an empty tier. Reject early to avoid evicting the whole warm tier
	// for nothing.
	if recSize > s.maxBytes {
		return ErrOverBudget
	}
	for {
		if s.stats.bytes.Load()+recSize <= s.maxBytes {
			return nil
		}
		if _, ok := s.evictOne(); !ok {
			return ErrOverBudget
		}
	}
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
func (s *Store) evictOne() (uint64, bool) {
	seg, err := s.activeSeg()
	if err != nil {
		return 0, false
	}

	seg.mu.Lock()
	defer seg.mu.Unlock()
	s.idxMu.Lock()
	defer s.idxMu.Unlock()

	if s.evictList.Len() == 0 {
		return 0, false
	}

	victimKey, victimLoc, found := s.pickEvictVictim()
	if !found {
		return 0, false
	}

	// Write the tombstone to the active segment. Both seg.mu and idxMu
	// are held, so a concurrent Put for victimKey cannot interleave a
	// new live record between this tombstone and the index removal
	// below — the on-disk order is tombstone-then-future-live, which
	// rebuildIndexFromScan honors (tombstone applied first, then the
	// later live record wins). On failure, restore the SIEVE entry and
	// index entry so the victim is not lost.
	if _, err := seg.f.Seek(seg.size, io.SeekStart); err != nil {
		s.restoreSIEVEEntry(victimKey, victimLoc)
		return 0, false
	}
	if err := writeRecord(seg.f, magicDead, victimKey, nil); err != nil {
		s.restoreSIEVEEntry(victimKey, victimLoc)
		return 0, false
	}
	seg.size += int64(headerLen + footerLen)

	// Remove from index and decrement stats. The SIEVE entry was
	// already removed by Evict() and returned to the pool.
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

// pickEvictVictim sweeps the SIEVE list (bounded by maxWarmEvictSkips)
// for a non-protected victim. Protected entries are re-inserted at the
// head with visited=true (second chance). idxMu must be held.
func (s *Store) pickEvictVictim() (key uint64, loc warmLoc, found bool) {
	for range maxWarmEvictSkips {
		cand, ok := s.evictList.Evict()
		if !ok {
			return 0, warmLoc{}, false
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
			e, _ := s.evictList.Access(cand, func(uint64) *sieve.Entry[uint64] { return nil })
			e.MarkVisited()
			candLoc.sieve = e
			s.index[cand] = candLoc
			continue
		}
		return cand, candLoc, true
	}
	return 0, warmLoc{}, false
}

// restoreSIEVEEntry re-inserts a victim into the SIEVE list and restores
// its index entry after a failed tombstone write. idxMu must be held.
// The SIEVE entry was removed by Evict(), so a fresh entry is inserted
// at the head with visited=false; the next Get will re-mark it.
func (s *Store) restoreSIEVEEntry(key uint64, loc warmLoc) {
	e, _ := s.evictList.Access(key, func(uint64) *sieve.Entry[uint64] { return nil })
	loc.sieve = e
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
		if err := seg.f.Sync(); err != nil && firstErr == nil {
			firstErr = err
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
	seg.mu.Lock()
	defer seg.mu.Unlock()
	return seg.f.Sync()
}

// Close syncs and closes all segment files.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for _, seg := range s.segs {
		if err := seg.f.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := seg.f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
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

// OverBudget reports whether live entry bytes have reached the configured
// maxBytes budget. The warm sync loop consults this before attempting
// hot→warm promotion to avoid wasting I/O on Put calls that will return
// ErrOverBudget (#205). maxBytes == 0 means unlimited, so OverBudget
// always returns false.
func (s *Store) OverBudget() bool {
	if s.maxBytes <= 0 {
		return false
	}
	return s.stats.bytes.Load() >= s.maxBytes
}

func (s *Store) activeSeg() (*Segment, error) {
	s.mu.RLock()
	if len(s.segs) > 0 {
		last := s.segs[len(s.segs)-1]
		last.mu.Lock()
		full := last.size >= last.maxBytes
		last.mu.Unlock()
		if !full {
			s.mu.RUnlock()
			return last, nil
		}
	}
	s.mu.RUnlock()

	return s.newSegment()
}

func (s *Store) newSegment() (*Segment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after lock upgrade.
	if len(s.segs) > 0 {
		last := s.segs[len(s.segs)-1]
		last.mu.Lock()
		full := last.size >= last.maxBytes
		last.mu.Unlock()
		if !full {
			return last, nil
		}
	}

	id := int(s.nextID.Add(1) - 1)
	seg, err := openSegment(filepath.Join(s.dir, segName(id)), id, s.segMax)
	if err != nil {
		return nil, err
	}
	s.segs = append(s.segs, seg)
	s.rebuildSegByID()
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
	return &Segment{
		ID:       id,
		Path:     path,
		f:        f,
		size:     info.Size(),
		maxBytes: maxBytes,
	}, nil
}

func segName(id int) string {
	return fmt.Sprintf("%06d%s", id, segExt)
}

func writeRecord(w io.Writer, magic uint32, key uint64, body []byte) error {
	hdrPtr := recordHdrPool.Get().(*[]byte)
	hdr := *hdrPtr
	defer recordHdrPool.Put(hdrPtr)
	binary.LittleEndian.PutUint32(hdr[0:4], magic)
	binary.LittleEndian.PutUint64(hdr[4:12], key)
	binary.LittleEndian.PutUint32(hdr[12:16], uint32(len(body))) //nolint:gosec // body capped by segment size

	crc := crc32.New(crcTable)
	if _, err := crc.Write(hdr); err != nil {
		return err
	}
	if len(body) > 0 {
		if _, err := crc.Write(body); err != nil {
			return err
		}
	}

	footPtr := recordFootPool.Get().(*[]byte)
	foot := *footPtr
	defer recordFootPool.Put(footPtr)
	binary.LittleEndian.PutUint32(foot, crc.Sum32())

	if _, err := w.Write(hdr); err != nil {
		return err
	}
	if len(body) > 0 {
		if _, err := w.Write(body); err != nil {
			return err
		}
	}
	if _, err := w.Write(foot); err != nil {
		return err
	}
	return nil
}

func readRecordAt(f *os.File, offset int64, segID int) (*Record, error) {
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
	key := binary.LittleEndian.Uint64(hdr[4:12])
	bodyLen := binary.LittleEndian.Uint32(hdr[12:16])

	var body []byte
	if bodyLen > 0 {
		body = make([]byte, bodyLen)
		if _, err := f.ReadAt(body, offset+headerLen); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, ErrTornRecord
			}
			return nil, fmt.Errorf("warm: read body at %d: %w", offset, err)
		}
	}

	footPtr := recordFootPool.Get().(*[]byte)
	footBuf := *footPtr
	defer recordFootPool.Put(footPtr)
	if _, err := f.ReadAt(footBuf, offset+headerLen+int64(bodyLen)); err != nil {
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
		if offset+headerLen > len(data) {
			return nil // torn trailing header
		}
		magic := binary.LittleEndian.Uint32(data[offset : offset+4])
		key := binary.LittleEndian.Uint64(data[offset+4 : offset+12])
		bodyLen := int(binary.LittleEndian.Uint32(data[offset+12 : offset+16]))

		recEnd := offset + headerLen + bodyLen + footerLen
		if recEnd > len(data) {
			return nil // torn trailing record
		}

		// CRC is computed over the mmap data directly — no copy needed
		// for the checksum itself.
		storedCRC := binary.LittleEndian.Uint32(data[offset+headerLen+bodyLen : recEnd])
		crc := crc32.New(crcTable)
		_, _ = crc.Write(data[offset : offset+headerLen])
		if bodyLen > 0 {
			_, _ = crc.Write(data[offset+headerLen : offset+headerLen+bodyLen])
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
			copy(body, data[offset+headerLen:offset+headerLen+bodyLen])
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
		rec, err := readRecordAt(f, offset, segID)
		if err != nil {
			if errors.Is(err, ErrTornRecord) {
				return nil
			}
			return err
		}
		if err := fn(*rec); err != nil {
			return err
		}
		offset += int64(headerLen + len(rec.Body) + footerLen)
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
// Records are streamed per-segment: live records from one segment are
// collected, the segment lock is released, and only then written to the
// temp store. Peak heap is O(1 segment) instead of O(total live bytes).
func (s *Store) Compact() error {
	s.metrics.IncCompactionTriggered()
	s.idxMu.RLock()
	idxSnap := make(map[uint64]warmLoc, len(s.index))
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
	newIndex, orderedKeys, written, err := s.compactSegments(tmp, idxSnap, compactDir, s.compactKeysBuf)
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

	// Hold s.mu.Lock across the segment-file swap + internal replacement
	// so that concurrent Put/Delete (which call activeSeg → newSegment
	// → openSegment) cannot hit missing files.
	//
	// The compact directory is a subdirectory of dir so it lives on the
	// same writable volume. This is required for read-only root
	// filesystems (e.g. Kubernetes readOnlyRootFilesystem: true) where
	// only the mounted PVC at dir is writable.
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := swapSegmentFiles(dir, compactDir); err != nil {
		return err
	}

	fresh, err := NewStore(Config{Dir: dir, MaxBytes: s.maxBytes, SegMax: s.segMax})
	if err != nil {
		return fmt.Errorf("compact: reopen: %w", err)
	}

	// Now safe to close old handles: new store is open, old inodes are
	// unlinked and will be freed when these fds close.
	for _, seg := range s.segs {
		_ = seg.f.Close()
	}
	s.segs = fresh.segs
	s.segByID = fresh.segByID
	// Replace the index and rebuild the SIEVE list in append order.
	// compactSegments returns keys in the order records were written to
	// the new store (by segID then offset), so we can build the SIEVE list
	// directly — no O(n log n) sort needed. The SIEVE tail holds the
	// oldest entries (first written), preserving the aging property.
	// All rebuilt entries start with visited=false — the first eviction
	// sweep after compaction will prefer the tail (oldest) entries.
	s.idxMu.Lock()
	s.index = newIndex
	// Clear the old SIEVE list in-place, returning entries to the pool.
	// The rebuilt list reuses the same pool, avoiding a multi-MB
	// allocation spike on compaction with millions of entries.
	s.evictList.Clear()
	for _, key := range orderedKeys {
		e, _ := s.evictList.Access(key, func(uint64) *sieve.Entry[uint64] { return nil })
		loc := s.index[key]
		loc.sieve = e
		s.index[key] = loc
	}
	s.idxMu.Unlock()
	s.stats.entries.Store(int64(len(newIndex)))
	// Recompute stats.bytes from the new index. The fresh store (NewStore)
	// doesn't run RecomputeStats, so its stats.bytes is 0 — using it would
	// leave evictToFit blind (it gates on stats.bytes == 0 → no eviction).
	// Every entry's size was set by compactSegments, so summing the index
	// gives the correct live-bytes count without a segment scan.
	var liveBytes int64
	for _, loc := range newIndex {
		liveBytes += loc.size
	}
	s.stats.bytes.Store(liveBytes)
	// Retain the grown buffer for the next compaction cycle.
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
	key  uint64
	body []byte
}

// compactSegments scans each source segment for live records and writes
// them to tmp, returning the new index, the keys in append order (by
// segID then offset — the order records were written to the new store),
// and the count. Segment locks are held only during the scan, not
// during cross-store writes. The ordered keys are appended to keysBuf
// (reset to zero length on entry), which the caller provides — typically
// a reusable buffer on the Store — to avoid a per-compaction allocation.
func (s *Store) compactSegments(tmp *Store, idxSnap map[uint64]warmLoc, compactDir string, keysBuf []uint64) (map[uint64]warmLoc, []uint64, int, error) {
	s.mu.RLock()
	segs := make([]*Segment, len(s.segs))
	copy(segs, s.segs)
	s.mu.RUnlock()

	orderedKeys := keysBuf[:0]
	newIndex := make(map[uint64]warmLoc, len(idxSnap))
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
			recSize := int64(headerLen + len(p.body) + footerLen)
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
