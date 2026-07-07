// Package warm implements the L1 warm-tier disk storage for bouine.
//
// The warm tier stores cache objects in append-only segmented files
// backed by mmap. Each segment is a fixed-size file (default 64 MiB)
// containing a sequence of records. Each record has a CRC32C footer
// for integrity validation.
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
)

const (
	magicLive uint32 = 0x424F5549 // "BOUI"
	magicDead uint32 = 0x44454144 // "DEAD"
	headerLen        = 4 + 8 + 4  // magic + key + body_len
	footerLen        = 4          // crc32c
	segExt           = ".seg"
)

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
// the warm-tier write (the hot tier already holds the object).
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
type warmLoc struct {
	segID  int
	offset int64
	size   int64
}

// Store is the warm-tier disk store.
type Store struct {
	dir      string
	maxBytes int64
	segMax   int64
	mu       sync.RWMutex
	segs     []*Segment
	nextID   atomic.Int32
	stats    warmStats
	idxMu    sync.RWMutex
	index    map[uint64]warmLoc
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
		dir:      cfg.Dir,
		maxBytes: cfg.MaxBytes,
		segMax:   cfg.SegMax,
		index:    make(map[uint64]warmLoc),
	}
	if err := s.openExisting(); err != nil {
		return nil, err
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
// a budget check; tombstones are reclaimed by compaction. This means
// diskBytes can transiently exceed maxBytes after deletes, and the
// next Put will be rejected until compaction reclaims the dead space.
func (s *Store) Put(key uint64, body []byte) (segID int, offset int64, err error) {
	recSize := int64(headerLen + len(body) + footerLen)

	// Enforce total disk budget before appending. maxBytes == 0 means
	// no limit (backward compatible with the default).
	if s.maxBytes > 0 {
		if s.diskBytes()+recSize > s.maxBytes {
			return 0, 0, fmt.Errorf("warm: put %d bytes: %w", recSize, ErrOverBudget)
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
	s.stats.entries.Add(1)
	s.stats.bytes.Add(recSize)

	s.idxMu.Lock()
	s.index[key] = warmLoc{segID: seg.ID, offset: off, size: recSize}
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
// Compact from swapping segments) → s.idxMu.RLock (protects the
// index map against concurrent Put/Delete writes).
func (s *Store) Get(key uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.idxMu.RLock()
	loc, ok := s.index[key]
	s.idxMu.RUnlock()
	if !ok {
		return nil, nil
	}

	var seg *Segment
	for _, ss := range s.segs {
		if ss.ID == loc.segID {
			seg = ss
			break
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
	return rec.Body, nil
}

// SetIndex adds or replaces an index entry. Called during WAL replay
// on startup to rebuild the index from persisted write history. The
// size is set to 0 because the WAL does not record record sizes;
// RecomputeStats fills it in by scanning segments after replay.
func (s *Store) SetIndex(key uint64, segID int, offset int64) {
	s.idxMu.Lock()
	s.index[key] = warmLoc{segID: segID, offset: offset}
	s.idxMu.Unlock()
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
	delete(s.index, key)
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
func (s *Store) dropStaleIndex(key uint64, stale warmLoc) {
	s.idxMu.Lock()
	if cur, ok := s.index[key]; ok && cur == stale {
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
// anti-entropy knows which keys the node owns, not just which are in RAM.
func (s *Store) Keys() []uint64 {
	s.idxMu.RLock()
	defer s.idxMu.RUnlock()
	keys := make([]uint64, 0, len(s.index))
	for k := range s.index {
		keys = append(keys, k)
	}
	return keys
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
	return firstErr
}

// diskBytes returns the total on-disk size of all segments. This is
// the sum of each segment's current file size, not stats.bytes (which
// only counts live record bytes and is reduced by tombstones and
// compaction). The total disk footprint is the correct figure for
// MaxBytes enforcement because the OS sees the file sizes, not the
// logical live-byte count.
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
	hdr := make([]byte, headerLen)
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

	foot := make([]byte, footerLen)
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
	offset := int64(0)
	for offset < info.Size() {
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
	diskBytes := int64(0)
	s.mu.RLock()
	for _, seg := range s.segs {
		seg.mu.Lock()
		diskBytes += seg.size
		seg.mu.Unlock()
	}
	s.mu.RUnlock()
	if diskBytes == 0 {
		return false
	}
	liveFraction := float64(total) / float64(diskBytes)
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
	s.idxMu.RLock()
	idxSnap := make(map[uint64]warmLoc, len(s.index))
	for k, v := range s.index {
		idxSnap[k] = v
	}
	s.idxMu.RUnlock()

	dir := s.dir
	compactDir := filepath.Join(dir, ".compact")
	_ = os.RemoveAll(compactDir) // stale dir from a previous failed run
	tmp, err := NewStore(Config{Dir: compactDir, MaxBytes: s.maxBytes, SegMax: s.segMax})
	if err != nil {
		return fmt.Errorf("compact: create temp store: %w", err)
	}
	newIndex, written, err := s.compactSegments(tmp, idxSnap, compactDir)
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
	s.idxMu.Lock()
	s.index = newIndex
	s.idxMu.Unlock()
	s.stats.entries.Store(int64(len(newIndex)))
	s.stats.bytes.Store(fresh.stats.bytes.Load())
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
// them to tmp, returning the new index and count. Segment locks are held
// only during the scan, not during cross-store writes.
func (s *Store) compactSegments(tmp *Store, idxSnap map[uint64]warmLoc, compactDir string) (map[uint64]warmLoc, int, error) {
	s.mu.RLock()
	segs := make([]*Segment, len(s.segs))
	copy(segs, s.segs)
	s.mu.RUnlock()

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
			return nil, 0, fmt.Errorf("compact: scan: %w", scanErr)
		}
		for _, p := range pending {
			segID, offset, wErr := tmp.Put(p.key, p.body)
			if wErr != nil {
				_ = tmp.Close()
				_ = os.RemoveAll(compactDir)
				return nil, 0, fmt.Errorf("compact: write: %w", wErr)
			}
			recSize := int64(headerLen + len(p.body) + footerLen)
			newIndex[p.key] = warmLoc{segID: segID, offset: offset, size: recSize}
			written++
		}
	}
	return newIndex, written, nil
}
