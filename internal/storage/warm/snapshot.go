package warm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"

	"github.com/bouine-cache/bouine/internal/storage/evictor"
	"github.com/bouine-cache/bouine/pkg/api"
)

const (
	snapMagic       uint32 = 0x49445853 // "SXDI"
	snapVersion     uint32 = 2
	snapEndMagic    uint32 = 0x464E4524 // "$END"
	snapHdrLen             = 32
	snapSegEntryLen        = 12 // seg_id (4) + seg_size (8)
	snapEntryLen           = 36 // key (16) + seg_id (4) + offset (8) + size (8)
	snapFooterLen          = 8  // footer_crc + end_magic
	snapFile               = "index.snap"
)

var snapCRCTable = crc32.MakeTable(crc32.Castagnoli)

type snapEntry struct {
	key    api.Key
	segID  int32
	offset int64
	size   int64
}

// SnapshotPath returns the path to the index snapshot file, or "" if
// no warm directory is configured.
func (s *Store) SnapshotPath() string {
	if s.dir == "" {
		return ""
	}
	return filepath.Join(s.dir, snapFile)
}

// WriteSnapshot writes the current index state to index.snap atomically.
// It takes a consistent copy of the index under idxMu.RLock (blocking
// writers for ~50ms at 10M entries), releases the lock, then writes the
// snapshot lock-free. The snapshot includes a segment table for O(S)
// validation on load.
func (s *Store) WriteSnapshot() error {
	s.idxMu.RLock()
	idxSnap := make(map[api.Key]warmLoc, len(s.index))
	for k, v := range s.index {
		idxSnap[k] = v
	}
	s.idxMu.RUnlock()

	return s.WriteSnapshotFromCopy(idxSnap)
}

// WriteSnapshotFromCopy writes a snapshot from a pre-copied index map.
// Used by checkpoint() which takes the copy as part of its crash-safe
// sequence. The caller must ensure idxSnap is not mutated during this
// call. The segByID map is copied internally under s.mu.RLock, so the
// caller does not need to synchronize access to it.
func (s *Store) WriteSnapshotFromCopy(idxSnap map[api.Key]warmLoc) error {
	// Copy segByID under s.mu.RLock. The segByID map is modified by
	// rebuildSegByID during compaction (under s.mu.Lock). Reading the
	// map reference without the lock and then iterating it after release
	// causes a concurrent map read and map write fatal crash.
	// segByID has O(segments) entries (~hundreds), so the copy is cheap.
	s.mu.RLock()
	segByID := make(map[int]*Segment, len(s.segByID))
	for id, seg := range s.segByID {
		segByID[id] = seg
	}
	s.mu.RUnlock()

	segSizes := make(map[int]int64)
	for _, loc := range idxSnap {
		if seg, ok := segByID[loc.segID]; ok {
			seg.mu.Lock()
			segSizes[loc.segID] = seg.size
			seg.mu.Unlock()
		}
	}

	segIDs := make([]int, 0, len(segSizes))
	for id := range segSizes {
		segIDs = append(segIDs, id)
	}
	slices.Sort(segIDs)

	entries := make([]snapEntry, 0, len(idxSnap))
	var totalBytes int64
	for key, loc := range idxSnap {
		entries = append(entries, snapEntry{
			key:    key,
			segID:  int32(loc.segID), //nolint:gosec // segID fits int32
			offset: loc.offset,
			size:   loc.size,
		})
		totalBytes += loc.size
	}
	slices.SortFunc(entries, func(a, b snapEntry) int {
		return bytes.Compare(a.key[:], b.key[:])
	})

	hdr, bodyData := encodeSnapshot(segIDs, segSizes, entries, totalBytes)
	return s.writeSnapshotFile(hdr, bodyData)
}

// writeSnapshotFile writes the header, body, and footer to a temp file,
// fsyncs, and atomically renames to the snapshot path.
func (s *Store) writeSnapshotFile(hdr, bodyData []byte) error {
	tmpPath := filepath.Join(s.dir, snapFile+".tmp")
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600) //nolint:gosec // operator-configured path
	if err != nil {
		return fmt.Errorf("warm: snapshot: open tmp: %w", err)
	}

	if _, err := f.Write(hdr); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("warm: snapshot: write header: %w", err)
	}

	if _, err := f.Write(bodyData); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("warm: snapshot: write body: %w", err)
	}

	footerCRC := crc32.Checksum(bodyData, snapCRCTable)
	footer := make([]byte, snapFooterLen)
	binary.LittleEndian.PutUint32(footer[0:4], footerCRC)
	binary.LittleEndian.PutUint32(footer[4:8], snapEndMagic)
	if _, err := f.Write(footer); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("warm: snapshot: write footer: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("warm: snapshot: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("warm: snapshot: close tmp: %w", err)
	}

	snapPath := s.SnapshotPath()
	if err := os.Rename(tmpPath, snapPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("warm: snapshot: rename: %w", err)
	}

	return nil
}

// encodeSnapshot builds the header, segment table, and entry bytes for
// a snapshot. Returns the header and the body (segment table + entries)
// so the caller can compute the footer CRC over the body.
func encodeSnapshot(segIDs []int, segSizes map[int]int64, entries []snapEntry, totalBytes int64) (hdr, body []byte) {
	hdr = make([]byte, snapHdrLen)
	binary.LittleEndian.PutUint32(hdr[0:4], snapMagic)
	binary.LittleEndian.PutUint32(hdr[4:8], snapVersion)
	binary.LittleEndian.PutUint64(hdr[8:16], uint64(len(entries)))
	binary.LittleEndian.PutUint64(hdr[16:24], uint64(totalBytes))  //nolint:gosec // totalBytes fits uint64
	binary.LittleEndian.PutUint32(hdr[24:28], uint32(len(segIDs))) //nolint:gosec // segment count fits uint32
	binary.LittleEndian.PutUint32(hdr[28:32], crc32.Checksum(hdr[0:28], snapCRCTable))

	segTbl := make([]byte, snapSegEntryLen*len(segIDs))
	for i, id := range segIDs {
		binary.LittleEndian.PutUint32(segTbl[i*snapSegEntryLen:], uint32(id))             //nolint:gosec // segment IDs are bounded
		binary.LittleEndian.PutUint64(segTbl[i*snapSegEntryLen+4:], uint64(segSizes[id])) //nolint:gosec // file sizes fit in uint64
	}

	entryData := make([]byte, snapEntryLen*len(entries))
	for i, e := range entries {
		off := i * snapEntryLen
		copy(entryData[off:off+16], e.key[:])
		binary.LittleEndian.PutUint32(entryData[off+16:], uint32(e.segID))  //nolint:gosec // segID fits uint32
		binary.LittleEndian.PutUint64(entryData[off+20:], uint64(e.offset)) //nolint:gosec // offset fits uint64
		binary.LittleEndian.PutUint64(entryData[off+28:], uint64(e.size))   //nolint:gosec // size fits uint64
	}

	body = make([]byte, 0, len(segTbl)+len(entryData))
	body = append(body, segTbl...)
	body = append(body, entryData...)
	return hdr, body
}

// validateSegmentTable checks that every segment ID in the snapshot
// exists on disk and that the on-disk size is at least as large as the
// snapshot size. Segments may have grown since the snapshot was taken
// (writes between checkpoint and restart append to segments and are
// captured by WAL replay), so actualSize > segSize is normal and safe.
// A segment smaller than the snapshot indicates data loss (compaction
// or truncation) and means the snapshot's offsets are stale — reject
// and fall back to WAL replay + segment scan.
func (s *Store) validateSegmentTable(segTbl []byte, segCount uint32) error {
	for i := range segCount {
		se := segTbl[i*snapSegEntryLen : (i+1)*snapSegEntryLen]
		segID := int(int32(binary.LittleEndian.Uint32(se[0:4]))) //nolint:gosec // segID fits int32
		segSize := int64(binary.LittleEndian.Uint64(se[4:12]))   //nolint:gosec // segSize fits int64

		s.mu.RLock()
		seg, exists := s.segByID[segID]
		s.mu.RUnlock()
		if !exists {
			return fmt.Errorf("%w: segment %d missing from disk", ErrSnapshotInvalid, segID)
		}
		seg.mu.Lock()
		actualSize := seg.size
		seg.mu.Unlock()
		if actualSize < segSize {
			return fmt.Errorf("%w: segment %d shrank (snapshot=%d, disk=%d)", ErrSnapshotInvalid, segID, segSize, actualSize)
		}
	}
	return nil
}

// LoadSnapshot reads the index snapshot and populates the in-memory
// index and stats counters. Returns ErrSnapshotInvalid if the snapshot
// is corrupt or references missing segments; the caller should fall
// back to WAL replay + RecomputeStats.
//
//nolint:funlen // 52 statements: snapshot loading is sequential
func (s *Store) LoadSnapshot(path string) error {
	data, err := os.ReadFile(path) //nolint:gosec // operator-configured path
	if err != nil {
		return err
	}

	minSize := snapHdrLen + snapFooterLen
	if len(data) < minSize {
		return fmt.Errorf("%w: file too small (%d bytes)", ErrSnapshotInvalid, len(data))
	}

	hdr := data[:snapHdrLen]
	if binary.LittleEndian.Uint32(hdr[0:4]) != snapMagic {
		return fmt.Errorf("%w: bad magic", ErrSnapshotInvalid)
	}
	if binary.LittleEndian.Uint32(hdr[4:8]) != snapVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrSnapshotInvalid, binary.LittleEndian.Uint32(hdr[4:8]))
	}
	storedHdrCRC := binary.LittleEndian.Uint32(hdr[28:32])
	if crc32.Checksum(hdr[0:28], snapCRCTable) != storedHdrCRC {
		return fmt.Errorf("%w: header CRC mismatch", ErrSnapshotInvalid)
	}

	entryCount := binary.LittleEndian.Uint64(hdr[8:16])
	totalBytes := int64(binary.LittleEndian.Uint64(hdr[16:24])) //nolint:gosec // total bytes fits int64
	segCount := binary.LittleEndian.Uint32(hdr[24:28])

	expectedSize := snapHdrLen + int(segCount)*snapSegEntryLen + int(entryCount)*snapEntryLen + snapFooterLen //nolint:gosec // entry count fits int
	if len(data) != expectedSize {
		return fmt.Errorf("%w: size mismatch (got %d, expected %d)", ErrSnapshotInvalid, len(data), expectedSize)
	}

	off := snapHdrLen
	segTbl := data[off : off+int(segCount)*snapSegEntryLen]
	off += int(segCount) * snapSegEntryLen

	if err := s.validateSegmentTable(segTbl, segCount); err != nil {
		return err
	}

	entryData := data[off : off+int(entryCount)*snapEntryLen] //nolint:gosec // entryCount fits int
	off += int(entryCount) * snapEntryLen                     //nolint:gosec // entryCount fits int

	bodyData := data[snapHdrLen:off]
	footerCRC := binary.LittleEndian.Uint32(data[off : off+4])
	if crc32.Checksum(bodyData, snapCRCTable) != footerCRC {
		return fmt.Errorf("%w: footer CRC mismatch", ErrSnapshotInvalid)
	}
	if binary.LittleEndian.Uint32(data[off+4:off+8]) != snapEndMagic {
		return fmt.Errorf("%w: bad end magic", ErrSnapshotInvalid)
	}

	s.idxMu.Lock()
	for k := range s.index {
		delete(s.index, k)
	}
	s.protectedCount.Store(0)           // snapshot entries load as unprotected
	if len(s.index) < int(entryCount) { //nolint:gosec // entryCount fits int
		s.index = make(map[api.Key]warmLoc, entryCount)
	}
	for i := range entryCount {
		ee := entryData[i*snapEntryLen : (i+1)*snapEntryLen]
		var key api.Key
		copy(key[:], ee[0:16])
		segID := int(int32(binary.LittleEndian.Uint32(ee[16:20]))) //nolint:gosec // segID fits int32
		offset := int64(binary.LittleEndian.Uint64(ee[20:28]))     //nolint:gosec // offset fits int64
		size := int64(binary.LittleEndian.Uint64(ee[28:36]))       //nolint:gosec // size fits int64

		e, _ := s.evictList.Access(key, func(k api.Key) *evictor.Entry[api.Key] {
			if loc, ok := s.index[k]; ok {
				return loc.entry
			}
			return nil
		})
		s.index[key] = warmLoc{segID: segID, offset: offset, size: size, entry: e}
	}
	s.idxMu.Unlock()

	s.stats.entries.Store(int64(entryCount)) //nolint:gosec // entryCount fits int64
	s.stats.bytes.Store(totalBytes)

	return nil
}

// ErrSnapshotInvalid indicates the snapshot file is corrupt or stale.
// Callers should fall back to WAL replay + RecomputeStats.
var ErrSnapshotInvalid = errors.New("warm: snapshot invalid")
