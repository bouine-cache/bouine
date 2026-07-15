//go:build linux

package warm

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"golang.org/x/sys/unix"

	"github.com/bouine-cache/bouine/internal/platform"
)

// tryMmap mmaps the segment file read-only for zero-syscall point reads.
// Only called on inactive (sealed) segments. The mapping uses MAP_SHARED
// with MADV_RANDOM for random-access workloads. On failure, seg.mmap stays
// nil and the segment falls back to pread.
//
// seg.mu is acquired to serialize concurrent tryMmap calls from multiple
// Get goroutines holding s.mu.RLock. Without seg.mu, two goroutines would
// both pass the nil check, both call Mmap, and leak the first mapping.
// The double-check after acquiring seg.mu prevents the duplicate.
func (seg *Segment) tryMmap() {
	if seg.mmap != nil || seg.size <= 0 || seg.f == nil {
		return
	}
	seg.mu.Lock()
	defer seg.mu.Unlock()
	if seg.mmap != nil {
		return
	}
	data, err := unix.Mmap(int(seg.f.Fd()), 0, int(seg.size),
		unix.PROT_READ, unix.MAP_SHARED|platform.MmapPopulate) //nolint:gosec // fd is a small non-negative integer
	if err != nil {
		return
	}
	_ = unix.Madvise(data, unix.MADV_RANDOM)
	seg.mmap = data
}

// munmap removes the mmap mapping for a segment. The caller MUST hold
// s.mu.Lock() (Store-level), which prevents concurrent reads. seg.mu is
// also acquired to serialize with concurrent tryMmap calls (which can
// run under s.mu.RLock from Get).
func (seg *Segment) munmap() {
	seg.mu.Lock()
	defer seg.mu.Unlock()
	if seg.mmap == nil {
		return
	}
	_ = unix.Munmap(seg.mmap)
	seg.mmap = nil
}

// munmapAll removes mmap mappings for all segments. Used by Compact and Close.
// The caller MUST hold s.mu.Lock().
func munmapAll(segs []*Segment) {
	for _, seg := range segs {
		seg.munmap()
	}
}

// readRecordAtMmap reads a record from the mmap'd region. Returns nil, nil
// if the segment is not mmap'd (caller should fall back to pread).
// The body is copied out of the mmap region because Compact may munmap
// the segment after s.mu.RLock is released.
//
// seg.mu is acquired briefly to snapshot seg.mmap (a []byte slice header
// is 3 words and cannot be read atomically). Once snapshotted, the
// mapping is valid for the duration of the caller's s.mu.RLock because
// munmap only happens under s.mu.Lock (in Compact/Close).
func readRecordAtMmap(seg *Segment, offset int64, size int64) (*Record, error) {
	seg.mu.Lock()
	mmapData := seg.mmap
	seg.mu.Unlock()
	if mmapData == nil || size <= 0 {
		return nil, nil
	}
	if offset+size > int64(len(mmapData)) {
		return nil, ErrTornRecord
	}
	data := mmapData[offset : offset+size]

	magic := binary.LittleEndian.Uint32(data[0:4])
	key := binary.LittleEndian.Uint64(data[4:12])
	bodyLen := binary.LittleEndian.Uint32(data[12:16])

	if uint64(bodyLen) > uint64(len(data))-uint64(HeaderLen+FooterLen) {
		return nil, ErrTornRecord
	}

	body := make([]byte, bodyLen)
	copy(body, data[HeaderLen:HeaderLen+bodyLen])

	storedCRC := binary.LittleEndian.Uint32(data[len(data)-FooterLen:])
	if crc32.Checksum(data[:len(data)-FooterLen], crcTable) != storedCRC {
		return nil, fmt.Errorf("warm: CRC mismatch at seg %d offset %d", seg.ID, offset)
	}

	return &Record{
		Key:    key,
		Body:   body,
		IsTomb: magic == magicDead,
		Offset: offset,
		SegID:  seg.ID,
	}, nil
}

// ensureMmapOpened ensures the segment has an mmap mapping if it is inactive.
// Called lazily from Get for segments that were not mmap'd at creation time
// (e.g., segments created by compaction or startup WAL replay).
// The caller MUST hold s.mu (RLock or Lock) and must have already called
// seg.readers.Add(1) so that fdCache eviction cannot close the fd mid-mmap.
func (seg *Segment) ensureMmapOpened(isActive bool) {
	if isActive || seg.mmap != nil {
		return
	}
	_ = seg.ensureOpen()
	seg.tryMmap()
}
