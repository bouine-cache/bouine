// Package wal implements an append-only write-ahead log for the
// storage index. It records which keys are stored in which warm-tier
// segment at which offset, so the index can be rebuilt after a crash
// without scanning all segments.
//
// Record layout (little-endian):
//
//	[1]  op        PUT=1, DELETE=2
//	[8]  key       uint64
//	[4]  seg_id    int32 (PUT only; 0 for DELETE)
//	[8]  offset    int64 (PUT only; 0 for DELETE)
//	[4]  crc32c    checksum of the preceding bytes
//
// Total: 25 bytes per record (PUT), 25 bytes (DELETE, seg_id+offset
// are zero).
//
// The WAL is truncated after a successful checkpoint (when the warm
// tier and hot tier indices are known-good).
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

const (
	opPut    byte = 1
	opDelete byte = 2
	recLen        = 1 + 8 + 4 + 8 + 4 // 25 bytes
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Entry is a single WAL record.
type Entry struct {
	Op     byte
	Key    uint64
	SegID  int32
	Offset int64
}

// Log is an append-only WAL file.
type Log struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

// Open opens or creates a WAL file at path.
func Open(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600) //nolint:gosec // operator-configured path
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}
	return &Log{f: f, path: path}, nil
}

// Append writes a single entry to the WAL. The write is fsynced.
func (l *Log) Append(e Entry) error {
	buf := make([]byte, recLen)
	buf[0] = e.Op
	binary.LittleEndian.PutUint64(buf[1:9], e.Key)
	binary.LittleEndian.PutUint32(buf[9:13], uint32(e.SegID))   //nolint:gosec // seg IDs are small
	binary.LittleEndian.PutUint64(buf[13:21], uint64(e.Offset)) //nolint:gosec // offsets are positive

	crc := crc32.Checksum(buf[:21], crcTable)
	binary.LittleEndian.PutUint32(buf[21:25], crc)

	l.mu.Lock()
	defer l.mu.Unlock()

	if _, err := l.f.Write(buf); err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}
	return l.f.Sync()
}

// Replay reads all valid entries from the WAL and calls fn for each.
// Corrupt trailing records (partial writes) are silently skipped —
// the WAL is append-only and a partial last record means a crash
// interrupted the write.
func Replay(path string, fn func(Entry) error) error {
	f, err := os.Open(path) //nolint:gosec // operator-configured path
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("wal: open for replay %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	buf := make([]byte, recLen)
	for {
		if _, err := io.ReadFull(f, buf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return fmt.Errorf("wal: read: %w", err)
		}

		crc := crc32.Checksum(buf[:21], crcTable)
		stored := binary.LittleEndian.Uint32(buf[21:25])
		if crc != stored {
			return nil
		}

		e := Entry{
			Op:     buf[0],
			Key:    binary.LittleEndian.Uint64(buf[1:9]),
			SegID:  int32(binary.LittleEndian.Uint32(buf[9:13])),  //nolint:gosec // bounded by segment count,
			Offset: int64(binary.LittleEndian.Uint64(buf[13:21])), //nolint:gosec // file offsets,
		}
		if err := fn(e); err != nil {
			return err
		}
	}
}

// AppendBatch writes multiple entries to the WAL and syncs once. Use
// for batch writes where per-entry durability is not required — the
// entire batch is atomic: either all entries are durable or none are
// (the file is fsynced once after all writes). Existing callers that
// need per-entry durability continue to use Append (which syncs per
// entry).
func (l *Log) AppendBatch(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range entries {
		buf := make([]byte, recLen)
		buf[0] = e.Op
		binary.LittleEndian.PutUint64(buf[1:9], e.Key)
		binary.LittleEndian.PutUint32(buf[9:13], uint32(e.SegID))   //nolint:gosec // seg IDs are small
		binary.LittleEndian.PutUint64(buf[13:21], uint64(e.Offset)) //nolint:gosec // offsets are positive
		crc := crc32.Checksum(buf[:21], crcTable)
		binary.LittleEndian.PutUint32(buf[21:25], crc)
		if _, err := l.f.Write(buf); err != nil {
			return fmt.Errorf("wal: batch write: %w", err)
		}
	}
	return l.f.Sync()
}

// Truncate discards the WAL contents (called after a checkpoint).
func (l *Log) Truncate() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.f.Truncate(0); err != nil {
		return fmt.Errorf("wal: truncate: %w", err)
	}
	if _, err := l.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek: %w", err)
	}
	return l.f.Sync()
}

// Close closes the WAL file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}

// IsPut returns true if the entry is a PUT.
func (e Entry) IsPut() bool { return e.Op == opPut }

// IsDelete returns true if the entry is a DELETE.
func (e Entry) IsDelete() bool { return e.Op == opDelete }

// PutEntry creates a PUT WAL entry.
func PutEntry(key uint64, segID int32, offset int64) Entry {
	return Entry{Op: opPut, Key: key, SegID: segID, Offset: offset}
}

// DeleteEntry creates a DELETE WAL entry.
func DeleteEntry(key uint64) Entry {
	return Entry{Op: opDelete, Key: key}
}
