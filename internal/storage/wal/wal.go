// Package wal implements an append-only write-ahead log for the
// storage index. It records which keys are stored in which warm-tier
// segment at which offset, so the index can be rebuilt after a crash
// without scanning all segments.
//
// Record layout (little-endian). v3: all records use 128-bit (16-byte)
// keys. There is no v1/v2 decode path — a clean break with no backward
// compat (nothing runs in production). Existing WAL files are deleted
// on upgrade.
//
// v3 base (33 bytes, op=1 PUT / op=2 DELETE):
//
//	[1]  op        PUT=1, DELETE=2
//	[16] key       api.Key (128-bit)
//	[4]  seg_id    int32 (PUT only; 0 for DELETE)
//	[8]  offset    int64 (PUT only; 0 for DELETE)
//	[4]  crc32c    checksum of the preceding 29 bytes
//
// v3 +size (41 bytes, op=3 PUT only):
//
//	[1]  op        PUT_V2=3
//	[16] key       api.Key (128-bit)
//	[4]  seg_id    int32
//	[8]  offset    int64
//	[8]  size      int64 (on-disk record size: HeaderLen + body + FooterLen)
//	[4]  crc32c    checksum of the preceding 37 bytes
//
// The +size record eliminates the need for RecomputeStats after WAL
// replay because the record size is persisted. Replay auto-detects the
// two widths by checking the op byte: opPutV2 (3) records are 41 bytes,
// all others are 33 bytes. This avoids a format header or magic number.
//
// The WAL is truncated after a successful checkpoint (when the warm
// tier and hot tier indices are known-good).
//
// # Async mode
//
// OpenAsync starts a background goroutine (walSyncLoop) that batches
// channel-enqueued entries and writes them to the O_DSYNC/O_SYNC file
// descriptor once per syncInterval (default 100 ms). This eliminates
// the goroutine serialization caused by holding l.mu across f.Sync()
// on every Append call (issue #220). With O_DSYNC, each Write() is
// already durable — the sync loop's role is to drain the channel and
// update lastSyncTime. Callers use Enqueue/EnqueueBatch (non-blocking,
// drop-on-full) instead of Append/AppendBatch. rebuildIndexFromScan is
// the durability backstop.
//
// Open stays synchronous for tests and rewriteWAL tmp files.
// Enqueue on a sync-only log (syncCh == nil) falls back to Append.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
)

const (
	opPut    byte = 1
	opDelete byte = 2
	opPutV2  byte = 3                      // PUT with record size (+size variant)
	recLen        = 1 + 16 + 4 + 8 + 4     // 33 bytes (v3 base: 128-bit key)
	recLenV2      = 1 + 16 + 4 + 8 + 8 + 4 // 41 bytes (v3 +size: adds 8-byte size)
	crcLen        = 29                     // bytes covered by CRC in v3 base
	crcLenV2      = 37                     // bytes covered by CRC in v3 +size

	// syncChSize is the bounded channel for async WAL entries.
	// Matches existing tombstoneQueue / warmEvictQueue patterns.
	// At 2000 req/s with 100 ms sync interval, ~200 entries/cycle
	// arrive — well within the 4096 buffer.
	syncChSize = 4096

	// DefaultSyncInterval is the default WAL fsync batching interval
	// for async mode (ADR-0024).
	DefaultSyncInterval = 100 * time.Millisecond
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// entryBufPool pools 41-byte encode buffers (v3 +size max) to avoid
// allocation on the Enqueue path. Base entries use the first 33 bytes and
// return a sub-slice. Stores *[recLenV2]byte (pointer to fixed-size array),
// matching the recordHdrPool / recordFootPool pattern in warm.go.
var entryBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, recLenV2)
		return &buf
	},
}

// Entry is a single WAL record.
type Entry struct {
	Op     byte
	Key    api.Key
	SegID  int32
	Offset int64
	Size   int64 // +size variant only: on-disk record size (0 for base entries)
}

// Log is an append-only WAL file opened with O_DSYNC (Linux) or O_SYNC
// (other platforms), so every Write() is durable without explicit fsync.
//
// In synchronous mode (opened with Open), every Append/AppendBatch
// write is immediately durable. In async mode (opened with OpenAsync),
// the walSyncLoop goroutine batches Enqueue'd entries and writes them
// once per syncInterval.
type Log struct {
	f *os.File
	// Async-mode fields. nil/zero when opened with Open (sync mode).
	syncCh  chan []byte        // buffered 4096; carries encoded entries
	flushCh chan chan struct{} // per-caller done channels for Sync()
	stopCh  chan struct{}
	// metrics holds Prometheus collectors for WAL write operations.
	// Nil when metrics are not registered (tests, sync-only mode).
	metrics      *Metrics
	path         string
	syncWg       sync.WaitGroup
	syncInterval time.Duration
	dropped      atomic.Int64
	lastSync     atomic.Int64 // Unix nanoseconds; 0 = never synced
	mu           sync.Mutex
}

// Open opens or creates a WAL file at path in synchronous mode.
// Every Append/AppendBatch write is flushed to disk via O_DSYNC (Linux)
// or O_SYNC (other platforms) — no explicit fsync is needed.
func Open(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND|syncFlag, 0o600) //nolint:gosec // operator-configured path
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}
	return &Log{f: f, path: path}, nil
}

// OpenAsync opens or creates a WAL file at path in async mode and starts
// the walSyncLoop goroutine. Enqueue'd entries are batched and written
// with O_DSYNC/O_SYNC, so every Write() is durable without explicit fsync.
// The sync loop still updates lastSyncTime and handles flush requests.
//
// syncInterval <= 0 selects synchronous mode: Enqueue falls back to
// Append (same as Open). The sync loop is not started.
func OpenAsync(path string, syncInterval time.Duration) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND|syncFlag, 0o600) //nolint:gosec // operator-configured path
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}
	l := &Log{
		f:            f,
		path:         path,
		syncInterval: syncInterval,
	}
	if syncInterval <= 0 {
		// Synchronous mode: no sync loop, Enqueue falls back to Append.
		return l, nil
	}
	l.syncCh = make(chan []byte, syncChSize)
	l.flushCh = make(chan chan struct{}, 1)
	l.stopCh = make(chan struct{})
	l.syncWg.Add(1)
	go l.walSyncLoop()
	return l, nil
}

// encodeEntry encodes an Entry into a pooled buffer. +size entries
// (opPutV2) produce a 41-byte buffer; base entries produce a 33-byte
// sub-slice. The pool stores 41-byte buffers; reslicing to full capacity
// on Get ensures +size writes never panic even after a base sub-slice
// was returned.
func encodeEntry(e Entry) []byte {
	bufp := entryBufPool.Get().(*[]byte)
	buf := (*bufp)[:cap(*bufp)] // reslice to full capacity (always recLenV2)
	buf[0] = e.Op
	copy(buf[1:17], e.Key[:])
	binary.LittleEndian.PutUint32(buf[17:21], uint32(e.SegID))  //nolint:gosec // seg IDs are small
	binary.LittleEndian.PutUint64(buf[21:29], uint64(e.Offset)) //nolint:gosec // offsets are positive
	if e.Op == opPutV2 {
		binary.LittleEndian.PutUint64(buf[29:37], uint64(e.Size)) //nolint:gosec // size is positive
		crc := crc32.Checksum(buf[:crcLenV2], crcTable)
		binary.LittleEndian.PutUint32(buf[37:41], crc)
		return buf[:recLenV2]
	}
	crc := crc32.Checksum(buf[:crcLen], crcTable)
	binary.LittleEndian.PutUint32(buf[29:33], crc)
	return buf[:recLen]
}

// Append writes a single entry to the WAL. The write is durable via
// O_DSYNC/O_SYNC on the file descriptor — no explicit fsync needed.
// Use for synchronous logs (opened with Open). Handles both v1 (25-byte)
// and v2 (33-byte) formats based on the entry's Op.
func (l *Log) Append(e Entry) error {
	buf := encodeEntry(e)
	defer entryBufPool.Put(&buf)

	l.mu.Lock()
	defer l.mu.Unlock()

	if _, err := l.f.Write(buf); err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}
	return nil
}

// Enqueue encodes and enqueues a single entry for async fsync.
// Non-blocking: if the channel is full, the entry is dropped (counter
// incremented) and nil is returned — rebuildIndexFromScan is the
// durability backstop.
//
// On a sync-only log (syncCh == nil, opened with Open or
// syncInterval <= 0), falls back to synchronous Append.
func (l *Log) Enqueue(e Entry) error {
	if l.syncCh == nil {
		return l.Append(e)
	}
	buf := encodeEntry(e)
	l.metrics.IncWriteTotal(1)
	select {
	case l.syncCh <- buf:
	default:
		l.dropped.Add(1)
		entryBufPool.Put(&buf)
	}
	return nil
}

// EnqueueBatch enqueues multiple entries for async fsync. Each entry is
// sent individually with non-blocking sends. If the channel fills
// mid-batch, the remaining entries are dropped (counter incremented)
// and their buffers returned to the pool.
//
// On a sync-only log (syncCh == nil), falls back to synchronous
// AppendBatch.
func (l *Log) EnqueueBatch(entries []Entry) {
	if l.syncCh == nil {
		_ = l.AppendBatch(entries)
		return
	}
	for _, e := range entries {
		buf := encodeEntry(e)
		l.metrics.IncWriteTotal(1)
		select {
		case l.syncCh <- buf:
		default:
			l.dropped.Add(1)
			entryBufPool.Put(&buf)
		}
	}
}

// walSyncLoop drains the sync channel, writes all pending entries to
// the file under l.mu, and fsyncs once per cycle. Triggered by:
//   - tick.C  — periodic batching (every syncInterval)
//   - flushCh — immediate flush requested by Sync() (carries a
//     per-caller done channel that is closed once the flush completes)
//   - stopCh  — Close() signals shutdown; loop drains + fsyncs, closes
//     any pending flush done channels, then exits.
func (l *Log) walSyncLoop() {
	defer l.syncWg.Done()
	ticker := time.NewTicker(l.syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			l.drainAndSync(nil)
		case done := <-l.flushCh:
			l.drainAndSync(done)
		case <-l.stopCh:
			l.drainAndSync(nil)
			// Close any flush requests that arrived during shutdown
			// so Sync() callers blocked on <-done don't deadlock.
			for {
				select {
				case done := <-l.flushCh:
					close(done)
				default:
					return
				}
			}
		}
	}
}

// drainAndSync drains all pending entries from syncCh and writes them
// to the file in a single batch under l.mu, then fsyncs once. If done
// is non-nil it is closed after the flush completes (or after the
// channel is found empty) so the requesting Sync() caller unblocks.
func (l *Log) drainAndSync(done chan struct{}) {
	var batch [][]byte
	for {
		select {
		case buf := <-l.syncCh:
			batch = append(batch, buf)
		default:
			goto write
		}
	}
write:
	if len(batch) == 0 {
		if done != nil {
			close(done)
		}
		return
	}
	writeStart := time.Now()
	l.mu.Lock()
	written := 0
	var writeErr error
	for _, buf := range batch {
		if _, err := l.f.Write(buf); err != nil {
			writeErr = err
			break
		}
		written++
		entryBufPool.Put(&buf)
	}
	if writeErr == nil {
		l.lastSync.Store(time.Now().UnixNano())
	} else {
		l.dropped.Add(int64(len(batch) - written))
		for _, buf := range batch[written:] {
			entryBufPool.Put(&buf)
		}
	}
	l.mu.Unlock()
	l.metrics.ObserveWriteDuration(time.Since(writeStart))
	if done != nil {
		close(done)
	}
}

// Sync triggers an immediate flush of pending entries and waits for
// the sync loop to complete the drain. With O_DSYNC/O_SYNC on the file
// descriptor, each Write() is already durable — Sync() ensures all
// channel-enqueued entries have been written and updates lastSyncTime.
// Blocks until the sync loop closes the per-call done channel. Safe to
// call concurrently; callers are serialized by the buffered flushCh.
// Returns nil if the sync loop has already exited (Close in progress).
//
// On a sync-only log (syncCh == nil), this is a no-op (Append writes
// are already durable via the sync flag).
func (l *Log) Sync() error {
	if l.syncCh == nil {
		return nil
	}
	done := make(chan struct{})
	select {
	case l.flushCh <- done:
	case <-l.stopCh:
		return nil
	}
	select {
	case <-done:
		return nil
	case <-l.stopCh:
		return nil
	}
}

// SetMetrics injects Prometheus collectors for WAL write operations.
// Must be called before the first Enqueue/EnqueueBatch. Nil is safe
// (all metric methods are no-ops on a nil Metrics).
func (l *Log) SetMetrics(m *Metrics) {
	l.metrics = m
}

// QueueDepth returns the current number of entries buffered in the
// async sync channel. Returns 0 for sync-only logs.
func (l *Log) QueueDepth() int {
	if l.syncCh == nil {
		return 0
	}
	return len(l.syncCh)
}

// DroppedEntries returns the number of WAL entries dropped because the
// async channel was full, and resets the counter to zero (caller owns
// the delta). Use for Prometheus metric export.
func (l *Log) DroppedEntries() int64 {
	return l.dropped.Swap(0)
}

// LastSyncTime returns the Unix nanosecond timestamp of the last
// successful WAL fsync. Returns 0 if the sync loop has never completed
// a cycle.
func (l *Log) LastSyncTime() time.Time {
	ns := l.lastSync.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// Replay reads all valid entries from the WAL and calls fn for each.
// Corrupt trailing records (partial writes) are silently skipped —
// the WAL is append-only and a partial last record means a crash
// interrupted the write. Auto-detects the two v3 widths by checking the
// op byte: opPutV2 (3) records are 41 bytes, all others are 33 bytes.
func Replay(path string, fn func(Entry) error) error {
	f, err := os.Open(path) //nolint:gosec // operator-configured path
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("wal: open for replay %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	buf := make([]byte, recLenV2) // max record size
	for {
		if _, err := io.ReadFull(f, buf[:recLen]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return fmt.Errorf("wal: read: %w", err)
		}

		if buf[0] == opPutV2 {
			// +size record: read 8 more bytes for size field
			if _, err := io.ReadFull(f, buf[recLen:]); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					return nil
				}
				return fmt.Errorf("wal: read v2: %w", err)
			}
			crc := crc32.Checksum(buf[:crcLenV2], crcTable)
			stored := binary.LittleEndian.Uint32(buf[37:41])
			if crc != stored {
				return nil
			}
			var key api.Key
			copy(key[:], buf[1:17])
			e := Entry{
				Op:     opPutV2,
				Key:    key,
				SegID:  int32(binary.LittleEndian.Uint32(buf[17:21])), //nolint:gosec // bounded by segment count
				Offset: int64(binary.LittleEndian.Uint64(buf[21:29])), //nolint:gosec // file offsets
				Size:   int64(binary.LittleEndian.Uint64(buf[29:37])), //nolint:gosec // record sizes
			}
			if err := fn(e); err != nil {
				return err
			}
		} else {
			// base record: 33 bytes already read
			crc := crc32.Checksum(buf[:crcLen], crcTable)
			stored := binary.LittleEndian.Uint32(buf[29:33])
			if crc != stored {
				return nil
			}
			var key api.Key
			copy(key[:], buf[1:17])
			e := Entry{
				Op:     buf[0],
				Key:    key,
				SegID:  int32(binary.LittleEndian.Uint32(buf[17:21])), //nolint:gosec // bounded by segment count
				Offset: int64(binary.LittleEndian.Uint64(buf[21:29])), //nolint:gosec // file offsets
			}
			if err := fn(e); err != nil {
				return err
			}
		}
	}
}

// AppendBatch writes multiple entries to the WAL and syncs once. Use
// for batch writes where per-entry durability is not required — the
// batch is flushed with a single fsync after all writes. On partial
// failure (Write error on entry N), entries 0..N-1 may already be in
// the file buffer and the file is not truncated; callers that need
// all-or-nothing semantics must use a temp file + rename. Existing
// callers that need per-entry durability continue to use Append (which
// syncs per entry).
func (l *Log) AppendBatch(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range entries {
		buf := encodeEntry(e)
		if _, err := l.f.Write(buf); err != nil {
			entryBufPool.Put(&buf)
			return fmt.Errorf("wal: batch write: %w", err)
		}
		entryBufPool.Put(&buf)
	}
	return nil
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

// Close stops the sync loop (if running), flushes pending entries, and
// closes the WAL file. In async mode, the sync loop drains remaining
// entries and fsyncs before exiting.
func (l *Log) Close() error {
	if l.syncCh != nil {
		// Stop the sync loop: close stopCh, wait for drain + final fsync.
		close(l.stopCh)
		l.syncWg.Wait()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}

// IsPut returns true if the entry is a PUT (v1 or v2).
func (e Entry) IsPut() bool { return e.Op == opPut || e.Op == opPutV2 }

// IsDelete returns true if the entry is a DELETE.
func (e Entry) IsDelete() bool { return e.Op == opDelete }

// HasSize reports whether this entry carries a v2 record size. v1
// entries have Size == 0 and must fall back to RecomputeStats for size
// backfill after replay.
func (e Entry) HasSize() bool { return e.Op == opPutV2 }

// PutEntry creates a base PUT WAL entry (no record size). Maintained for
// callers that don't have the size; replay backfills size via RecomputeStats.
func PutEntry(key api.Key, segID int32, offset int64) Entry {
	return Entry{Op: opPut, Key: key, SegID: segID, Offset: offset}
}

// PutEntryWithSize creates a +size PUT WAL entry that includes the on-disk
// record size. Replay can set the index size directly, avoiding a full
// segment scan (RecomputeStats) on startup.
func PutEntryWithSize(key api.Key, segID int32, offset, size int64) Entry {
	return Entry{Op: opPutV2, Key: key, SegID: segID, Offset: offset, Size: size}
}

// DeleteEntry creates a DELETE WAL entry.
func DeleteEntry(key api.Key) Entry {
	return Entry{Op: opDelete, Key: key}
}
