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
)

const (
	opPut    byte = 1
	opDelete byte = 2
	recLen        = 1 + 8 + 4 + 8 + 4 // 25 bytes

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

// entryBufPool pools 25-byte encode buffers to avoid allocation on
// the Enqueue path. Stores *[recLen]byte (pointer to fixed-size array),
// matching the recordHdrPool / recordFootPool pattern in warm.go.
var entryBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, recLen)
		return &buf
	},
}

// Entry is a single WAL record.
type Entry struct {
	Op     byte
	Key    uint64
	SegID  int32
	Offset int64
}

// Log is an append-only WAL file opened with O_DSYNC (Linux) or O_SYNC
// (other platforms), so every Write() is durable without explicit fsync.
//
// In synchronous mode (opened with Open), every Append/AppendBatch
// write is immediately durable. In async mode (opened with OpenAsync),
// the walSyncLoop goroutine batches Enqueue'd entries and writes them
// once per syncInterval.
type Log struct {
	mu   sync.Mutex
	f    *os.File
	path string

	// Async-mode fields. nil/zero when opened with Open (sync mode).
	syncCh       chan []byte        // buffered 4096; carries encoded entries
	flushCh      chan chan struct{} // per-caller done channels for Sync()
	stopCh       chan struct{}
	syncInterval time.Duration
	dropped      atomic.Int64
	lastSync     atomic.Int64 // Unix nanoseconds; 0 = never synced
	syncWg       sync.WaitGroup
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

// encodeEntry encodes an Entry into a pooled 25-byte buffer.
func encodeEntry(e Entry) []byte {
	bufp := entryBufPool.Get().(*[]byte)
	buf := *bufp
	buf[0] = e.Op
	binary.LittleEndian.PutUint64(buf[1:9], e.Key)
	binary.LittleEndian.PutUint32(buf[9:13], uint32(e.SegID))   //nolint:gosec // seg IDs are small
	binary.LittleEndian.PutUint64(buf[13:21], uint64(e.Offset)) //nolint:gosec // offsets are positive
	crc := crc32.Checksum(buf[:21], crcTable)
	binary.LittleEndian.PutUint32(buf[21:25], crc)
	return buf
}

// Append writes a single entry to the WAL. The write is durable via
// O_DSYNC/O_SYNC on the file descriptor — no explicit fsync needed.
// Use for synchronous logs (opened with Open).
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
		// Write failed: entries from batch[written:] were drained from
		// the channel but never persisted. Count them as dropped so the
		// operator metric reflects the loss and return their buffers to
		// the pool. LastSyncTime is left stale — the runbook tells
		// operators to alert when it lags past 2x sync_interval.
		l.dropped.Add(int64(len(batch) - written))
		for _, buf := range batch[written:] {
			entryBufPool.Put(&buf)
		}
	}
	l.mu.Unlock()
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
