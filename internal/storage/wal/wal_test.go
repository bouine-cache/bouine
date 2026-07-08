package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func tmpWAL(t *testing.T) (*Log, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.wal")
	l, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l, path
}

func TestAppendAndReplay(t *testing.T) {
	t.Parallel()
	l, path := tmpWAL(t)

	entries := []Entry{
		PutEntry(100, 0, 0),
		PutEntry(200, 0, 1024),
		DeleteEntry(100),
		PutEntry(300, 1, 0),
	}
	for _, e := range entries {
		if err := l.Append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	var replayed []Entry
	err := Replay(path, func(e Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != len(entries) {
		t.Fatalf("replayed %d entries, want %d", len(replayed), len(entries))
	}
	for i, e := range replayed {
		if e.Op != entries[i].Op || e.Key != entries[i].Key {
			t.Errorf("[%d] op=%d key=%d, want op=%d key=%d",
				i, e.Op, e.Key, entries[i].Op, entries[i].Key)
		}
	}
}

func TestReplay_EmptyFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "empty.wal")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	if err != nil {
		t.Fatalf("replay empty: %v", err)
	}
	if count != 0 {
		t.Fatalf("count = %d", count)
	}
}

func TestReplay_MissingFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "missing.wal")
	err := Replay(path, func(_ Entry) error { return nil })
	if err != nil {
		t.Fatalf("replay missing: %v", err)
	}
}

func TestReplay_TruncatedRecord(t *testing.T) {
	t.Parallel()
	l, path := tmpWAL(t)
	if err := l.Append(PutEntry(1, 0, 0)); err != nil {
		t.Fatalf("append: %v", err)
	}
	_ = l.Close()

	// Truncate the file to simulate a partial write.
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := f.Stat()
	_ = f.Truncate(info.Size() - 5) // chop off part of the CRC
	_ = f.Close()

	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	if err != nil {
		t.Fatalf("replay truncated: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 valid records from truncated WAL, got %d", count)
	}
}

func TestTruncate(t *testing.T) {
	t.Parallel()
	l, path := tmpWAL(t)
	for range 5 {
		if err := l.Append(PutEntry(1, 0, 0)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := l.Truncate(); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	var count int
	err := Replay(path, func(_ Entry) error { count++; return nil })
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 after truncate, got %d", count)
	}
}

func TestEntryHelpers(t *testing.T) {
	t.Parallel()
	p := PutEntry(42, 3, 1024)
	if !p.IsPut() || p.IsDelete() {
		t.Fatal("PutEntry flags wrong")
	}
	d := DeleteEntry(42)
	if !d.IsDelete() || d.IsPut() {
		t.Fatal("DeleteEntry flags wrong")
	}
}

func tmpAsyncWAL(t *testing.T, syncInterval time.Duration) (*Log, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "async.wal")
	l, err := OpenAsync(path, syncInterval)
	if err != nil {
		t.Fatalf("open async: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l, path
}

func TestAsyncEnqueueSyncReplay(t *testing.T) {
	t.Parallel()
	l, path := tmpAsyncWAL(t, 50*time.Millisecond)

	entries := []Entry{
		PutEntry(100, 0, 0),
		PutEntry(200, 0, 1024),
		DeleteEntry(100),
		PutEntry(300, 1, 0),
	}
	for _, e := range entries {
		if err := l.Enqueue(e); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	if err := l.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	var replayed []Entry
	err := Replay(path, func(e Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != len(entries) {
		t.Fatalf("replayed %d entries, want %d", len(replayed), len(entries))
	}
	for i, e := range replayed {
		if e.Op != entries[i].Op || e.Key != entries[i].Key {
			t.Errorf("[%d] op=%d key=%d, want op=%d key=%d",
				i, e.Op, e.Key, entries[i].Op, entries[i].Key)
		}
	}
}

func TestAsyncEnqueueBatchSyncReplay(t *testing.T) {
	t.Parallel()
	l, path := tmpAsyncWAL(t, 50*time.Millisecond)

	entries := []Entry{
		PutEntry(1, 0, 0),
		PutEntry(2, 0, 100),
		PutEntry(3, 1, 200),
		DeleteEntry(2),
	}
	l.EnqueueBatch(entries)

	if err := l.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	var count int
	err := Replay(path, func(_ Entry) error { count++; return nil })
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if count != len(entries) {
		t.Fatalf("replayed %d, want %d", count, len(entries))
	}
}

func TestAsyncCloseFlushesPending(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "close.wal")
	l, err := OpenAsync(path, 10*time.Second) // long interval so Close must flush
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	for i := range 10 {
		if err := l.Enqueue(PutEntry(uint64(i+1), 0, int64(i*100))); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if count != 10 {
		t.Fatalf("expected 10 entries flushed on close, got %d", count)
	}
}

func TestAsyncDropOnFull(t *testing.T) {
	t.Parallel()
	// Use a very long sync interval so the channel fills without
	// the loop draining. syncChSize is 4096; send more than that.
	path := filepath.Join(t.TempDir(), "drop.wal")
	l, err := OpenAsync(path, 10*time.Second)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	sent := syncChSize + 100
	for i := range sent {
		_ = l.Enqueue(PutEntry(uint64(i+1), 0, 0))
	}

	dropped := l.DroppedEntries()
	if dropped == 0 {
		t.Fatalf("expected some dropped entries, got 0")
	}
}

func TestOpenSyncVsAsync(t *testing.T) {
	t.Parallel()
	// Open (sync) must have nil syncCh.
	syncLog, syncPath := tmpWAL(t)
	if syncLog.syncCh != nil {
		t.Fatal("Open log should have nil syncCh")
	}
	// Enqueue on sync log falls back to Append — data is immediately
	// durable without calling Sync.
	if err := syncLog.Enqueue(PutEntry(42, 0, 0)); err != nil {
		t.Fatalf("enqueue on sync log: %v", err)
	}
	var count int
	_ = Replay(syncPath, func(_ Entry) error { count++; return nil })
	if count != 1 {
		t.Fatalf("sync log enqueue: expected 1 entry, got %d", count)
	}

	// OpenAsync (async) must have non-nil syncCh.
	asyncLog, _ := tmpAsyncWAL(t, 50*time.Millisecond)
	if asyncLog.syncCh == nil {
		t.Fatal("OpenAsync log should have non-nil syncCh")
	}
}

func TestOpenAsyncSyncIntervalNeg1Fallback(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "sync.wal")
	l, err := OpenAsync(path, -1)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	if l.syncCh != nil {
		t.Fatal("OpenAsync with syncInterval < 0 should not start sync loop")
	}

	// Enqueue falls back to Append (synchronous).
	if err := l.Enqueue(PutEntry(42, 0, 0)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Data should be immediately durable (no async delay).
	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 entry (synchronous fallback), got %d", count)
	}
}

func TestAsyncTornRecord(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "torn.wal")
	l, err := OpenAsync(path, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Enqueue one valid entry and Sync to flush it.
	if err := l.Enqueue(PutEntry(1, 0, 0)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	// Now write a partial record directly to the file (simulating a
	// crash mid-batch-write: Write succeeded but the full 25 bytes
	// weren't written).
	l.mu.Lock()
	_, _ = l.f.Write([]byte{opPut, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}) // 9 of 25 bytes
	l.mu.Unlock()

	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Replay must return the valid entry and skip the torn tail.
	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 valid record (torn tail skipped), got %d", count)
	}
}

func TestLastSyncTime(t *testing.T) {
	t.Parallel()
	l, _ := tmpAsyncWAL(t, 20*time.Millisecond)

	// Initially zero (never synced).
	if !l.LastSyncTime().IsZero() {
		t.Fatal("expected zero LastSyncTime before first sync")
	}

	if err := l.Enqueue(PutEntry(1, 0, 0)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	// After Sync, LastSyncTime should be recent.
	last := l.LastSyncTime()
	if last.IsZero() {
		t.Fatal("expected non-zero LastSyncTime after sync")
	}
	if time.Since(last) > 5*time.Second {
		t.Fatalf("LastSyncTime too old: %v", time.Since(last))
	}
}

func TestDroppedEntriesResets(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "dropreset.wal")
	l, err := OpenAsync(path, 10*time.Second)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	// Fill the channel beyond capacity to generate drops.
	for i := range syncChSize + 50 {
		_ = l.Enqueue(PutEntry(uint64(i+1), 0, 0))
	}

	first := l.DroppedEntries()
	if first == 0 {
		t.Fatalf("expected dropped entries on first read, got 0")
	}
	// Swap(0) resets the counter; second read should be 0 (no new drops
	// since the first read — the channel is already full).
	second := l.DroppedEntries()
	if second != 0 {
		t.Fatalf("expected 0 dropped after reset, got %d", second)
	}
}
