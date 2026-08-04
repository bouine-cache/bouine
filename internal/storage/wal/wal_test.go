package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func tmpWAL(t *testing.T) (*Log, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.wal")
	l, err := Open(path)
	require.NoErrorf(t, err, "open: %v", err)
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
		err := l.Append(e)
		require.NoErrorf(t, err, "append: %v", err)
	}

	var replayed []Entry
	err := Replay(path, func(e Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	require.NoErrorf(t, err, "replay: %v", err)
	require.Len(t, replayed, len(entries))
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
	require.NoError(t, err)
	_ = f.Close()

	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	require.NoErrorf(t, err, "replay empty: %v", err)
	require.Equal(t, 0, count)
}

func TestReplay_MissingFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "missing.wal")
	err := Replay(path, func(_ Entry) error { return nil })
	require.NoErrorf(t, err, "replay missing: %v", err)
}

func TestReplay_TruncatedRecord(t *testing.T) {
	t.Parallel()
	l, path := tmpWAL(t)
	err := l.Append(PutEntry(1, 0, 0))
	require.NoErrorf(t, err, "append: %v", err)
	_ = l.Close()

	// Truncate the file to simulate a partial write.
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	info, _ := f.Stat()
	_ = f.Truncate(info.Size() - 5) // chop off part of the CRC
	_ = f.Close()

	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	require.NoErrorf(t, err, "replay truncated: %v", err)
	require.Equal(t, 0, count)
}

func TestTruncate(t *testing.T) {
	t.Parallel()
	l, path := tmpWAL(t)
	for range 5 {
		err := l.Append(PutEntry(1, 0, 0))
		require.NoErrorf(t, err, "append: %v", err)
	}
	err := l.Truncate()
	require.NoErrorf(t, err, "truncate: %v", err)

	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	require.NoErrorf(t, err, "replay: %v", err)
	require.Equal(t, 0, count)
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
	require.NoErrorf(t, err, "open async: %v", err)
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
		err := l.Enqueue(e)
		require.NoErrorf(t, err, "enqueue: %v", err)
	}

	err := l.Sync()
	require.NoErrorf(t, err, "sync: %v", err)

	var replayed []Entry
	err = Replay(path, func(e Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	require.NoErrorf(t, err, "replay: %v", err)
	require.Len(t, replayed, len(entries))
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

	err := l.Sync()
	require.NoErrorf(t, err, "sync: %v", err)

	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	require.NoErrorf(t, err, "replay: %v", err)
	require.Len(t, entries, count)
}

func TestAsyncCloseFlushesPending(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "close.wal")
	l, err := OpenAsync(path, 10*time.Second) // long interval so Close must flush
	require.NoErrorf(t, err, "open: %v", err)

	for i := range 10 {
		err := l.Enqueue(PutEntry(uint64(i+1), 0, int64(i*100)))
		require.NoErrorf(t, err, "enqueue: %v", err)
	}

	err = l.Close()
	require.NoErrorf(t, err, "close: %v", err)

	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	require.NoErrorf(t, err, "replay: %v", err)
	require.Equal(t, 10, count)
}

func TestAsyncDropOnFull(t *testing.T) {
	t.Parallel()
	// Use a very long sync interval so the channel fills without
	// the loop draining. syncChSize is 4096; send more than that.
	path := filepath.Join(t.TempDir(), "drop.wal")
	l, err := OpenAsync(path, 10*time.Second)
	require.NoErrorf(t, err, "open: %v", err)
	t.Cleanup(func() { _ = l.Close() })

	sent := syncChSize + 100
	for i := range sent {
		_ = l.Enqueue(PutEntry(uint64(i+1), 0, 0))
	}

	dropped := l.DroppedEntries()
	require.NotEqual(t, 0, dropped)
}

func TestOpenSyncVsAsync(t *testing.T) {
	t.Parallel()
	// Open (sync) must have nil syncCh.
	syncLog, syncPath := tmpWAL(t)
	require.Nil(t, syncLog.syncCh)
	// Enqueue on sync log falls back to Append — data is immediately
	// durable without calling Sync.
	err := syncLog.Enqueue(PutEntry(42, 0, 0))
	require.NoErrorf(t, err, "enqueue on sync log: %v", err)
	var count int
	_ = Replay(syncPath, func(_ Entry) error { count++; return nil })
	require.Equal(t, 1, count)

	// OpenAsync (async) must have non-nil syncCh.
	asyncLog, _ := tmpAsyncWAL(t, 50*time.Millisecond)
	require.NotNil(t, asyncLog.syncCh)
}

func TestOpenAsyncSyncIntervalNeg1Fallback(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "sync.wal")
	l, err := OpenAsync(path, -1)
	require.NoErrorf(t, err, "open: %v", err)
	t.Cleanup(func() { _ = l.Close() })

	require.Nil(t, l.syncCh)

	// Enqueue falls back to Append (synchronous).
	err = l.Enqueue(PutEntry(42, 0, 0))
	require.NoErrorf(t, err, "enqueue: %v", err)

	// Data should be immediately durable (no async delay).
	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	require.NoErrorf(t, err, "replay: %v", err)
	require.Equal(t, 1, count)
}

func TestAsyncTornRecord(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "torn.wal")
	l, err := OpenAsync(path, 50*time.Millisecond)
	require.NoErrorf(t, err, "open: %v", err)

	// Enqueue one valid entry and Sync to flush it.
	err = l.Enqueue(PutEntry(1, 0, 0))
	require.NoErrorf(t, err, "enqueue: %v", err)
	err = l.Sync()
	require.NoErrorf(t, err, "sync: %v", err)

	// Now write a partial record directly to the file (simulating a
	// crash mid-batch-write: Write succeeded but the full 25 bytes
	// weren't written).
	l.mu.Lock()
	_, _ = l.f.Write([]byte{opPut, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}) // 9 of 25 bytes
	l.mu.Unlock()

	err = l.Close()
	require.NoErrorf(t, err, "close: %v", err)

	// Replay must return the valid entry and skip the torn tail.
	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	require.NoErrorf(t, err, "replay: %v", err)
	require.Equal(t, 1, count)
}

func TestLastSyncTime(t *testing.T) {
	t.Parallel()
	l, _ := tmpAsyncWAL(t, 20*time.Millisecond)

	// Initially zero (never synced).
	require.True(t, l.LastSyncTime().IsZero())

	err := l.Enqueue(PutEntry(1, 0, 0))
	require.NoErrorf(t, err, "enqueue: %v", err)
	err = l.Sync()
	require.NoErrorf(t, err, "sync: %v", err)

	// After Sync, LastSyncTime should be recent.
	last := l.LastSyncTime()
	require.False(t, last.IsZero())
	if time.Since(last) > 5*time.Second {
		t.Fatalf("LastSyncTime too old: %v", time.Since(last))
	}
}

func TestDroppedEntriesResets(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "dropreset.wal")
	l, err := OpenAsync(path, 10*time.Second)
	require.NoErrorf(t, err, "open: %v", err)
	t.Cleanup(func() { _ = l.Close() })

	// Fill the channel beyond capacity to generate drops.
	for i := range syncChSize + 50 {
		_ = l.Enqueue(PutEntry(uint64(i+1), 0, 0))
	}

	first := l.DroppedEntries()
	require.NotEqual(t, 0, first)
	// Swap(0) resets the counter; second read should be 0 (no new drops
	// since the first read — the channel is already full).
	second := l.DroppedEntries()
	require.Equal(t, int64(0), second)
}
