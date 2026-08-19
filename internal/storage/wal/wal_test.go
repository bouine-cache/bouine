package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

func tmpWAL(t *testing.T) (*Log, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.wal")
	l, err := Open(path)
	require.NoError(t, err, "open")
	t.Cleanup(func() { _ = l.Close() })
	return l, path
}

func TestAppendAndReplay(t *testing.T) {
	t.Parallel()
	l, path := tmpWAL(t)

	entries := []Entry{
		PutEntry(testkey.Key(100), 0, 0),
		PutEntry(testkey.Key(200), 0, 1024),
		DeleteEntry(testkey.Key(100)),
		PutEntry(testkey.Key(300), 1, 0),
	}
	for _, e := range entries {
		err := l.Append(e)
		require.NoError(t, err, "append")
	}

	var replayed []Entry
	err := Replay(path, func(e Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	require.NoError(t, err, "replay")
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
	require.NoError(t, err, "replay empty")
	require.Equal(t, 0, count)
}

func TestReplay_MissingFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "missing.wal")
	err := Replay(path, func(_ Entry) error { return nil })
	require.NoError(t, err, "replay missing")
}

func TestReplay_TruncatedRecord(t *testing.T) {
	t.Parallel()
	l, path := tmpWAL(t)
	err := l.Append(PutEntry(testkey.Key(1), 0, 0))
	require.NoError(t, err, "append")
	_ = l.Close()

	// Truncate the file to simulate a partial write.
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	info, _ := f.Stat()
	_ = f.Truncate(info.Size() - 5) // chop off part of the CRC
	_ = f.Close()

	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	require.NoError(t, err, "replay truncated")
	require.Equal(t, 0, count)
}

func TestTruncate(t *testing.T) {
	t.Parallel()
	l, path := tmpWAL(t)
	for range 5 {
		err := l.Append(PutEntry(testkey.Key(1), 0, 0))
		require.NoError(t, err, "append")
	}
	err := l.Truncate()
	require.NoError(t, err, "truncate")

	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	require.NoError(t, err, "replay")
	require.Equal(t, 0, count)
}

func TestEntryHelpers(t *testing.T) {
	t.Parallel()
	p := PutEntry(testkey.Key(42), 3, 1024)
	if !p.IsPut() || p.IsDelete() {
		t.Fatal("PutEntry flags wrong")
	}
	d := DeleteEntry(testkey.Key(42))
	if !d.IsDelete() || d.IsPut() {
		t.Fatal("DeleteEntry flags wrong")
	}
}

func tmpAsyncWAL(t *testing.T, syncInterval time.Duration) (*Log, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "async.wal")
	l, err := OpenAsync(path, syncInterval)
	require.NoError(t, err, "open async")
	t.Cleanup(func() { _ = l.Close() })
	return l, path
}

func TestAsyncEnqueueSyncReplay(t *testing.T) {
	t.Parallel()
	l, path := tmpAsyncWAL(t, 50*time.Millisecond)

	entries := []Entry{
		PutEntry(testkey.Key(100), 0, 0),
		PutEntry(testkey.Key(200), 0, 1024),
		DeleteEntry(testkey.Key(100)),
		PutEntry(testkey.Key(300), 1, 0),
	}
	for _, e := range entries {
		err := l.Enqueue(e)
		require.NoError(t, err, "enqueue")
	}

	err := l.Sync()
	require.NoError(t, err, "sync")

	var replayed []Entry
	err = Replay(path, func(e Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	require.NoError(t, err, "replay")
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
		PutEntry(testkey.Key(1), 0, 0),
		PutEntry(testkey.Key(2), 0, 100),
		PutEntry(testkey.Key(3), 1, 200),
		DeleteEntry(testkey.Key(2)),
	}
	l.EnqueueBatch(entries)

	err := l.Sync()
	require.NoError(t, err, "sync")

	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	require.NoError(t, err, "replay")
	require.Len(t, entries, count)
}

func TestAsyncCloseFlushesPending(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "close.wal")
	l, err := OpenAsync(path, 10*time.Second) // long interval so Close must flush
	require.NoError(t, err, "open")

	for i := range 10 {
		err := l.Enqueue(PutEntry(testkey.Key(uint64(i+1)), 0, int64(i*100)))
		require.NoError(t, err, "enqueue")
	}

	err = l.Close()
	require.NoError(t, err, "close")

	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	require.NoError(t, err, "replay")
	require.Equal(t, 10, count)
}

func TestAsyncDropOnFull(t *testing.T) {
	t.Parallel()
	// Use a very long sync interval so the channel fills without
	// the loop draining. syncChSize is 4096; send more than that.
	path := filepath.Join(t.TempDir(), "drop.wal")
	l, err := OpenAsync(path, 10*time.Second)
	require.NoError(t, err, "open")
	t.Cleanup(func() { _ = l.Close() })

	sent := syncChSize + 100
	for i := range sent {
		_ = l.Enqueue(PutEntry(testkey.Key(uint64(i+1)), 0, 0))
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
	err := syncLog.Enqueue(PutEntry(testkey.Key(42), 0, 0))
	require.NoError(t, err, "enqueue on sync log")
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
	require.NoError(t, err, "open")
	t.Cleanup(func() { _ = l.Close() })

	require.Nil(t, l.syncCh)

	// Enqueue falls back to Append (synchronous).
	err = l.Enqueue(PutEntry(testkey.Key(42), 0, 0))
	require.NoError(t, err, "enqueue")

	// Data should be immediately durable (no async delay).
	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	require.NoError(t, err, "replay")
	require.Equal(t, 1, count)
}

func TestAsyncTornRecord(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "torn.wal")
	l, err := OpenAsync(path, 50*time.Millisecond)
	require.NoError(t, err, "open")

	// Enqueue one valid entry and Sync to flush it.
	err = l.Enqueue(PutEntry(testkey.Key(1), 0, 0))
	require.NoError(t, err, "enqueue")
	err = l.Sync()
	require.NoError(t, err, "sync")

	// Now write a partial record directly to the file (simulating a
	// crash mid-batch-write: Write succeeded but the full 25 bytes
	// weren't written).
	l.mu.Lock()
	_, _ = l.f.Write([]byte{opPut, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}) // 9 of 25 bytes
	l.mu.Unlock()

	err = l.Close()
	require.NoError(t, err, "close")

	// Replay must return the valid entry and skip the torn tail.
	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	require.NoError(t, err, "replay")
	require.Equal(t, 1, count)
}

func TestLastSyncTime(t *testing.T) {
	t.Parallel()
	l, _ := tmpAsyncWAL(t, 20*time.Millisecond)

	// Initially zero (never synced).
	require.True(t, l.LastSyncTime().IsZero())

	err := l.Enqueue(PutEntry(testkey.Key(1), 0, 0))
	require.NoError(t, err, "enqueue")
	err = l.Sync()
	require.NoError(t, err, "sync")

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
	require.NoError(t, err, "open")
	t.Cleanup(func() { _ = l.Close() })

	// Fill the channel beyond capacity to generate drops.
	for i := range syncChSize + 50 {
		_ = l.Enqueue(PutEntry(testkey.Key(uint64(i+1)), 0, 0))
	}

	first := l.DroppedEntries()
	require.NotEqual(t, 0, first)
	// Swap(0) resets the counter; second read should be 0 (no new drops
	// since the first read — the channel is already full).
	second := l.DroppedEntries()
	require.Equal(t, int64(0), second)
}
func TestEntry_HasSize(t *testing.T) {
	t.Parallel()
	key := testkey.Key(1)
	assert.False(t, PutEntry(key, 1, 0).HasSize())
	assert.True(t, PutEntryWithSize(key, 1, 0, 100).HasSize())
	assert.False(t, DeleteEntry(key).HasSize())
}

func TestEntry_IsPut(t *testing.T) {
	t.Parallel()
	key := testkey.Key(1)
	assert.True(t, PutEntry(key, 1, 0).IsPut())
	assert.True(t, PutEntryWithSize(key, 1, 0, 100).IsPut())
	assert.False(t, DeleteEntry(key).IsPut())
}

func TestEntry_IsDelete(t *testing.T) {
	t.Parallel()
	key := testkey.Key(1)
	assert.False(t, PutEntry(key, 1, 0).IsDelete())
	assert.False(t, PutEntryWithSize(key, 1, 0, 100).IsDelete())
	assert.True(t, DeleteEntry(key).IsDelete())
}

func TestPutEntryWithSize_Fields(t *testing.T) {
	t.Parallel()
	key := testkey.Key(42)
	e := PutEntryWithSize(key, 5, 1000, 512)
	assert.Equal(t, key, e.Key)
	assert.Equal(t, int32(5), e.SegID)
	assert.Equal(t, int64(1000), e.Offset)
	assert.Equal(t, int64(512), e.Size)
	assert.True(t, e.HasSize())
}

func TestPutEntry_Fields(t *testing.T) {
	t.Parallel()
	key := testkey.Key(7)
	e := PutEntry(key, 3, 200)
	assert.Equal(t, key, e.Key)
	assert.Equal(t, int32(3), e.SegID)
	assert.Equal(t, int64(200), e.Offset)
	assert.Equal(t, int64(0), e.Size)
	assert.False(t, e.HasSize())
}

func TestDeleteEntry_Fields(t *testing.T) {
	t.Parallel()
	key := testkey.Key(99)
	e := DeleteEntry(key)
	assert.Equal(t, key, e.Key)
	assert.True(t, e.IsDelete())
	assert.False(t, e.IsPut())
}

func TestEntry_OpTypes(t *testing.T) {
	t.Parallel()
	key := api.Key{}
	assert.NotEqual(t, PutEntry(key, 0, 0).Op, DeleteEntry(key).Op)
	assert.NotEqual(t, PutEntry(key, 0, 0).Op, PutEntryWithSize(key, 0, 0, 0).Op)
}

func TestOpen_Error_Path(t *testing.T) {
	t.Parallel()
	// Opening a WAL in a non-existent directory must fail.
	_, err := Open(filepath.Join(t.TempDir(), "no", "such", "dir", "test.wal"))
	require.Error(t, err)
}

func TestOpenAsync_Error_Path(t *testing.T) {
	t.Parallel()
	_, err := OpenAsync(filepath.Join(t.TempDir(), "no", "dir", "test.wal"), 50*time.Millisecond)
	require.Error(t, err)
}

func TestReplay_V2Entries(t *testing.T) {
	t.Parallel()
	l, path := tmpWAL(t)
	entries := []Entry{
		PutEntryWithSize(testkey.Key(10), 0, 0, 100),
		PutEntryWithSize(testkey.Key(20), 1, 512, 2048),
		DeleteEntry(testkey.Key(10)),
	}
	for _, e := range entries {
		require.NoError(t, l.Append(e))
	}

	var replayed []Entry
	err := Replay(path, func(e Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, replayed, len(entries))
	// v2 entries should carry size.
	assert.Equal(t, int64(100), replayed[0].Size)
	assert.Equal(t, int64(2048), replayed[1].Size)
	assert.True(t, replayed[0].HasSize())
}

func TestReplay_CallbackError(t *testing.T) {
	t.Parallel()
	l, path := tmpWAL(t)
	require.NoError(t, l.Append(PutEntry(testkey.Key(1), 0, 0)))
	require.NoError(t, l.Append(PutEntry(testkey.Key(2), 0, 0)))
	_ = l.Close()

	stopErr := assert.AnError
	err := Replay(path, func(e Entry) error {
		return stopErr
	})
	require.ErrorIs(t, err, stopErr)
}

func TestReplay_ReadError(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "notawal.txt")
	require.NoError(t, os.WriteFile(path, []byte("garbage"), 0o600))

	// Not a valid WAL — short read returns nil (treated as truncated).
	err := Replay(path, func(_ Entry) error { return nil })
	require.NoError(t, err)
}

func TestAppendBatch_EmptySlice(t *testing.T) {
	t.Parallel()
	l, _ := tmpWAL(t)
	require.NoError(t, l.AppendBatch(nil))
	require.NoError(t, l.AppendBatch([]Entry{}))
}

func TestAppendBatch_MixedEntries(t *testing.T) {
	t.Parallel()
	l, path := tmpWAL(t)
	entries := []Entry{
		PutEntry(testkey.Key(1), 0, 0),
		PutEntryWithSize(testkey.Key(2), 0, 100, 200),
		DeleteEntry(testkey.Key(1)),
	}
	require.NoError(t, l.AppendBatch(entries))

	var count int
	require.NoError(t, Replay(path, func(_ Entry) error { count++; return nil }))
	assert.Equal(t, 3, count)
}

func TestEnqueueBatch_SyncOnlyLog(t *testing.T) {
	t.Parallel()
	l, path := tmpWAL(t)
	entries := []Entry{
		PutEntry(testkey.Key(1), 0, 0),
		PutEntry(testkey.Key(2), 0, 100),
	}
	l.EnqueueBatch(entries)

	var count int
	require.NoError(t, Replay(path, func(_ Entry) error { count++; return nil }))
	assert.Equal(t, 2, count)
}

func TestSync_SyncOnlyLog_NoOp(t *testing.T) {
	t.Parallel()
	l, _ := tmpWAL(t)
	// Sync on a sync-only log (syncCh == nil) is a no-op.
	require.NoError(t, l.Sync())
}

func TestTruncate_AfterClose_Error(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "trunc.wal")
	l, err := Open(path)
	require.NoError(t, err)
	require.NoError(t, l.Close())

	// Truncate on a closed file should error.
	err = l.Truncate()
	require.Error(t, err)
}

func TestAppend_AfterClose_Error(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "appendclosed.wal")
	l, err := Open(path)
	require.NoError(t, err)
	require.NoError(t, l.Close())

	err = l.Append(PutEntry(testkey.Key(1), 0, 0))
	require.Error(t, err)
}

func TestReplay_V2TornRecord(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "v2torn.wal")
	l, err := Open(path)
	require.NoError(t, err)

	require.NoError(t, l.Append(PutEntryWithSize(testkey.Key(1), 0, 0, 100)))
	_ = l.Close()

	// Append a partial v2 record (only the base 33 bytes, missing the 8 size bytes).
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o600)
	require.NoError(t, err)
	buf := make([]byte, recLen)
	buf[0] = opPutV2
	key2 := testkey.Key(2)
	copy(buf[1:17], key2[:])
	// Write only the base 33 bytes — the v2 extra 8 bytes are missing.
	_, err = f.Write(buf)
	require.NoError(t, err)
	_ = f.Close()

	// Replay should return the first valid entry and skip the torn v2 tail.
	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}
