package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendBatch(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "batch.wal")
	l, err := Open(path)
	require.NoErrorf(t, err, "open: %v", err)
	t.Cleanup(func() { _ = l.Close() })

	entries := []Entry{
		PutEntry(10, 0, 0),
		PutEntry(20, 1, 512),
		DeleteEntry(10),
		PutEntry(30, 2, 1024),
	}
	err = l.AppendBatch(entries)
	require.NoErrorf(t, err, "AppendBatch: %v", err)

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

func TestAppendBatch_Empty(t *testing.T) {
	t.Parallel()
	l, path := tmpWAL(t)
	err := l.AppendBatch(nil)
	require.NoErrorf(t, err, "AppendBatch(nil): %v", err)

	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	require.NoErrorf(t, err, "replay: %v", err)
	require.Equal(t, 0, count)
}

func TestAppendBatch_Atomicity(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "atomic.wal")
	l, err := Open(path)
	require.NoErrorf(t, err, "open: %v", err)

	entries := []Entry{
		PutEntry(100, 0, 0),
		PutEntry(200, 0, 256),
	}
	err = l.AppendBatch(entries)
	require.NoErrorf(t, err, "AppendBatch: %v", err)

	// Verify file size matches 2 records.
	info, err := os.Stat(path)
	require.NoErrorf(t, err, "stat: %v", err)
	expectedSize := int64(2 * recLen)
	require.Equal(t, expectedSize, info.Size())
	_ = l.Close()

	// Verify all entries replay correctly.
	var replayed []Entry
	err = Replay(path, func(e Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	require.NoErrorf(t, err, "replay: %v", err)
	require.Len(t, replayed, 2)
}
