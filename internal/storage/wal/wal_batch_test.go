package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
)

func TestAppendBatch(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "batch.wal")
	l, err := Open(path)
	require.NoError(t, err, "open")
	t.Cleanup(func() { _ = l.Close() })

	entries := []Entry{
		PutEntry(testkey.Key(10), 0, 0),
		PutEntry(testkey.Key(20), 1, 512),
		DeleteEntry(testkey.Key(10)),
		PutEntry(testkey.Key(30), 2, 1024),
	}
	err = l.AppendBatch(entries)
	require.NoError(t, err, "AppendBatch")

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

func TestAppendBatch_Empty(t *testing.T) {
	t.Parallel()
	l, path := tmpWAL(t)
	err := l.AppendBatch(nil)
	require.NoError(t, err, "AppendBatch(nil)")

	var count int
	err = Replay(path, func(_ Entry) error { count++; return nil })
	require.NoError(t, err, "replay")
	require.Equal(t, 0, count)
}

func TestAppendBatch_Atomicity(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "atomic.wal")
	l, err := Open(path)
	require.NoError(t, err, "open")

	entries := []Entry{
		PutEntry(testkey.Key(100), 0, 0),
		PutEntry(testkey.Key(200), 0, 256),
	}
	err = l.AppendBatch(entries)
	require.NoError(t, err, "AppendBatch")

	// Verify file size matches 2 records.
	info, err := os.Stat(path)
	require.NoError(t, err, "stat")
	expectedSize := int64(2 * recLen)
	require.Equal(t, expectedSize, info.Size())
	_ = l.Close()

	// Verify all entries replay correctly.
	var replayed []Entry
	err = Replay(path, func(e Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	require.NoError(t, err, "replay")
	require.Len(t, replayed, 2)
}
