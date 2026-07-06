package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppendBatch(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "batch.wal")
	l, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	entries := []Entry{
		PutEntry(10, 0, 0),
		PutEntry(20, 1, 512),
		DeleteEntry(10),
		PutEntry(30, 2, 1024),
	}
	if err := l.AppendBatch(entries); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	var replayed []Entry
	err = Replay(path, func(e Entry) error {
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

func TestAppendBatch_Empty(t *testing.T) {
	t.Parallel()
	l, path := tmpWAL(t)
	if err := l.AppendBatch(nil); err != nil {
		t.Fatalf("AppendBatch(nil): %v", err)
	}

	var count int
	err := Replay(path, func(_ Entry) error { count++; return nil })
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 entries, got %d", count)
	}
}

func TestAppendBatch_Atomicity(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "atomic.wal")
	l, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	entries := []Entry{
		PutEntry(100, 0, 0),
		PutEntry(200, 0, 256),
	}
	if err := l.AppendBatch(entries); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	// Verify file size matches 2 records.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	expectedSize := int64(2 * recLen)
	if info.Size() != expectedSize {
		t.Fatalf("file size = %d, want %d", info.Size(), expectedSize)
	}
	_ = l.Close()

	// Verify all entries replay correctly.
	var replayed []Entry
	err = Replay(path, func(e Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != 2 {
		t.Fatalf("replayed %d entries, want 2", len(replayed))
	}
}
