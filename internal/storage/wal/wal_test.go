package wal

import (
	"os"
	"path/filepath"
	"testing"
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
