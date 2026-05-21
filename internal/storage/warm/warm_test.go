package warm

import (
	"os"
	"path/filepath"
	"testing"
)

func tmpStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := NewStore(Config{
		Dir:      dir,
		MaxBytes: 100 << 20,
		SegMax:   1 << 20, // 1 MiB segments for fast rollover
	})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestPutAndRead(t *testing.T) {
	s := tmpStore(t)

	body := []byte("hello warm tier")
	segID, off, err := s.Put(42, body)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	rec, err := s.ReadRecord(segID, off)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if rec.Key != 42 {
		t.Fatalf("key = %d", rec.Key)
	}
	if string(rec.Body) != "hello warm tier" {
		t.Fatalf("body = %q", rec.Body)
	}
	if rec.IsTomb {
		t.Fatal("should not be tombstone")
	}
}

func TestTombstone(t *testing.T) {
	s := tmpStore(t)

	_, _, err := s.Put(99, []byte("data"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := s.Delete(99); err != nil {
		t.Fatalf("delete: %v", err)
	}

	var found []Record
	err = s.Scan(func(r Record) error {
		found = append(found, r)
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(found) != 2 {
		t.Fatalf("expected 2 records, got %d", len(found))
	}
	if found[0].IsTomb {
		t.Fatal("first record should be live")
	}
	if !found[1].IsTomb {
		t.Fatal("second record should be tombstone")
	}
}

func TestSegmentRollover(t *testing.T) {
	s := tmpStore(t)
	bigBody := make([]byte, 512*1024) // 512 KiB

	// Write enough to trigger segment rollover (1 MiB segments).
	for range 4 {
		if _, _, err := s.Put(1, bigBody); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	s.mu.RLock()
	n := len(s.segs)
	s.mu.RUnlock()

	if n < 2 {
		t.Fatalf("expected >= 2 segments, got %d", n)
	}
}

func TestCRCCorruption(t *testing.T) {
	s := tmpStore(t)

	body := []byte("integrity check")
	segID, off, err := s.Put(77, body)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	// Corrupt one byte of the body on disk.
	s.mu.RLock()
	var seg *Segment
	for _, ss := range s.segs {
		if ss.ID == segID {
			seg = ss
			break
		}
	}
	s.mu.RUnlock()

	seg.mu.Lock()
	corrupt := []byte{0xFF}
	if _, err := seg.f.WriteAt(corrupt, off+headerLen+2); err != nil {
		seg.mu.Unlock()
		t.Fatalf("corrupt: %v", err)
	}
	seg.mu.Unlock()

	_, err = s.ReadRecord(segID, off)
	if err == nil {
		t.Fatal("expected CRC error on corrupted record")
	}
}

func TestScan_MultipleRecords(t *testing.T) {
	s := tmpStore(t)

	for i := range 10 {
		body := []byte("record-" + string(rune('A'+i)))
		if _, _, err := s.Put(uint64(i), body); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	var count int
	err := s.Scan(func(_ Record) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if count != 10 {
		t.Fatalf("scanned %d records, want 10", count)
	}
}

func TestOpenExisting(t *testing.T) {
	dir := t.TempDir()
	s1, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	body := []byte("persist me")
	segID, off, err := s1.Put(123, body)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	_ = s1.Close()

	// Reopen.
	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()

	rec, err := s2.ReadRecord(segID, off)
	if err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	if rec.Key != 123 || string(rec.Body) != "persist me" {
		t.Fatalf("bad record: %+v", rec)
	}
}

func TestStats(t *testing.T) {
	s := tmpStore(t)
	for i := range 5 {
		if _, _, err := s.Put(uint64(i), make([]byte, 100)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	ent, byt := s.Stats()
	if ent != 5 {
		t.Fatalf("entries = %d", ent)
	}
	if byt <= 0 {
		t.Fatalf("bytes = %d", byt)
	}
}

func TestNonexistentSegment(t *testing.T) {
	s := tmpStore(t)
	_, err := s.ReadRecord(9999, 0)
	if err == nil {
		t.Fatal("expected error for missing segment")
	}
}

func TestEmptyDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sub")
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer func() { _ = s.Close() }()

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("expected dir")
	}
}
