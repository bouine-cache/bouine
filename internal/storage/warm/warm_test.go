package warm

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
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
	t.Parallel()
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
	t.Parallel()
	s := tmpStore(t)

	_, _, err := s.Put(99, []byte("data"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := s.Delete(99); err != nil {
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

func TestRecomputeStats_ScanError(t *testing.T) {
	t.Parallel()
	s := tmpStore(t)

	segID, off, err := s.Put(42, []byte("live record"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}

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
	if _, err := seg.f.WriteAt([]byte{0xFF}, off+headerLen+2); err != nil {
		seg.mu.Unlock()
		t.Fatalf("corrupt: %v", err)
	}
	seg.mu.Unlock()

	wantEntries, wantBytes := s.Stats()
	if wantEntries != 1 {
		t.Fatalf("precondition: entries = %d, want 1", wantEntries)
	}

	if err := s.RecomputeStats(); err == nil {
		t.Fatal("expected error from RecomputeStats on corrupt segment, got nil")
	}

	ent, byt := s.Stats()
	if ent != wantEntries || byt != wantBytes {
		t.Fatalf("stats should be unchanged on error, got entries=%d bytes=%d, want %d/%d",
			ent, byt, wantEntries, wantBytes)
	}
}

func TestNonexistentSegment(t *testing.T) {
	t.Parallel()
	s := tmpStore(t)
	_, err := s.ReadRecord(9999, 0)
	if err == nil {
		t.Fatal("expected error for missing segment")
	}
}

func TestEmptyDir(t *testing.T) {
	t.Parallel()
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

func TestGet_IndexMaintained(t *testing.T) {
	t.Parallel()
	s := tmpStore(t)

	body := []byte("warm get test")
	if _, _, err := s.Put(77, body); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, err := s.Get(77)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(body) {
		t.Fatalf("Get = %q, want %q", got, body)
	}
}

func TestGet_MissingKey(t *testing.T) {
	t.Parallel()
	s := tmpStore(t)
	got, err := s.Get(9999)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for missing key, got %q", got)
	}
}

func TestGet_AfterDelete(t *testing.T) {
	t.Parallel()
	s := tmpStore(t)

	if _, _, err := s.Put(55, []byte("to be deleted")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := s.Delete(55); err != nil {
		t.Fatalf("delete: %v", err)
	}

	got, err := s.Get(55)
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil after delete, got %q", got)
	}
}

func TestSetIndex_DelIndex(t *testing.T) {
	t.Parallel()
	s := tmpStore(t)

	body := []byte("index rebuild")
	segID, offset, err := s.Put(42, body)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	// Simulate WAL replay: build a fresh store with no index and replay.
	s2 := tmpStore(t)
	// s2.index is empty — SetIndex injects the entry.
	s2.SetIndex(42, segID, offset)

	// ReadRecord works via s, but for the index test use s directly.
	got, err := s.Get(42)
	if err != nil || string(got) != string(body) {
		t.Fatalf("Get after SetIndex: err=%v body=%q", err, got)
	}

	s.DelIndex(42)
	got, err = s.Get(42)
	if err != nil || got != nil {
		t.Fatalf("expected nil after DelIndex: err=%v body=%q", err, got)
	}
	_ = s2
}

func TestStore_ConcurrentGetCompact(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 256 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	// Populate with enough records to make compaction meaningful.
	for i := uint64(0); i < 500; i++ {
		body := []byte(fmt.Sprintf("body-%d", i))
		segID, offset, err := s.Put(i, body)
		if err != nil {
			t.Fatal(err)
		}
		s.SetIndex(i, segID, offset)
	}
	// Delete half to create tombstones.
	for i := uint64(0); i < 250; i++ {
		if _, err := s.Delete(i); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	// Concurrent readers.
	for g := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 200 {
				key := uint64(250 + (j % 250)) // read live keys
				body, err := s.Get(key)
				if err != nil {
					t.Errorf("goroutine %d: Get(%d): %v", g, key, err)
					return
				}
				if body == nil {
					t.Errorf("goroutine %d: Get(%d): unexpected miss", g, key)
					return
				}
				if want := fmt.Sprintf("body-%d", key); string(body) != want {
					t.Errorf("goroutine %d: Get(%d) = %q, want %q", g, key, body, want)
					return
				}
			}
		}()
	}
	// Concurrent compaction.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.Compact(); err != nil {
			t.Errorf("Compact: %v", err)
		}
	}()

	wg.Wait()
}

func TestStore_ConcurrentGetPut(t *testing.T) {
	t.Parallel()
	s := tmpStore(t)

	// Pre-populate keys so readers have something to find.
	for i := uint64(0); i < 100; i++ {
		body := []byte(fmt.Sprintf("initial-%d", i))
		if _, _, err := s.Put(i, body); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	// Concurrent readers.
	for g := range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 500 {
				key := uint64(j % 100)
				body, err := s.Get(key)
				if err != nil {
					t.Errorf("reader %d: Get(%d): %v", g, key, err)
					return
				}
				if body == nil {
					t.Errorf("reader %d: Get(%d): unexpected miss", g, key)
					return
				}
			}
		}()
	}
	// Concurrent writers.
	for g := range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 100 {
				key := uint64(j % 100)
				body := []byte(fmt.Sprintf("writer-%d-%d", g, j))
				if _, _, err := s.Put(key, body); err != nil {
					t.Errorf("writer %d: Put(%d): %v", g, key, err)
					return
				}
			}
		}()
	}

	wg.Wait()
}

func BenchmarkGet(b *testing.B) {
	dir := b.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 256 << 20, SegMax: 4 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	for i := uint64(0); i < 1000; i++ {
		body := []byte(fmt.Sprintf("bench-body-%04d-padding-to-256-bytes"+
			"------------------------------------------------------------"+
			"------------------------------------------------------------"+
			"------------------------------------------------------------"+
			"----------------------------------------------", i))
		if _, _, err := s.Put(i, body); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			key := i % 1000
			_, _ = s.Get(key)
			i++
		}
	})
}

func TestStore_CompactStreamsLiveRecords(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 256 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	const liveKeys = 300
	for i := range liveKeys {
		body := []byte(fmt.Sprintf("v1-%d", i))
		segID, off, err := s.Put(uint64(i), body)
		if err != nil {
			t.Fatalf("Put v1 %d: %v", i, err)
		}
		s.SetIndex(uint64(i), segID, off)
	}
	for i := range 100 {
		if _, err := s.Delete(uint64(i)); err != nil {
			t.Fatalf("Delete %d: %v", i, err)
		}
	}
	for i := 100; i < liveKeys; i++ {
		body := []byte(fmt.Sprintf("v2-%d", i))
		segID, off, err := s.Put(uint64(i), body)
		if err != nil {
			t.Fatalf("Put v2 %d: %v", i, err)
		}
		s.SetIndex(uint64(i), segID, off)
	}

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	for i := range liveKeys {
		got, err := s.Get(uint64(i))
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		if i < 100 {
			if got != nil {
				t.Fatalf("key %d: expected nil after delete+compact, got %q", i, got)
			}
			continue
		}
		want := fmt.Sprintf("v2-%d", i)
		if string(got) != want {
			t.Fatalf("key %d: got %q, want %q", i, got, want)
		}
	}

	entries, _ := s.Stats()
	if want := int64(liveKeys - 100); entries != want {
		t.Fatalf("entries after compact: got %d, want %d", entries, want)
	}
}

// TestStore_CompactReadOnlyParent verifies that compaction works when the
// parent of the warm dir is read-only. This reproduces the Kubernetes
// readOnlyRootFilesystem deployment where only the PVC mount (warm_dir) is
// writable and the old sibling compactDir (dir+".compact") landed on the
// read-only root filesystem and failed with EROFS.
func TestStore_CompactReadOnlyParent(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()

	// Create the warm dir and make the parent read-only so that creating
	// a sibling directory (dir+".compact") would fail with EROFS.
	warmDir := filepath.Join(parent, "warm")
	if err := os.MkdirAll(warmDir, 0o750); err != nil {
		t.Fatal(err)
	}
	// Store must be created before the parent goes read-only.
	s, err := NewStore(Config{Dir: warmDir, MaxBytes: 256 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = s.Close()
		_ = os.Chmod(parent, 0o755) // restore for cleanup
	})

	// Populate and create enough dead bytes for compaction.
	for i := range 200 {
		body := []byte(fmt.Sprintf("body-%d-padding-------------------------------------", i))
		segID, off, err := s.Put(uint64(i), body)
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		s.SetIndex(uint64(i), segID, off)
	}
	for i := range 100 {
		if _, err := s.Delete(uint64(i)); err != nil {
			t.Fatalf("Delete %d: %v", i, err)
		}
	}

	// Make the parent read-only: a sibling compactDir would fail.
	if err := os.Chmod(parent, 0o500); err != nil {
		t.Fatal(err)
	}

	// Compact must succeed — the compact dir is a subdirectory of warmDir
	// which lives on the writable PVC, not a sibling on the read-only parent.
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact with read-only parent: %v", err)
	}

	// Verify live keys survived.
	for i := 100; i < 200; i++ {
		got, err := s.Get(uint64(i))
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		if got == nil {
			t.Fatalf("key %d: expected live after compact, got nil", i)
		}
	}
}

// truncateLastRecord truncates the given segment file so the last record
// written at lastOff is torn: the header is present but the body is cut
// short. This simulates a crash where the WAL entry was fsynced but the
// segment data was not fully flushed.
func truncateLastRecord(t *testing.T, s *Store, segID int, lastOff int64) {
	t.Helper()
	s.mu.RLock()
	var seg *Segment
	for _, ss := range s.segs {
		if ss.ID == segID {
			seg = ss
			break
		}
	}
	s.mu.RUnlock()
	if seg == nil {
		t.Fatalf("segment %d not found", segID)
	}
	seg.mu.Lock()
	defer seg.mu.Unlock()
	cutAt := lastOff + headerLen + 4 // header + partial body
	if err := seg.f.Truncate(cutAt); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func TestTornRecord_GetReturnsMiss(t *testing.T) {
	t.Parallel()
	s := tmpStore(t)

	body := make([]byte, 256)
	segID, off, err := s.Put(42, body)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	truncateLastRecord(t, s, segID, off)

	got, err := s.Get(42)
	if err != nil {
		t.Fatalf("Get after torn write: expected nil error, got %v", err)
	}
	if got != nil {
		t.Fatalf("Get after torn write: expected nil (miss), got %q", got)
	}

	s.idxMu.RLock()
	_, ok := s.index[42]
	s.idxMu.RUnlock()
	if ok {
		t.Fatal("stale index entry should be dropped after torn read")
	}
}

func TestTornRecord_ReadRecordReturnsNil(t *testing.T) {
	t.Parallel()
	s := tmpStore(t)

	body := make([]byte, 256)
	segID, off, err := s.Put(42, body)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	truncateLastRecord(t, s, segID, off)

	rec, err := s.ReadRecord(segID, off)
	if err != nil {
		t.Fatalf("ReadRecord after torn write: expected nil error, got %v", err)
	}
	if rec != nil {
		t.Fatalf("ReadRecord after torn write: expected nil, got %+v", rec)
	}
}

func TestTornRecord_ScanSkipsTrailing(t *testing.T) {
	t.Parallel()
	s := tmpStore(t)

	var lastSegID int
	var lastOff int64
	for i := range 5 {
		segID, off, err := s.Put(uint64(i), []byte("good"))
		if err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		lastSegID, lastOff = segID, off
	}

	truncateLastRecord(t, s, lastSegID, lastOff)

	var count int
	if err := s.Scan(func(_ Record) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("Scan with torn trailing record: expected nil error, got %v", err)
	}
	if count != 4 {
		t.Fatalf("Scan count = %d, want 4 (torn 5th record skipped)", count)
	}
}

func TestTornRecord_RecomputeStatsSucceeds(t *testing.T) {
	t.Parallel()
	s := tmpStore(t)

	var lastSegID int
	var lastOff int64
	for i := range 5 {
		segID, off, err := s.Put(uint64(i), []byte("good"))
		if err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		lastSegID, lastOff = segID, off
	}

	truncateLastRecord(t, s, lastSegID, lastOff)

	if err := s.RecomputeStats(); err != nil {
		t.Fatalf("RecomputeStats with torn trailing record: %v", err)
	}
}

func TestTornRecord_CompactSucceeds(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 256 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	for i := range 10 {
		if _, _, err := s.Put(uint64(i), []byte("compact-me")); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	for i := range 5 {
		if _, err := s.Delete(uint64(i)); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}

	var lastSegID int
	var lastOff int64
	lastSegID, lastOff, err = s.Put(99, make([]byte, 128))
	if err != nil {
		t.Fatalf("put last: %v", err)
	}

	truncateLastRecord(t, s, lastSegID, lastOff)

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact with torn trailing record: %v", err)
	}

	for i := 5; i < 10; i++ {
		got, err := s.Get(uint64(i))
		if err != nil {
			t.Fatalf("Get %d after compact: %v", i, err)
		}
		if string(got) != "compact-me" {
			t.Fatalf("Get %d = %q, want %q", i, got, "compact-me")
		}
	}
}
