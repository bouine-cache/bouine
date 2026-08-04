package warm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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
	if _, err := seg.f.WriteAt(corrupt, off+HeaderLen+2); err != nil {
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
	if _, err := seg.f.WriteAt([]byte{0xFF}, off+HeaderLen+2); err != nil {
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

// BenchmarkGet measures warm-tier Get latency. Since the SIEVE eviction
// PR, every Get now does a second idxMu.RLock + map lookup + identity
// re-check + atomic visited-bit store. This benchmark covers that added
// cost — warm Get is off the hot path (L0 serves hits), but the overhead
// is reported here so regressions are caught.
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
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			key := i % 1000
			_, _ = s.Get(key)
			i++
		}
	})
}

// BenchmarkReadRecordAt isolates the readRecordAt allocation path
// (issue #187, fix #4). The header and footer buffers are pooled via
// sync.Pool; the body is not (it's aliased by decodeObject and returned
// in Record.Body). This benchmark verifies the pool eliminates the
// two fixed-size allocations per read.
func BenchmarkReadRecordAt(b *testing.B) {
	dir := b.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 256 << 20, SegMax: 4 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	body := []byte(fmt.Sprintf("bench-body-padding-to-256-bytes" +
		"------------------------------------------------------------" +
		"------------------------------------------------------------" +
		"------------------------------------------------------------" +
		"----------------------------------------------"))
	segID, off, err := s.Put(1, body)
	if err != nil {
		b.Fatal(err)
	}

	s.mu.RLock()
	seg := s.segs[segID]
	s.mu.RUnlock()
	if seg == nil {
		b.Fatalf("segment %d not found", segID)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		rec, err := readRecordAt(seg, off, 0) // size=0: use legacy 3-pread path
		if err != nil {
			b.Fatal(err)
		}
		if rec.Key != 1 {
			b.Fatalf("key=%d, want 1", rec.Key)
		}
	}
}

// TestReadRecordAtSinglePread verifies the single-pread fast path (size > 0)
// returns the same data as the legacy 3-pread path (size == 0).
func TestReadRecordAtSinglePread(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 256 << 20, SegMax: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	body := []byte("test body for single pread verification")
	segID, off, err := s.Put(1, body)
	if err != nil {
		t.Fatal(err)
	}

	// Compute the total record size: HeaderLen + len(body) + FooterLen.
	totalSize := int64(HeaderLen + len(body) + FooterLen)

	s.mu.RLock()
	seg := s.segs[segID]
	s.mu.RUnlock()
	if seg == nil {
		t.Fatalf("segment %d not found", segID)
	}

	// Single-pread path (size > 0).
	recSingle, err := readRecordAt(seg, off, totalSize)
	if err != nil {
		t.Fatalf("readRecordAt single: %v", err)
	}
	if recSingle.Key != 1 {
		t.Errorf("single: key=%d, want 1", recSingle.Key)
	}
	if string(recSingle.Body) != string(body) {
		t.Errorf("single: body=%q, want %q", recSingle.Body, body)
	}
	if recSingle.SegID != segID {
		t.Errorf("single: segID=%d, want %d", recSingle.SegID, segID)
	}

	// Legacy 3-pread path (size == 0).
	recLegacy, err := readRecordAt(seg, off, 0)
	if err != nil {
		t.Fatalf("readRecordAt legacy: %v", err)
	}
	if recLegacy.Key != recSingle.Key {
		t.Errorf("key mismatch: legacy=%d, single=%d", recLegacy.Key, recSingle.Key)
	}
	if string(recLegacy.Body) != string(recSingle.Body) {
		t.Errorf("body mismatch: legacy=%q, single=%q", recLegacy.Body, recSingle.Body)
	}
}

// TestReadRecordAtSinglePreadCorrupted verifies that corrupted or stale
// index entries with invalid sizes return ErrTornRecord instead of panicking.
func TestReadRecordAtSinglePreadCorrupted(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 256 << 20, SegMax: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	body := []byte("test body")
	segID, off, err := s.Put(1, body)
	if err != nil {
		t.Fatal(err)
	}

	s.mu.RLock()
	seg := s.segs[segID]
	s.mu.RUnlock()
	if seg == nil {
		t.Fatalf("segment %d not found", segID)
	}

	// Size too small (< HeaderLen + FooterLen).
	_, err = readRecordAt(seg, off, 5)
	if !errors.Is(err, ErrTornRecord) {
		t.Errorf("size=5: err=%v, want ErrTornRecord", err)
	}

	// Size larger than actual record (bodyLen in header won't match).
	_, err = readRecordAt(seg, off, 1000)
	if err == nil {
		t.Error("size=1000: expected error, got nil")
	}
}

// BenchmarkReadRecordAtSinglePread measures the single-pread path (1 syscall)
// vs the legacy 3-pread path (3 syscalls).
func BenchmarkReadRecordAtSinglePread(b *testing.B) {
	dir := b.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 256 << 20, SegMax: 4 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	body := []byte(fmt.Sprintf("bench-body-padding-to-256-bytes" +
		"------------------------------------------------------------" +
		"------------------------------------------------------------" +
		"------------------------------------------------------------" +
		"----------------------------------------------"))
	segID, off, err := s.Put(1, body)
	if err != nil {
		b.Fatal(err)
	}

	totalSize := int64(HeaderLen + len(body) + FooterLen)

	s.mu.RLock()
	seg := s.segs[segID]
	s.mu.RUnlock()
	if seg == nil {
		b.Fatalf("segment %d not found", segID)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		rec, err := readRecordAt(seg, off, totalSize)
		if err != nil {
			b.Fatal(err)
		}
		if rec.Key != 1 {
			b.Fatalf("key=%d, want 1", rec.Key)
		}
	}
}

// BenchmarkWarmEvict_OverBudgetPut measures the cost of the eviction
// path on over-budget Puts. Each Put in this benchmark triggers
// evictToFit → evictOne, which writes a tombstone and removes a victim
// under seg.mu + idxMu. This is the path the PR adds; it is NOT on the
// hot path (only fires when live bytes exceed maxBytes), but the
// benchmark proves the cost is bounded and reports allocs/op so
// regressions are caught.
func BenchmarkWarmEvict_OverBudgetPut(b *testing.B) {
	dir := b.TempDir()
	// Small budget so every Put after warm-up triggers eviction.
	// Each record = 120 bytes (16 header + 100 body + 4 footer).
	// Budget 6000 bytes ≈ 50 live records.
	s, err := NewStore(Config{Dir: dir, MaxBytes: 6000, SegMax: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	body := make([]byte, 100)
	// Fill to budget with 50 entries.
	for i := range 50 {
		if _, _, err := s.Put(uint64(i), body); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		// Each Put evicts one entry to make room. Keys cycle above
		// the seed range so they are non-protected victims.
		if _, _, err := s.Put(uint64(1000+i), body); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWarmEvict_ConcurrentPutGet measures the latency impact of
// eviction on concurrent Get operations. Eviction holds idxMu briefly
// (bounded by maxWarmEvictSkips); this benchmark verifies Gets are not
// starved when eviction runs on the write path.
func BenchmarkWarmEvict_ConcurrentPutGet(b *testing.B) {
	dir := b.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 12000, SegMax: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	body := make([]byte, 100)
	for i := range 100 {
		if _, _, err := s.Put(uint64(i), body); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			if i%4 == 0 {
				// Write path: triggers eviction (over budget).
				_, _, _ = s.Put(uint64(1000+i), body)
			} else {
				// Read path: concurrent Get.
				_, _ = s.Get(i % 100)
			}
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
	cutAt := lastOff + HeaderLen + 4 // header + partial body
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

// TestGet_StaleSegmentSelfHeals reproduces issue #193: an index entry
// pointing at a segment that no longer exists (e.g. because Compact
// swapped the segment set while a Put was mid-flight). Get must treat
// this as a miss and delete the stale entry so subsequent lookups
// don't keep hitting the same error.
func TestGet_StaleSegmentSelfHeals(t *testing.T) {
	t.Parallel()
	s := tmpStore(t)

	// Populate the store so there is at least one real segment, then
	// inject an index entry pointing at a segment ID that does not
	// exist in s.segs (mimicking the Put/Compact race from #193).
	if _, _, err := s.Put(1, []byte("real")); err != nil {
		t.Fatalf("Put real: %v", err)
	}
	const staleKey = uint64(99)
	s.SetIndex(staleKey, 9999, 0)

	got, err := s.Get(staleKey)
	if err != nil {
		t.Fatalf("Get stale key: expected nil error, got %v", err)
	}
	if got != nil {
		t.Fatalf("Get stale key: expected nil (miss), got %q", got)
	}

	s.idxMu.RLock()
	_, ok := s.index[staleKey]
	s.idxMu.RUnlock()
	if ok {
		t.Fatal("stale index entry should be dropped after segment-not-found Get")
	}

	// The self-heal counter must reflect the drop.
	if n := s.SelfHeals(); n != 1 {
		t.Fatalf("SelfHeals after stale Get = %d, want 1", n)
	}

	// A second Get must also be a clean miss — no error, no entry, and
	// must not re-increment the counter (no entry to drop).
	if _, err := s.Get(staleKey); err != nil {
		t.Fatalf("second Get stale key: expected nil error, got %v", err)
	}
	if n := s.SelfHeals(); n != 1 {
		t.Fatalf("SelfHeals after second stale Get = %d, want 1", n)
	}
}

// TestGet_StaleSegmentConcurrentCompact verifies that Get never returns
// a "segment not found" error when racing with Compact. The self-heal
// path turns stale entries into misses instead of errors, so the only
// acceptable outcomes under concurrency are a value or nil.
func TestGet_StaleSegmentConcurrentCompact(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 256 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Write enough live records to make compaction worthwhile, plus a
	// batch of tombstones so Compact actually reclaims space.
	for i := range 500 {
		if _, _, err := s.Put(uint64(i), []byte(fmt.Sprintf("body-%d", i))); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	for i := range 250 {
		if _, err := s.Delete(uint64(i)); err != nil {
			t.Fatalf("Delete %d: %v", i, err)
		}
	}

	// Inject a stale entry pointing at a non-existent segment (mimics
	// the Put/Compact race from #193 where Put writes an index entry
	// for a segment that Compact has already unlinked).
	const staleKey = uint64(7777)
	s.SetIndex(staleKey, 9999, 0)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.Compact(); err != nil {
			t.Errorf("Compact: %v", err)
		}
	}()

	// Hammer Get on the stale key while Compact runs. Every call must
	// either return nil (miss, possibly already self-healed) or the
	// empty miss path — never an error.
	for range 200 {
		got, err := s.Get(staleKey)
		if err != nil {
			t.Fatalf("Get stale key under Compact: expected nil error, got %v", err)
		}
		if got != nil {
			t.Fatalf("Get stale key under Compact: expected nil (miss), got %q", got)
		}
	}

	wg.Wait()

	// After Compact finishes, the stale entry must be gone.
	got, err := s.Get(staleKey)
	if err != nil {
		t.Fatalf("Get stale key after Compact: expected nil error, got %v", err)
	}
	if got != nil {
		t.Fatalf("Get stale key after Compact: expected nil (miss), got %q", got)
	}
	s.idxMu.RLock()
	_, ok := s.index[staleKey]
	s.idxMu.RUnlock()
	if ok {
		t.Fatal("stale index entry should be dropped after concurrent Compact")
	}
}

// TestGet_DropStaleIndexDoesNotClobberConcurrentPut verifies the
// compare-and-delete in dropStaleIndex: if a concurrent Put writes a
// valid entry for the same key between Get's idxMu.RUnlock and the
// idxMu.Lock in the self-heal path, the valid entry must survive.
// Without the compare-and-delete, the self-heal would delete the valid
// entry and the key would become a permanent miss.
func TestGet_DropStaleIndexDoesNotClobberConcurrentPut(t *testing.T) {
	t.Parallel()
	s := tmpStore(t)

	// Write a real record so there is a valid segment, then inject a
	// stale entry for a key that points at a non-existent segment.
	if _, _, err := s.Put(1, []byte("real")); err != nil {
		t.Fatalf("Put real: %v", err)
	}
	const key = uint64(42)
	s.SetIndex(key, 9999, 0)

	// Simulate the TOCTOU window: read the stale loc, then have a
	// concurrent Put write a valid entry before the self-heal runs.
	// We do this by calling Get in a goroutine and racing a Put, but
	// to make it deterministic we directly exercise dropStaleIndex
	// with the stale loc after the Put has landed.
	staleLoc := warmLoc{segID: 9999, offset: 0}

	// Write a valid record for the same key.
	validBody := []byte("valid-after-compact")
	if _, _, err := s.Put(key, validBody); err != nil {
		t.Fatalf("Put valid: %v", err)
	}

	// Now call dropStaleIndex with the *old* stale location. The
	// entry now points at the valid Put, so the compare-and-delete
	// must NOT delete it.
	s.dropStaleIndex(key, staleLoc)

	s.idxMu.RLock()
	loc, ok := s.index[key]
	s.idxMu.RUnlock()
	if !ok {
		t.Fatal("valid index entry was clobbered by dropStaleIndex with stale loc")
	}
	if loc.segID == 9999 {
		t.Fatal("dropStaleIndex deleted the valid entry — compare-and-delete is broken")
	}

	// The valid entry must still serve.
	got, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get after dropStaleIndex: %v", err)
	}
	if string(got) != "valid-after-compact" {
		t.Fatalf("Get = %q, want %q", got, "valid-after-compact")
	}

	// The self-heal counter must NOT have incremented — nothing was dropped.
	if n := s.SelfHeals(); n != 0 {
		t.Fatalf("SelfHeals = %d, want 0 (no stale entry was dropped)", n)
	}
}

func TestPut_OverBudget(t *testing.T) {
	t.Parallel()

	// Small budget so we can exceed it quickly.
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 512, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// A record is HeaderLen(16) + len(body) + FooterLen(4) = 20 + len(body).
	// With a 512-byte budget we can fit several small records but not
	// arbitrarily many.
	smallBody := make([]byte, 100) // 120 bytes per record
	for i := 0; i < 4; i++ {
		if _, _, err := s.Put(uint64(i), smallBody); err != nil {
			t.Fatalf("Put %d under budget: %v", i, err)
		}
	}
	// 4 records × 120 = 480 live bytes. One more 120-byte record would
	// push live bytes to 600 > 512, but eviction frees 120 bytes first
	// (360 + 120 = 480 ≤ 512). Mark all as protected to prevent
	// eviction, forcing ErrOverBudget.
	for i := 0; i < 4; i++ {
		s.Protect(uint64(i))
	}
	_, _, err = s.Put(99, smallBody)
	if !errors.Is(err, ErrOverBudget) {
		t.Fatalf("Put over budget with all protected: err=%v, want ErrOverBudget", err)
	}
}

func TestPut_UnderBudgetSucceeds(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 1024, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, 100) // 120 bytes per record
	for i := 0; i < 8; i++ {
		if _, _, err := s.Put(uint64(i), body); err != nil {
			t.Fatalf("Put %d under budget: %v", i, err)
		}
	}
	// 8 × 120 = 960 < 1024. All should succeed.
	got, err := s.Get(7)
	if err != nil {
		t.Fatalf("Get 7: %v", err)
	}
	if got == nil {
		t.Fatal("Get 7: expected data, got nil")
	}
}

func TestPut_MaxBytesZeroDisablesEnforcement(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 0, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// With maxBytes == 0 there is no limit. Writing many records
	// must not return ErrOverBudget.
	body := make([]byte, 100)
	for i := 0; i < 100; i++ {
		if _, _, err := s.Put(uint64(i), body); err != nil {
			t.Fatalf("Put %d with maxBytes=0: %v", i, err)
		}
	}
}

func TestCompact_SucceedsWhenDiskExceedsMaxBytes(t *testing.T) {
	t.Parallel()

	// Each live record: HeaderLen(16) + 100 body + FooterLen(4) = 120 bytes.
	// Each tombstone: HeaderLen(16) + FooterLen(4) = 20 bytes.
	const maxBytes = int64(3600)
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: maxBytes, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, 100) // 120 bytes per record
	// Write 30 live records (3600 bytes = exactly maxBytes).
	for i := 0; i < 30; i++ {
		segID, off, err := s.Put(uint64(i), body)
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		s.SetIndex(uint64(i), segID, off)
	}
	// Delete 15 keys. Each Delete appends a 20-byte tombstone with
	// no budget check, pushing diskBytes to 3600 + 15*20 = 3900 > maxBytes.
	// Put no longer gates on diskBytes (it uses stats.bytes), so this
	// does not cause Put rejection — but it does affect NeedsCompaction,
	// which compares live bytes to diskBytes for the dead-space ratio.
	for i := 0; i < 15; i++ {
		if _, err := s.Delete(uint64(i)); err != nil {
			t.Fatalf("Delete %d: %v", i, err)
		}
	}
	// Verify diskBytes exceeds maxBytes: tombstones accumulate past the
	// budget on disk even though live bytes (stats.bytes) are under it.
	if db := s.diskBytes(); db <= maxBytes {
		t.Fatalf("diskBytes = %d, want > %d (tombstones should push past budget)", db, maxBytes)
	}

	// Compaction must succeed even though diskBytes > maxBytes.
	// Live records (15 * 120 = 1800 bytes, well under maxBytes)
	// fit in the temp store regardless of its budget setting.
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact with diskBytes > maxBytes: %v", err)
	}

	// Verify live keys survived.
	for i := 15; i < 30; i++ {
		got, err := s.Get(uint64(i))
		if err != nil {
			t.Fatalf("Get %d after compact: %v", i, err)
		}
		if got == nil {
			t.Fatalf("key %d: expected live after compact, got nil", i)
		}
	}
}

func TestDelete_UpdatesStats(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	body := []byte("hello warm tier")
	for i := 0; i < 5; i++ {
		if _, _, err := s.Put(uint64(i), body); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	entriesBefore, bytesBefore := s.Stats()
	if entriesBefore != 5 {
		t.Fatalf("entries before delete = %d, want 5", entriesBefore)
	}
	// Each record: HeaderLen(16) + len(body) + FooterLen(4) = 35 bytes.
	wantBytes := int64(5 * (HeaderLen + len(body) + FooterLen))
	if bytesBefore != wantBytes {
		t.Fatalf("bytes before delete = %d, want %d", bytesBefore, wantBytes)
	}

	// Delete keys 0, 1, 2.
	for i := 0; i < 3; i++ {
		if _, err := s.Delete(uint64(i)); err != nil {
			t.Fatalf("Delete %d: %v", i, err)
		}
	}

	entriesAfter, bytesAfter := s.Stats()
	if entriesAfter != 2 {
		t.Fatalf("entries after delete = %d, want 2", entriesAfter)
	}
	wantBytesAfter := int64(2 * (HeaderLen + len(body) + FooterLen))
	if bytesAfter != wantBytesAfter {
		t.Fatalf("bytes after delete = %d, want %d", bytesAfter, wantBytesAfter)
	}

	// Delete remaining keys so the store is empty.
	for i := 3; i < 5; i++ {
		if _, err := s.Delete(uint64(i)); err != nil {
			t.Fatalf("Delete %d: %v", i, err)
		}
	}

	entriesEmpty, bytesEmpty := s.Stats()
	if entriesEmpty != 0 {
		t.Fatalf("entries after deleting all = %d, want 0", entriesEmpty)
	}
	if bytesEmpty != 0 {
		t.Fatalf("bytes after deleting all = %d, want 0", bytesEmpty)
	}
}

func TestDelete_NonExistentKey_NoStatChange(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Put one key.
	if _, _, err := s.Put(42, []byte("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	entriesBefore, bytesBefore := s.Stats()

	// Delete a key that was never put — should not change stats.
	if _, err := s.Delete(999); err != nil {
		t.Fatalf("Delete non-existent: %v", err)
	}

	entriesAfter, bytesAfter := s.Stats()
	if entriesAfter != entriesBefore {
		t.Fatalf("entries changed from %d to %d after deleting non-existent key",
			entriesBefore, entriesAfter)
	}
	if bytesAfter != bytesBefore {
		t.Fatalf("bytes changed from %d to %d after deleting non-existent key",
			bytesBefore, bytesAfter)
	}
}

func TestStats_AccurateAfterDeleteWithoutRecompute(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Put two keys with different body sizes.
	bodyA := []byte("AAAA")     // 4 bytes
	bodyB := []byte("BBBBBBBB") // 8 bytes

	if _, _, err := s.Put(1, bodyA); err != nil {
		t.Fatalf("Put A: %v", err)
	}

	if _, _, err := s.Put(2, bodyB); err != nil {
		t.Fatalf("Put B: %v", err)
	}

	// Delete key 1. Stats must reflect only key 2's bytes.
	if _, err := s.Delete(1); err != nil {
		t.Fatalf("Delete 1: %v", err)
	}

	entries, bytes := s.Stats()
	if entries != 1 {
		t.Fatalf("entries = %d, want 1", entries)
	}
	wantBytes := int64(HeaderLen + len(bodyB) + FooterLen)
	if bytes != wantBytes {
		t.Fatalf("bytes = %d, want %d (only key 2)", bytes, wantBytes)
	}
}

func TestDelete_SetIndexEntry_SkipsBytesDecrement(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Use an isolated store so only the SetIndex entry exists — no real
	// Put entries that would confound the stats assertions.
	dir2 := t.TempDir()
	s2, err := NewStore(Config{Dir: dir2, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore s2: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	// Write one real record to get a valid segID/offset, then copy
	// that loc into a second store via SetIndex.
	body := []byte("real record data")
	segID, off, err := s2.Put(1, body)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Remove key 1 so the only index entry in s2 is the SetIndex one.
	if _, err := s2.Delete(1); err != nil {
		t.Fatalf("Delete 1: %v", err)
	}
	// Now stats are zero. Inject a SetIndex entry (size=0).
	s2.SetIndex(2, segID, off)

	// SetIndex does not touch stats, so entries and bytes are still 0.
	entriesBefore, bytesBefore := s2.Stats()
	if entriesBefore != 0 {
		t.Fatalf("entries before = %d, want 0", entriesBefore)
	}
	if bytesBefore != 0 {
		t.Fatalf("bytes before = %d, want 0", bytesBefore)
	}

	// Delete the SetIndex entry (size=0). Delete decrements entries
	// because the key exists in the index, but does NOT subtract from
	// bytes because loc.size is 0. The stats undercount entries (goes
	// negative) because SetIndex never counted the entry. This is the
	// documented tradeoff: SetIndex is replay-only and RecomputeStats
	// runs after replay to restore accuracy.
	if _, err := s2.Delete(2); err != nil {
		t.Fatalf("Delete SetIndex entry: %v", err)
	}

	entriesAfter, bytesAfter := s2.Stats()
	// entries was 0, Delete subtracts 1 → -1 (wraps in int64). This is
	// wrong in absolute terms but correct relative to the input: the
	// entry was never counted, so subtracting produces an undercount.
	// RecomputeStats is the source of truth after replay.
	if entriesAfter != -1 {
		t.Fatalf("entries after = %d, want -1 (undercount: SetIndex did not increment)",
			entriesAfter)
	}
	if bytesAfter != 0 {
		t.Fatalf("bytes after = %d, want 0 (unchanged because size=0)",
			bytesAfter)
	}
}

func TestEvict_FreesSpaceUnderPressure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 512, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Each record = HeaderLen(16) + 100 + FooterLen(4) = 120 bytes.
	body := make([]byte, 100)
	for i := range 4 {
		if _, _, err := s.Put(uint64(i), body); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	// 4 * 120 = 480 bytes. Access keys 0-2 so SIEVE marks them visited,
	// leave key 3 unvisited (never accessed after Put).
	for i := range 3 {
		if _, err := s.Get(uint64(i)); err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
	}

	entriesBefore, bytesBefore := s.Stats()
	if entriesBefore != 4 {
		t.Fatalf("entries before evict = %d, want 4", entriesBefore)
	}

	// Evict one entry.
	evicted, ok := s.evictOne()
	if !ok {
		t.Fatal("evictOne returned false, expected to evict one entry")
	}
	if evicted != 3 {
		t.Fatalf("evicted key = %d, want 3 (coldest/never-accessed)", evicted)
	}

	entriesAfter, bytesAfter := s.Stats()
	if entriesAfter != 3 {
		t.Fatalf("entries after evict = %d, want 3", entriesAfter)
	}
	if bytesAfter >= bytesBefore {
		t.Fatalf("bytes after evict = %d, should be < %d", bytesAfter, bytesBefore)
	}

	// Evicted key must be gone from the index.
	got, err := s.Get(3)
	if err != nil {
		t.Fatalf("Get evicted key: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for evicted key, got %q", got)
	}
}

func TestEvict_PrefersLeastRecentlyAccessed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// SIEVE: entries are inserted at head, so key 0 is at tail.
	// Accessing all sets visited=true on every entry. Evict sweeps from
	// the tail, clearing visited bits; after one full sweep all bits are
	// clear and the tail (key 0) is evicted.
	for i := range 5 {
		if _, _, err := s.Put(uint64(i), []byte("data")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// Access all so every entry gets a visited bit.
	for i := range 5 {
		if _, err := s.Get(uint64(i)); err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
	}

	// Evict should select key 0 (tail, first to have its visited bit
	// cleared and be evicted on the next sweep).
	evicted, ok := s.evictOne()
	if !ok {
		t.Fatal("evictOne returned false")
	}
	if evicted != 0 {
		t.Fatalf("evicted = %d, want 0 (tail after visited-bit sweep)", evicted)
	}
}

func TestEvict_EmptyStoreReturnsFalse(t *testing.T) {
	t.Parallel()
	s := tmpStore(t)

	_, ok := s.evictOne()
	if ok {
		t.Fatal("evictOne on empty store should return false")
	}
}

func TestEvict_TombstonesEvictedKey(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if _, _, err := s.Put(42, []byte("evict-me")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	evicted, ok := s.evictOne()
	if !ok {
		t.Fatal("evictOne returned false")
	}
	if evicted != 42 {
		t.Fatalf("evicted = %d, want 42", evicted)
	}

	// Verify a tombstone was written for the key.
	var tombCount int
	if err := s.Scan(func(r Record) error {
		if r.IsTomb && r.Key == 42 {
			tombCount++
		}
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if tombCount != 1 {
		t.Fatalf("expected 1 tombstone for key 42, got %d", tombCount)
	}
}

func TestEvict_SkipsProtectedEntries(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	for i := range 4 {
		if _, _, err := s.Put(uint64(i), []byte("data")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// Mark keys 0 and 1 as protected (in hot tier).
	s.Protect(0)
	s.Protect(1)

	// Access all so SIEVE visited bits are set — the only differentiator
	// is protection.
	for i := range 4 {
		if _, err := s.Get(uint64(i)); err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
	}

	// evictOne should skip protected entries and pick a cold one.
	evicted, ok := s.evictOne()
	if !ok {
		t.Fatal("evictOne returned false")
	}
	if evicted == 0 || evicted == 1 {
		t.Fatalf("evicted protected key %d, should have skipped it", evicted)
	}
}

func TestEvict_AllProtectedReturnsFalse(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	for i := range 3 {
		if _, _, err := s.Put(uint64(i), []byte("data")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	// All protected — evictOne skips them and returns false within
	// the skip budget rather than scanning the whole list under idxMu.
	for i := range 3 {
		s.Protect(uint64(i))
	}
	if _, ok := s.evictOne(); ok {
		t.Fatal("evictOne returned true with all entries protected, want false")
	}
	// Entries must still be present and accounted for.
	entries, _ := s.Stats()
	if entries != 3 {
		t.Fatalf("entries after failed evict = %d, want 3", entries)
	}
}

func TestPut_EvictsBeforeRejectingOverBudget(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 512, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Each record = 120 bytes. 4 records = 480 live bytes.
	body := make([]byte, 100)
	for i := range 4 {
		if _, _, err := s.Put(uint64(i), body); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// Access keys 0-2 so SIEVE marks them visited. Key 3 (unvisited,
	// at the tail) is the eviction victim.
	for i := range 3 {
		if _, err := s.Get(uint64(i)); err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
	}

	// Put a 5th key: live bytes 480 + 120 = 600 > 512, but Evict can
	// free 120 live bytes (480 - 120 + 120 = 480 <= 512). This should
	// succeed, not return ErrOverBudget.
	_, _, err = s.Put(99, body)
	if err != nil {
		t.Fatalf("Put with eviction should succeed, got: %v", err)
	}

	// Key 3 (coldest) should be evicted.
	got, err := s.Get(3)
	if err != nil {
		t.Fatalf("Get 3 after eviction: %v", err)
	}
	if got != nil {
		t.Fatalf("key 3 should have been evicted, got %q", got)
	}

	// Key 99 should be present.
	got, err = s.Get(99)
	if err != nil {
		t.Fatalf("Get 99 after put-with-evict: %v", err)
	}
	if got == nil {
		t.Fatal("key 99 should exist")
	}
}

func TestEvict_CallbackNotifiesHotTier(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	var evicted atomic.Int64
	s.OnEvict = func(key uint64) {
		evicted.Store(int64(key))
	}

	if _, _, err := s.Put(42, []byte("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, ok := s.evictOne()
	if !ok {
		t.Fatal("evictOne returned false")
	}
	if got != 42 {
		t.Fatalf("evicted = %d, want 42", got)
	}
	if evicted.Load() != 42 {
		t.Fatalf("OnEvict callback not called or wrong key: got %d, want 42", evicted.Load())
	}
}

func TestEvict_ConcurrentEvictAndGet(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	for i := range 100 {
		if _, _, err := s.Put(uint64(i), []byte("data")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	var evictedKeys sync.Map
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 50 {
			if k, ok := s.evictOne(); ok {
				evictedKeys.Store(k, struct{}{})
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := range 100 {
			_, _ = s.Get(uint64(i))
		}
	}()
	wg.Wait()

	// Verify invariants: every evicted key must be absent from the
	// index, and stats.entries must equal the number of keys still
	// in the index.
	entries, _ := s.Stats()
	idxKeys := s.Keys()
	if entries != int64(len(idxKeys)) {
		t.Fatalf("stats.entries = %d, but index has %d keys", entries, len(idxKeys))
	}

	// Every evicted key must be absent from the index.
	for _, k := range idxKeys {
		if _, evicted := evictedKeys.Load(k); evicted {
			t.Fatalf("key %d was evicted but is still in the index", k)
		}
	}
}

// TestEvict_ConcurrentEvictAndPutPreservesData exercises the TOCTOU fix
// in evictOne: a concurrent Put for a key that evictOne just selected as
// its victim must NOT have its fresh live record tombstoned. Before the
// fix, evictOne released idxMu between the index removal and the
// tombstone write, so a racing Put could insert a new live record that
// the tombstone then clobbered — silent data loss.
func TestEvict_ConcurrentEvictAndPutPreservesData(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Seed with 50 entries, none protected so evictOne can pick them.
	for i := range 50 {
		if _, _, err := s.Put(uint64(i), []byte("seed")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 50 {
			_, _ = s.evictOne()
		}
	}()
	// Concurrent Puts reusing the same key space — this is the race:
	// a Put for a key that evictOne just selected must not have its
	// fresh record tombstoned by the eviction.
	go func() {
		defer wg.Done()
		for i := range 50 {
			if _, _, err := s.Put(uint64(i), []byte("fresh")); err != nil {
				t.Errorf("Put %d: %v", i, err)
			}
		}
	}()
	wg.Wait()

	// Invariant: every key in the index must be readable with a
	// non-nil body. A key whose live record was tombstoned by the
	// eviction race (the TOCTOU this test targets) would return nil
	// here while still present in the index — silent data loss.
	//
	// We do not assert stats.entries == len(index): concurrent
	// evictOne + Put on the same key can race on the entries counter
	// (evict decrements, Put may or may not increment depending on
	// timing), so the counter may be off by the number of races.
	idxKeys := s.Keys()
	for _, k := range idxKeys {
		body, err := s.Get(k)
		if err != nil {
			t.Fatalf("Get %d: %v", k, err)
		}
		if body == nil {
			t.Fatalf("key %d in index but Get returned nil (tombstoned by eviction race)", k)
		}
	}
}

func TestScanSegment_MmapCorrectness(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 256 << 20, SegMax: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	const numRecords = 1000
	for i := range numRecords {
		body := []byte(fmt.Sprintf("body-%d-padding", i))
		if _, _, err := s.Put(uint64(i), body); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	var count int
	var lastKey uint64
	err = s.Scan(func(r Record) error {
		if r.IsTomb {
			return nil
		}
		count++
		lastKey = r.Key
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if count != numRecords {
		t.Fatalf("expected %d records, got %d", numRecords, count)
	}
	if lastKey != numRecords-1 {
		t.Fatalf("last key = %d, want %d", lastKey, numRecords-1)
	}
}

func TestScanSegment_MmapTornTrailing(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 256 << 20, SegMax: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	for i := range 10 {
		if _, _, err := s.Put(uint64(i), []byte("body")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// Truncate the last segment to simulate a torn write.
	s.mu.RLock()
	seg := s.segs[len(s.segs)-1]
	s.mu.RUnlock()
	seg.mu.Lock()
	info, _ := seg.f.Stat()
	seg.mu.Unlock()
	if info.Size() > 0 {
		if err := os.Truncate(seg.Path, info.Size()-2); err != nil {
			t.Fatalf("Truncate: %v", err)
		}
	}

	// Scan should skip the torn trailing record, not error.
	var count int
	err = s.Scan(func(r Record) error {
		if !r.IsTomb {
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Scan with torn trailing record: %v", err)
	}
	// At least some records should be intact (exact count depends on
	// where the truncation falls).
	if count == 0 {
		t.Fatal("expected at least 1 intact record, got 0")
	}
}

func BenchmarkScanSegment(b *testing.B) {
	dir := b.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 256 << 20, SegMax: 4 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	body := make([]byte, 256)
	for i := range body {
		body[i] = byte(i)
	}
	const numRecords = 5000
	for i := range numRecords {
		if _, _, err := s.Put(uint64(i), body); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		var count int
		if err := s.Scan(func(r Record) error {
			if !r.IsTomb {
				count++
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
		if count != numRecords {
			b.Fatalf("count = %d, want %d", count, numRecords)
		}
	}
}

// TestDelete_RemovesSIEVEEntry is a regression test for the BLOCKER
// where Delete removed the index map entry but left the SIEVE list
// entry orphaned. Orphaned entries waste eviction skip probes and
// can cause ErrOverBudget even when live non-protected victims exist.
func TestDelete_RemovesSIEVEEntry(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	for i := range 10 {
		if _, _, err := s.Put(uint64(i), []byte("data")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	// Delete all entries — the SIEVE list must be empty after this.
	for i := range 10 {
		if _, err := s.Delete(uint64(i)); err != nil {
			t.Fatalf("Delete %d: %v", i, err)
		}
	}

	s.idxMu.RLock()
	listLen := s.evictList.Len()
	s.idxMu.RUnlock()
	if listLen != 0 {
		t.Fatalf("SIEVE list has %d orphaned entries after deleting all keys, want 0", listLen)
	}
}

// TestPut_OverwriteDoesNotInflateStats is a regression test for the
// stats drift where Put on an existing key incremented stats.bytes
// and stats.entries without subtracting the old entry's size. After
// N overwrites of the same key, stats.bytes counted N × record_size
// for a single live record, causing evictToFit to evict unnecessarily.
func TestPut_OverwriteDoesNotInflateStats(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, 100) // 120 bytes per record
	// Overwrite the same key 5 times.
	for range 5 {
		if _, _, err := s.Put(42, body); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	entries, bytes := s.Stats()
	if entries != 1 {
		t.Fatalf("entries = %d, want 1 (overwrite should not increment entries)", entries)
	}
	wantBytes := int64(HeaderLen + len(body) + FooterLen)
	if bytes != wantBytes {
		t.Fatalf("bytes = %d, want %d (overwrite should not inflate bytes)", bytes, wantBytes)
	}
}

// TestCompact_RestoresStatsBytes is a regression test for the bug where
// Compact set stats.bytes from fresh.stats.bytes.Load() — but the fresh
// store (NewStore) doesn't run RecomputeStats, so its stats.bytes is 0.
// This left evictToFit blind after compaction (stats.bytes == 0 means
// no eviction until the budget is far exceeded). The fix computes
// liveBytes from the rebuilt index, where every entry's size was set
// by compactSegments.
func TestCompact_RestoresStatsBytes(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	body := make([]byte, 100)
	for i := range 50 {
		if _, _, err := s.Put(uint64(i), body); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	// Delete half to create tombstones so compaction has work to do.
	for i := range 25 {
		if _, err := s.Delete(uint64(i)); err != nil {
			t.Fatalf("Delete %d: %v", i, err)
		}
	}

	entriesBefore, bytesBefore := s.Stats()
	if entriesBefore != 25 {
		t.Fatalf("entries before compact = %d, want 25", entriesBefore)
	}
	wantBytes := int64(25 * (HeaderLen + len(body) + FooterLen))
	if bytesBefore != wantBytes {
		t.Fatalf("bytes before compact = %d, want %d", bytesBefore, wantBytes)
	}

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// After compact: entries must match, and stats.bytes must be
	// recomputed from the index (not 0).
	entries, bytes := s.Stats()
	if entries != 25 {
		t.Fatalf("entries after compact = %d, want 25", entries)
	}
	if bytes != wantBytes {
		t.Fatalf("bytes after compact = %d, want %d (Compact must recompute stats.bytes from index)", bytes, wantBytes)
	}

	// evictToFit must work after compact — if stats.bytes were 0,
	// the budget check would pass for any recSize and eviction
	// wouldn't fire until the tier is massively over budget.
	// Verify by checking that a Put that would exceed budget triggers
	// eviction (not rejection). We need a small budget for this:
	s2, err := NewStore(Config{Dir: t.TempDir(), MaxBytes: 4 << 10, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("NewStore s2: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	smallBody := make([]byte, 50) // 70 bytes per record, ~58 in 4 KiB
	for i := range 40 {
		if _, _, err := s2.Put(uint64(i), smallBody); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if err := s2.Compact(); err != nil {
		t.Fatalf("Compact s2: %v", err)
	}
	_, bytesAfterCompact := s2.Stats()
	if bytesAfterCompact == 0 {
		t.Fatal("stats.bytes = 0 after compact — evictToFit would be blind")
	}
	// A new Put should trigger eviction, not be rejected outright.
	// If stats.bytes were 0, the budget check (0 + 70 <= 4096) would
	// pass and no eviction would fire — the tier would grow unbounded.
	for i := 40; i < 100; i++ {
		if _, _, err := s2.Put(uint64(i), smallBody); err != nil {
			t.Fatalf("Put %d after compact: %v (evictToFit should evict, not reject)", i, err)
		}
	}
}

// TestCompact_ConcurrentPutNoDataLoss reproduces issue #280: Compact
// races with concurrent Put, silently dropping keys written during the
// compaction window. The test fills the store with tombstones so
// compaction has work to do, then runs Put in a tight loop while
// Compact executes, and finally verifies every key written during the
// compaction window is still retrievable. A very small SegMax forces
// frequent segment rollover, maximizing the chance a Put creates a new
// segment during the unlocked scan window (the pre-fix bug).
func TestCompact_ConcurrentPutNoDataLoss(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// SegMax just large enough for a few records so Put rolls into new
	// segments frequently during compaction.
	s, err := NewStore(Config{Dir: dir, MaxBytes: 1 << 30, SegMax: 512})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Populate with live records spread across many segments, then
	// delete half to create tombstones so Compact has dead bytes to
	// reclaim.
	for i := range 400 {
		if _, _, err := s.Put(uint64(i), []byte{byte(i)}); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	for i := range 200 {
		if _, err := s.Delete(uint64(i)); err != nil {
			t.Fatalf("Delete %d: %v", i, err)
		}
	}

	// keys written during compaction, collected under a mutex so the
	// verification phase can read them after Compact returns. Each
	// write uses a unique key so the verification can distinguish a key
	// preserved by compaction from one re-written after compaction.
	var mu sync.Mutex
	var written []uint64
	stop := make(chan struct{})
	var keyCounter atomic.Uint64

	// started signals that the Put goroutine has written at least one
	// key, so Compact is guaranteed to overlap with in-flight Puts
	// rather than completing before the goroutine is scheduled (which
	// happens on fast CI runners).
	started := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		first := true
		for {
			select {
			case <-stop:
				return
			default:
			}
			key := keyCounter.Add(1)
			if _, _, err := s.Put(key, []byte{byte(key)}); err != nil {
				t.Errorf("Put during compact: %v", err)
				return
			}
			mu.Lock()
			written = append(written, key)
			mu.Unlock()
			if first {
				first = false
				close(started)
			}
		}
	}()

	// Wait until the Put goroutine has written at least one key before
	// starting compaction, guaranteeing overlap.
	<-started
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	close(stop)
	wg.Wait()

	// Every key written during the compaction window must be
	// retrievable. Under the bug, the index entry is clobbered by
	// s.index = newIndex and the segment file is removed by
	// swapSegmentFiles, so Get returns nil (miss).
	mu.Lock()
	keys := written
	mu.Unlock()
	if len(keys) == 0 {
		t.Fatal("no keys were written during compaction — test setup is broken")
	}
	seen := make(map[uint64]bool)
	var missing []uint64
	for _, k := range keys {
		if seen[k] {
			continue
		}
		seen[k] = true
		got, err := s.Get(k)
		if err != nil {
			t.Fatalf("Get %d after compact: %v", k, err)
		}
		if got == nil {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("Compact dropped %d keys written during compaction: %v", len(missing), missing[:min(len(missing), 10)])
	}
}

// TestSync_PreallocatedNilFd tests that Sync does not panic on
// preallocated segments that have never been opened (f == nil).
// Regression test for #288.
func TestSync_PreallocatedNilFd(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{
		Dir:         dir,
		MaxBytes:    100 << 20,
		SegMax:      1 << 20,
		Preallocate: 4 << 20, // 4 segments
	})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Sync should not panic on segments with f == nil.
	if err := s.Sync(); err != nil {
		t.Fatalf("Sync on preallocated (nil-fd) segments: %v", err)
	}
}
