package warm

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

// TestMmapFieldNilByDefault verifies that a new segment's mmap field
// is nil before any tryMmap call.
func TestMmapFieldNilByDefault(t *testing.T) {
	t.Parallel()
	s := tmpStore(t)
	body := []byte("test body")
	segID, _, err := s.Put(1, body)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.RLock()
	seg := s.segs[segID]
	s.mu.RUnlock()
	if seg == nil {
		t.Fatalf("segment %d not found", segID)
	}
	if seg.mmap.Load() != nil {
		t.Fatal("new segment should have nil mmap")
	}
}

// TestReadRecordAtMmapFallback verifies that readRecordAtMmap returns
// nil, nil when seg.mmap is nil, causing readRecordAt to fall through
// to the pread path.
func TestReadRecordAtMmapFallback(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 256 << 20, SegMax: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	body := []byte("fallback test body")
	segID, off, err := s.Put(1, body)
	if err != nil {
		t.Fatal(err)
	}
	totalSize := int64(HeaderLen + len(body) + FooterLen)

	s.mu.RLock()
	seg := s.segs[segID]
	s.mu.RUnlock()

	// mmap is nil → readRecordAtMmap returns nil,nil → falls through to pread.
	rec, err := readRecordAtMmap(seg, off, totalSize)
	if rec != nil || err != nil {
		t.Fatalf("readRecordAtMmap with nil mmap: rec=%v, err=%v, want nil,nil", rec, err)
	}

	// Full readRecordAt should succeed via the pread fallback.
	rec2, err := readRecordAt(seg, off, totalSize)
	if err != nil {
		t.Fatalf("readRecordAt: %v", err)
	}
	if rec2.Key != 1 || string(rec2.Body) != string(body) {
		t.Errorf("readRecordAt: key=%d body=%q, want key=1 body=%q", rec2.Key, rec2.Body, body)
	}
}

// TestMmapGetAfterSegmentRotation verifies that reads from a sealed
// segment (after segment rollover) return correct data. On Linux,
// the sealed segment is mmap'd by newSegment; on non-Linux, reads
// fall back to pread.
func TestMmapGetAfterSegmentRotation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Small SegMax to trigger rollover quickly.
	// 120-byte records (16 header + 100 body + 4 footer).
	// SegMax=512 → 4 records per segment, 5th triggers rollover.
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	body := make([]byte, 100)
	for i := range body {
		body[i] = byte(i)
	}
	// Write enough records to trigger at least one rollover.
	var keys []uint64
	for i := uint64(0); i < 10; i++ {
		key := uint64(100 + i)
		if _, _, err := s.Put(key, body); err != nil {
			t.Fatalf("Put(%d): %v", key, err)
		}
		keys = append(keys, key)
	}

	// Verify all reads succeed, including from the sealed segment(s).
	for _, key := range keys {
		got, err := s.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if got == nil {
			t.Fatalf("Get(%d): unexpected nil", key)
		}
		if string(got) != string(body) {
			t.Errorf("Get(%d) = %q, want %q", key, got, body)
		}
	}

	// Verify multiple segments exist.
	s.mu.RLock()
	numSegs := len(s.segs)
	s.mu.RUnlock()
	if numSegs < 2 {
		t.Fatalf("expected >= 2 segments, got %d", numSegs)
	}
}

// TestMmapGetAfterCompact verifies that reads work correctly after
// compaction replaces all segments with fresh ones.
func TestMmapGetAfterCompact(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	body := []byte("compact test body")
	for i := uint64(0); i < 100; i++ {
		if _, _, err := s.Put(i, body); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}
	// Delete some to create tombstones for compaction.
	for i := uint64(0); i < 50; i++ {
		if _, err := s.Delete(i); err != nil {
			t.Fatalf("Delete(%d): %v", i, err)
		}
	}

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Reads from live keys should succeed.
	for i := uint64(50); i < 100; i++ {
		got, err := s.Get(i)
		if err != nil {
			t.Fatalf("Get(%d) after compact: %v", i, err)
		}
		if got == nil {
			t.Fatalf("Get(%d) after compact: unexpected nil", i)
		}
		if string(got) != string(body) {
			t.Errorf("Get(%d) after compact = %q, want %q", i, got, body)
		}
	}

	// Deleted keys should return nil (tombstone or missing).
	for i := uint64(0); i < 50; i++ {
		got, err := s.Get(i)
		if err != nil {
			t.Fatalf("Get(%d) deleted: %v", i, err)
		}
		if got != nil {
			t.Errorf("Get(%d) deleted = %q, want nil", i, got)
		}
	}
}

// TestMmapFdCacheEviction verifies that reads succeed after the fd
// is closed (simulating fdCache eviction). On Linux, the mmap mapping
// remains valid after fd close (POSIX guarantee). On non-Linux, the
// fd is reopened by ensureOpen.
func TestMmapFdCacheEviction(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// Small SegMax to trigger rollover so the old segment is sealed.
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	body := make([]byte, 100)
	for i := range body {
		body[i] = byte(i)
	}
	// Write enough to create 2 segments.
	for i := uint64(0); i < 10; i++ {
		if _, _, err := s.Put(i, body); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	// Read from the old segment to trigger lazy mmap init (on Linux).
	if _, err := s.Get(0); err != nil {
		t.Fatalf("Get(0) before eviction: %v", err)
	}

	// Close the old segment's fd to simulate fdCache eviction.
	s.mu.RLock()
	var oldSeg *Segment
	for _, seg := range s.segs {
		if seg.ID == 0 {
			oldSeg = seg
			break
		}
	}
	s.mu.RUnlock()
	if oldSeg == nil {
		t.Fatal("segment 0 not found")
	}
	if !oldSeg.closeIfIdle() {
		t.Fatal("closeIfIdle returned false (readers in flight?)")
	}

	// Read from the old segment after fd close.
	got, err := s.Get(0)
	if err != nil {
		t.Fatalf("Get(0) after fd close: %v", err)
	}
	if got == nil {
		t.Fatal("Get(0) after fd close: unexpected nil")
	}
	if string(got) != string(body) {
		t.Errorf("Get(0) after fd close = %q, want %q", got, body)
	}
}

// TestMmapConcurrentReadDuringCompact verifies that concurrent reads
// during compaction do not crash or return corrupted data. The mmap
// field adds a new pointer that Compact munmaps under s.mu.Lock;
// this test ensures the RLock/Lock hierarchy prevents use-after-munmap.
func TestMmapConcurrentReadDuringCompact(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	body := []byte("concurrent compact test body")
	for i := uint64(0); i < 500; i++ {
		if _, _, err := s.Put(i, body); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}
	// Delete half to create tombstones.
	for i := uint64(0); i < 250; i++ {
		if _, err := s.Delete(i); err != nil {
			t.Fatalf("Delete(%d): %v", i, err)
		}
	}

	var wg sync.WaitGroup
	// Concurrent readers.
	for g := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 200 {
				key := uint64(250 + (j % 250))
				got, err := s.Get(key)
				if err != nil {
					t.Errorf("goroutine %d: Get(%d): %v", g, key, err)
					return
				}
				if got == nil {
					t.Errorf("goroutine %d: Get(%d): unexpected nil", g, key)
					return
				}
				if string(got) != string(body) {
					t.Errorf("goroutine %d: Get(%d) = %q, want %q", g, key, got, body)
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

	// Verify reads still work after compaction.
	for i := uint64(250); i < 500; i++ {
		got, err := s.Get(i)
		if err != nil {
			t.Fatalf("Get(%d) post-compact: %v", i, err)
		}
		if got == nil {
			t.Fatalf("Get(%d) post-compact: unexpected nil", i)
		}
	}
}

// TestMmapNonLinuxStubs verifies that on non-Linux platforms, the mmap
// stubs are no-ops and reads fall back to pread.
func TestMmapNonLinuxStubs(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "linux" {
		t.Skip("non-Linux stub test")
	}
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	body := make([]byte, 100)
	for i := range body {
		body[i] = byte(i)
	}
	for i := uint64(0); i < 10; i++ {
		if _, _, err := s.Put(i, body); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	// tryMmap is a no-op → seg.mmap stays nil.
	s.mu.RLock()
	for _, seg := range s.segs {
		seg.tryMmap()
		if seg.mmap.Load() != nil {
			t.Errorf("segment %d: mmap should be nil on non-Linux", seg.ID)
		}
	}
	s.mu.RUnlock()

	// munmap is a no-op.
	s.mu.RLock()
	munmapAll(s.segs)
	s.mu.RUnlock()

	// readRecordAtMmap returns nil, nil.
	s.mu.RLock()
	seg := s.segs[0]
	s.mu.RUnlock()
	rec, err := readRecordAtMmap(seg, 0, 100)
	if rec != nil || err != nil {
		t.Errorf("readRecordAtMmap on non-Linux: rec=%v err=%v, want nil,nil", rec, err)
	}

	// Reads should work via pread fallback.
	for i := uint64(0); i < 10; i++ {
		got, err := s.Get(i)
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if got == nil || string(got) != string(body) {
			t.Errorf("Get(%d) = %q, want %q", i, got, body)
		}
	}
}

// BenchmarkReadRecordAtMmap benchmarks the read path after segment
// rotation. On Linux, the sealed segment is mmap'd and the read
// uses zero syscalls. On non-Linux, it falls back to single pread.
func BenchmarkReadRecordAtMmap(b *testing.B) {
	dir := b.TempDir()
	// SegMax=600: 2 records of 276 bytes fit (552), 3rd triggers rollover.
	s, err := NewStore(Config{Dir: dir, MaxBytes: 256 << 20, SegMax: 600})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	body := []byte(fmt.Sprintf("bench-body-padding-to-256-bytes" +
		"------------------------------------------------------------" +
		"------------------------------------------------------------" +
		"------------------------------------------------------------" +
		"----------------------------------------------"))
	// Write enough records to trigger rollover so key 0 is in a sealed segment.
	for i := uint64(0); i < 5; i++ {
		if _, _, err := s.Put(i, body); err != nil {
			b.Fatal(err)
		}
	}

	// Find segment 0 (sealed after rollover).
	s.mu.RLock()
	var oldSeg *Segment
	for _, seg := range s.segs {
		if seg.ID == 0 {
			oldSeg = seg
			break
		}
	}
	s.mu.RUnlock()
	if oldSeg == nil {
		b.Fatal("segment 0 not found (no rollover?)")
	}

	// Get the index entry for key 0 to find offset and size.
	s.idxMu.RLock()
	loc, ok := s.index[0]
	s.idxMu.RUnlock()
	if !ok {
		b.Fatal("key 0 not in index")
	}

	// Trigger lazy mmap init (on Linux) by doing one Get.
	if _, err := s.Get(0); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		rec, err := readRecordAt(oldSeg, loc.offset, loc.size)
		if err != nil {
			b.Fatal(err)
		}
		if rec.Key != 0 {
			b.Fatalf("key=%d, want 0", rec.Key)
		}
	}
}
