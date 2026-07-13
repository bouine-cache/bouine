package warm

import (
	"os"
	"sync"
	"testing"
	"time"
)

func TestFDCache_TouchAndEvict(t *testing.T) {
	t.Parallel()
	c := newFDCache(2)

	segs := make([]*Segment, 3)
	for i := range segs {
		segs[i] = &Segment{ID: i, Path: t.TempDir()}
	}

	for _, seg := range segs[:2] {
		c.touch(seg)
	}
	if c.Len() != 2 {
		t.Fatalf("Len = %d, want 2", c.Len())
	}

	c.touch(segs[2])
	if c.Len() != 2 {
		t.Fatalf("Len after eviction = %d, want 2", c.Len())
	}
}

func TestFDCache_LRUTouchToFront(t *testing.T) {
	t.Parallel()
	c := newFDCache(2)

	segs := make([]*Segment, 3)
	for i := range segs {
		segs[i] = &Segment{ID: i, Path: t.TempDir()}
	}

	c.touch(segs[0])
	c.touch(segs[1])
	c.touch(segs[0])

	c.touch(segs[2])
	if c.Len() != 2 {
		t.Fatalf("Len = %d, want 2", c.Len())
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.entries[segs[0].ID]; !ok {
		t.Fatal("seg 0 should still be cached (was recently touched)")
	}
	if _, ok := c.entries[segs[1].ID]; ok {
		t.Fatal("seg 1 should have been evicted (LRU)")
	}
}

func TestFDCache_ReaderProtection(t *testing.T) {
	t.Parallel()
	f, err := os.CreateTemp(t.TempDir(), "seg")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	seg := &Segment{ID: 0, Path: f.Name(), f: f}
	seg.opened.Store(true)
	seg.readers.Add(1)

	if seg.closeIfIdle() {
		t.Fatal("closeIfIdle should return false with active readers")
	}
	if seg.f == nil {
		t.Fatal("fd should not be closed with active readers")
	}
	seg.readers.Add(-1)
	if !seg.closeIfIdle() {
		t.Fatal("closeIfIdle should return true with no readers")
	}
	if seg.f != nil {
		t.Fatal("fd should be closed after closeIfIdle with no readers")
	}
}

func TestFDCache_EvictionSkipsReaders(t *testing.T) {
	t.Parallel()
	c := newFDCache(2)

	segs := make([]*Segment, 3)
	for i := range segs {
		f, err := os.CreateTemp(t.TempDir(), "seg")
		if err != nil {
			t.Fatalf("CreateTemp: %v", err)
		}
		segs[i] = &Segment{ID: i, Path: f.Name(), f: f}
		segs[i].opened.Store(true)
	}

	c.touch(segs[0])
	c.touch(segs[1])

	segs[0].readers.Add(1)
	defer segs[0].readers.Add(-1)

	c.touch(segs[2])

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.entries[segs[0].ID]; !ok {
		t.Fatal("seg 0 with readers should still be in cache (moved to front, not evicted)")
	}
	if _, ok := c.entries[segs[1].ID]; ok {
		t.Fatal("seg 1 should have been evicted (was LRU with no readers)")
	}
	if _, ok := c.entries[segs[2].ID]; !ok {
		t.Fatal("seg 2 should be in cache")
	}
	if segs[0].f == nil {
		t.Fatal("seg 0 fd should still be open (protected by readers)")
	}
	if segs[1].f != nil {
		t.Fatal("seg 1 fd should be closed (evicted)")
	}
}

func TestFDCache_Clear(t *testing.T) {
	t.Parallel()
	c := newFDCache(10)

	for i := 0; i < 5; i++ {
		c.touch(&Segment{ID: i, Path: t.TempDir()})
	}
	if c.Len() != 5 {
		t.Fatalf("Len = %d, want 5", c.Len())
	}

	c.clear()
	if c.Len() != 0 {
		t.Fatalf("Len after clear = %d, want 0", c.Len())
	}
}

func TestFDCache_EvictionAllReadersNoInfiniteLoop(t *testing.T) {
	t.Parallel()
	c := newFDCache(2)

	segs := make([]*Segment, 3)
	for i := range segs {
		f, err := os.CreateTemp(t.TempDir(), "seg")
		if err != nil {
			t.Fatalf("CreateTemp: %v", err)
		}
		segs[i] = &Segment{ID: i, Path: f.Name(), f: f}
		segs[i].opened.Store(true)
	}

	c.touch(segs[0])
	c.touch(segs[1])

	segs[0].readers.Add(1)
	segs[1].readers.Add(1)
	defer segs[0].readers.Add(-1)
	defer segs[1].readers.Add(-1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.touch(segs[2])
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("touch hung — infinite loop when all entries have readers")
	}
}

func TestFDCache_UnlimitedNil(t *testing.T) {
	t.Parallel()
	var c *fdCache
	c.touch(&Segment{ID: 0})
	c.clear()
	if c.Len() != 0 {
		t.Fatalf("Len = %d, want 0 for nil cache", c.Len())
	}
}

func TestFDCache_CapacityZeroUnlimited(t *testing.T) {
	t.Parallel()
	c := newFDCache(0)

	for i := 0; i < 100; i++ {
		c.touch(&Segment{ID: i, Path: t.TempDir()})
	}
	if c.Len() != 0 {
		t.Fatalf("Len = %d, want 0 for unlimited cache", c.Len())
	}
}

func TestStore_SegmentCacheSizeEviction(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{
		Dir:              dir,
		MaxBytes:         100 << 20,
		SegMax:           512,
		SegmentCacheSize: 1,
	})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	for i := 0; i < 5; i++ {
		_, _, err := s.Put(uint64(i), make([]byte, 500))
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	if s.fdCache == nil {
		t.Fatal("fdCache should be non-nil with SegmentCacheSize=1")
	}
	if s.fdCache.Len() > 1 {
		t.Fatalf("fdCache.Len = %d, want <= 1", s.fdCache.Len())
	}
}

func TestStore_SegmentCacheSizeUnlimited(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{
		Dir:              dir,
		MaxBytes:         100 << 20,
		SegMax:           1 << 20,
		SegmentCacheSize: -1,
	})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if s.fdCache != nil {
		t.Fatal("fdCache should be nil with SegmentCacheSize=-1")
	}
}

func TestStore_FDCacheClearedOnCompact(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{
		Dir:              dir,
		MaxBytes:         100 << 20,
		SegMax:           1 << 20,
		SegmentCacheSize: 256,
	})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	for i := 0; i < 3; i++ {
		_, _, err := s.Put(uint64(i), make([]byte, 100))
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	if s.fdCache.Len() == 0 {
		t.Fatal("fdCache should have entries before compact")
	}

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if s.fdCache.Len() != 0 {
		t.Fatalf("fdCache.Len after compact = %d, want 0", s.fdCache.Len())
	}
}

func TestStore_FDCacheConcurrentReaders(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{
		Dir:              dir,
		MaxBytes:         100 << 20,
		SegMax:           512,
		SegmentCacheSize: 2,
	})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	for i := 0; i < 10; i++ {
		_, _, err := s.Put(uint64(i), make([]byte, 500))
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(key uint64) {
			defer wg.Done()
			_, _ = s.Get(key)
		}(uint64(i))
	}
	wg.Wait()
}

func TestStore_FDCacheClearedOnClose(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{
		Dir:              dir,
		MaxBytes:         100 << 20,
		SegMax:           512,
		SegmentCacheSize: 256,
	})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	for i := 0; i < 3; i++ {
		_, _, err := s.Put(uint64(i), make([]byte, 500))
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	if s.fdCache.Len() == 0 {
		t.Fatal("fdCache should have entries before close")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if s.fdCache.Len() != 0 {
		t.Fatalf("fdCache.Len after close = %d, want 0", s.fdCache.Len())
	}
}
