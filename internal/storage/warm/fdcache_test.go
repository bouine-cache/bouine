package warm

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	require.Equal(t, 2, c.Len())

	c.touch(segs[2])
	require.Equal(t, 2, c.Len())
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
	require.Equal(t, 2, c.Len())

	c.mu.Lock()
	defer c.mu.Unlock()
	{
		_, ok := c.entries[segs[0].ID]
		require.True(t, ok)
	}
	{
		_, ok := c.entries[segs[1].ID]
		require.False(t, ok)
	}
}

func TestFDCache_ReaderProtection(t *testing.T) {
	t.Parallel()
	f, err := os.CreateTemp(t.TempDir(), "seg")
	require.NoErrorf(t, err, "CreateTemp: %v", err)
	seg := &Segment{ID: 0, Path: f.Name(), f: f}
	seg.opened.Store(true)
	seg.readers.Add(1)

	require.False(t, seg.closeIfIdle())
	require.NotNil(t, seg.f)
	seg.readers.Add(-1)
	require.True(t, seg.closeIfIdle())
	require.Nil(t, seg.f)
}

func TestFDCache_EvictionSkipsReaders(t *testing.T) {
	t.Parallel()
	c := newFDCache(2)

	segs := make([]*Segment, 3)
	for i := range segs {
		f, err := os.CreateTemp(t.TempDir(), "seg")
		require.NoErrorf(t, err, "CreateTemp: %v", err)
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
	{
		_, ok := c.entries[segs[0].ID]
		require.True(t, ok)
	}
	{
		_, ok := c.entries[segs[1].ID]
		require.False(t, ok)
	}
	{
		_, ok := c.entries[segs[2].ID]
		require.True(t, ok)
	}
	require.NotNil(t, segs[0].f)
	require.Nil(t, segs[1].f)
}

func TestFDCache_Clear(t *testing.T) {
	t.Parallel()
	c := newFDCache(10)

	for i := 0; i < 5; i++ {
		c.touch(&Segment{ID: i, Path: t.TempDir()})
	}
	require.Equal(t, 5, c.Len())

	c.clear()
	require.Equal(t, 0, c.Len())
}

func TestFDCache_EvictionAllReadersNoInfiniteLoop(t *testing.T) {
	t.Parallel()
	c := newFDCache(2)

	segs := make([]*Segment, 3)
	for i := range segs {
		f, err := os.CreateTemp(t.TempDir(), "seg")
		require.NoErrorf(t, err, "CreateTemp: %v", err)
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
	require.Equal(t, 0, c.Len())
}

func TestFDCache_CapacityZeroUnlimited(t *testing.T) {
	t.Parallel()
	c := newFDCache(0)

	for i := 0; i < 100; i++ {
		c.touch(&Segment{ID: i, Path: t.TempDir()})
	}
	require.Equal(t, 0, c.Len())
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
	require.NoErrorf(t, err, "NewStore: %v", err)
	t.Cleanup(func() { _ = s.Close() })

	for i := 0; i < 5; i++ {
		_, _, err := s.Put(uint64(i), make([]byte, 500))
		require.NoErrorf(t, err, "Put %d: %v", i, err)
	}

	require.NotNil(t, s.fdCache)
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
	require.NoErrorf(t, err, "NewStore: %v", err)
	t.Cleanup(func() { _ = s.Close() })

	require.Nil(t, s.fdCache)
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
	require.NoErrorf(t, err, "NewStore: %v", err)
	t.Cleanup(func() { _ = s.Close() })

	for i := 0; i < 3; i++ {
		_, _, err := s.Put(uint64(i), make([]byte, 100))
		require.NoErrorf(t, err, "Put %d: %v", i, err)
	}

	require.NotEqual(t, 0, s.fdCache.Len())

	{
		err := s.Compact()
		require.NoErrorf(t, err, "Compact: %v", err)
	}

	require.Equal(t, 0, s.fdCache.Len())
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
	require.NoErrorf(t, err, "NewStore: %v", err)
	t.Cleanup(func() { _ = s.Close() })

	for i := 0; i < 10; i++ {
		_, _, err := s.Put(uint64(i), make([]byte, 500))
		require.NoErrorf(t, err, "Put %d: %v", i, err)
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
	require.NoErrorf(t, err, "NewStore: %v", err)

	for i := 0; i < 3; i++ {
		_, _, err := s.Put(uint64(i), make([]byte, 500))
		require.NoErrorf(t, err, "Put %d: %v", i, err)
	}

	require.NotEqual(t, 0, s.fdCache.Len())

	{
		err := s.Close()
		require.NoErrorf(t, err, "Close: %v", err)
	}

	require.Equal(t, 0, s.fdCache.Len())
}
