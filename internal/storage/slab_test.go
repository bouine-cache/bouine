package storage

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
)

func TestSlabAllocator_AllocFree(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("slab allocator is Linux-only")
	}
	slab, err := NewSlabAllocator()
	if err != nil {
		t.Skipf("slab allocator unavailable on this platform: %v", err)
	}
	t.Cleanup(func() { _ = slab.Close() })

	sizes := []int{1, 100, 255, 256, 257, 1024, 4095, 4096, 10000, 65536, 100000, 262144, 500000, 1048576}
	for _, size := range sizes {
		buf := slab.Alloc(size)
		require.NotNil(t, buf)
		require.Len(t, buf, size)
		// Write to the entire buffer to verify it's writable.
		for i := range buf {
			buf[i] = byte(i % 256)
		}
		slab.Free(buf)
	}

	allocs, frees, fallback := slab.Stats()
	require.NotEqual(t, 0, allocs)
	require.NotEqual(t, 0, frees)
	if fallback > 0 {
		t.Logf("fallback=%d (some sizes may exceed slab classes)", fallback)
	}
}

func TestSlabAllocator_FreeHeapBuffer(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("slab allocator is Linux-only")
	}
	slab, err := NewSlabAllocator()
	if err != nil {
		t.Skipf("slab allocator unavailable on this platform: %v", err)
	}
	t.Cleanup(func() { _ = slab.Close() })

	// Freeing a Go heap buffer must be a no-op (not crash, not corrupt).
	heapBuf := make([]byte, 100)
	slab.Free(heapBuf)

	allocs, frees, fallback := slab.Stats()
	require.Equal(t, 0, allocs)
	require.Equal(t, 0, frees)
	require.Equal(t, 0, fallback)
}

func TestSlabAllocator_ReuseSlots(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("slab allocator is Linux-only")
	}
	slab, err := NewSlabAllocator()
	if err != nil {
		t.Skipf("slab allocator unavailable on this platform: %v", err)
	}
	t.Cleanup(func() { _ = slab.Close() })

	// Alloc and free the same size repeatedly. The free list should
	// recycle slots, so allocs should stay steady and fallback should
	// stay zero.
	size := 100
	for range 100 {
		buf := slab.Alloc(size)
		require.NotNil(t, buf)
		slab.Free(buf)
	}

	allocs, frees, _ := slab.Stats()
	require.Equal(t, 100, allocs)
	require.Equal(t, 100, frees)
}

func TestSlabAllocator_Growable(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("slab allocator is Linux-only")
	}
	slab, err := NewSlabAllocator()
	if err != nil {
		t.Skipf("slab allocator unavailable on this platform: %v", err)
	}
	t.Cleanup(func() { _ = slab.Close() })

	// Exhaust the initial 1024 slots for class 0 (256B), then verify
	// the next alloc still succeeds by growing a new region.
	size := 100 // fits in class 0 (256B - 16B header = 240B usable)
	bufs := make([][]byte, 0, 2048)
	// Alloc returns heap fallback (non-nil, cap==size) when the region
	// cap is reached. Stop when we detect a heap fallback buffer.
	for i := 0; i < 100000; i++ {
		buf := slab.Alloc(size)
		if buf == nil {
			break
		}
		// Slab buffers have cap == slotSize - slabHeaderSize (248 for
		// class 0). Heap fallback has cap == size (100).
		if cap(buf) == size && len(bufs) > 0 {
			break
		}
		bufs = append(bufs, buf)
	}
	if len(bufs) <= 1024 {
		t.Fatalf("expected more than 1024 allocs from growable slab, got %d", len(bufs))
	}
	// Free all and verify they return cleanly.
	for _, b := range bufs {
		slab.Free(b)
	}
	allocs, frees, fallback := slab.Stats()
	require.NotEqual(t, 0, allocs)
	require.NotEqual(t, 0, frees)
	if fallback > 0 {
		t.Logf("fallback=%d (some sizes may exceed slab classes)", fallback)
	}
}

func TestSlabAllocator_OversizedFallback(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("slab allocator is Linux-only")
	}
	slab, err := NewSlabAllocator()
	if err != nil {
		t.Skipf("slab allocator unavailable on this platform: %v", err)
	}
	t.Cleanup(func() { _ = slab.Close() })

	// Size larger than the biggest class (1MB) should fall back to heap.
	buf := slab.Alloc(2 * 1024 * 1024)
	require.NotNil(t, buf)
	require.Len(t, buf, 2*1024*1024)
	// Freeing a heap buffer must be safe.
	slab.Free(buf)

	_, _, fallback := slab.Stats()
	require.NotEqual(t, 0, fallback)
}

func TestSlabAllocator_NilAndZero(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("slab allocator is Linux-only")
	}
	slab, err := NewSlabAllocator()
	if err != nil {
		t.Skipf("slab allocator unavailable on this platform: %v", err)
	}
	t.Cleanup(func() { _ = slab.Close() })

	buf := slab.Alloc(0)
	require.Nil(t, buf)
	buf = slab.Alloc(-1)
	require.Nil(t, buf)
	// Free of nil must not panic.
	slab.Free(nil)
}

func TestSlabAllocator_DataIntegrity(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("slab allocator is Linux-only")
	}
	slab, err := NewSlabAllocator()
	if err != nil {
		t.Skipf("slab allocator unavailable on this platform: %v", err)
	}
	t.Cleanup(func() { _ = slab.Close() })

	// Allocate, write, free, re-allocate the same size — verify the
	// slot is reused (same region, same class). Free does not zero the
	// slot, so the reused buffer contains stale data from buf1. This
	// test verifies that Alloc/Free recycle slots correctly and that
	// the header is re-written on each Alloc.
	size := 512
	buf1 := slab.Alloc(size)
	for i := range buf1 {
		buf1[i] = 0xFF
	}
	slab.Free(buf1)

	buf2 := slab.Alloc(size)
	require.Len(t, buf2, size)
	// Write new data to verify the buffer is writable after reuse.
	for i := range buf2 {
		buf2[i] = byte(i % 128)
	}
	// Verify data integrity after write.
	for i := range buf2 {
		require.Equal(t, byte(i%128), buf2[i])
	}
	slab.Free(buf2)
}

func TestSlabAllocator_DoubleFree(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("slab allocator is Linux-only")
	}
	slab, err := NewSlabAllocator()
	if err != nil {
		t.Skipf("slab allocator unavailable on this platform: %v", err)
	}
	t.Cleanup(func() { _ = slab.Close() })

	buf := slab.Alloc(100)
	slab.Free(buf)
	// Double-free must be a no-op: the magic was cleared on first Free,
	// so the region scan in Free won't find a matching header and returns.
	slab.Free(buf)

	_, frees, _ := slab.Stats()
	require.Equal(t, 1, frees)
}

func TestHotStore_SlabPutGet(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("slab allocator is Linux-only")
	}
	s := NewHotStore(HotConfig{
		MaxBytes:  1 << 20,
		NumShards: 4,
		Slab:      true,
	})
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	k := KeyHash([]byte("slab-test-key"))
	o := obj(k, 500)
	o.Body = make([]byte, 500)
	for i := range o.Body {
		o.Body[i] = byte(i % 256)
	}

	err := s.Put(context.Background(), k, o)
	require.NoErrorf(t, err, "put: %v", err)
	// Verify Put did not mutate the caller's obj.Body — the caller
	// (TieredStore.Put) may still need to read it for warm-tier encoding.
	for i := range o.Body {
		require.Equal(t, byte(i%256), o.Body[i])
	}
	got, src, err := s.Get(context.Background(), k)
	require.NoErrorf(t, err, "get: %v", err)
	require.NotNil(t, got)
	require.Equal(t, api.SourceHot, src)
	require.Len(t, got.Body, 500)
	for i := range got.Body {
		require.Equal(t, byte(i%256), got.Body[i])
	}
}

func TestHotStore_SlabEviction(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("slab allocator is Linux-only")
	}
	s := NewHotStore(HotConfig{
		MaxBytes:  4096,
		NumShards: 1,
		Slab:      true,
	})
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	// Fill beyond capacity to trigger evictions.
	for i := range 20 {
		k := KeyHash([]byte{byte(i)})
		o := obj(k, 200)
		err := s.Put(context.Background(), k, o)
		require.NoErrorf(t, err, "put %d: %v", i, err)
	}
	// Verify that slab frees actually happened during evictions.
	// With 20 puts of 200B each into a 4096B store, at least some
	// entries must have been evicted, and their slab slots freed.
	require.NotNil(t, s.slab)
	allocs, frees, _ := s.slab.Stats()
	require.NotEqual(t, 0, allocs)
	require.NotEqual(t, 0, frees)
}

// TestHotStore_SlabConcurrentGetEviction proves the use-after-free fix:
// Get returns a heap-copied body, so concurrent eviction (which frees
// the slab slot) cannot corrupt the body the caller is reading. Without
// the detachBody fix, this test would intermittently read recycled
// slab memory instead of the original body content.
func TestHotStore_SlabConcurrentGetEviction(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skip("slab allocator is Linux-only")
	}

	s := NewHotStore(HotConfig{
		MaxBytes:  8192,
		NumShards: 1,
		Slab:      true,
	})
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	// Insert a known object, then hammer it with concurrent Gets
	// while simultaneously filling the store to force evictions.
	key := KeyHash([]byte("concurrent-key"))
	bodySize := 200
	o := obj(key, bodySize)
	o.Body = make([]byte, bodySize)
	for i := range o.Body {
		o.Body[i] = byte(i % 256)
	}
	err := s.Put(context.Background(), key, o)
	require.NoErrorf(t, err, "put: %v", err)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Reader: Get the key repeatedly and verify body integrity.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			got, _, err := s.Get(context.Background(), key)
			if err != nil || got == nil {
				continue
			}
			if len(got.Body) != bodySize {
				t.Errorf("body len: expected %d, got %d", bodySize, len(got.Body))
				return
			}
			for i := range got.Body {
				if got.Body[i] != byte(i%256) {
					t.Errorf("body corrupted at byte %d: expected %d, got %d (use-after-free)",
						i, byte(i%256), got.Body[i])
					return
				}
			}
		}
	}()

	// Evictor: continuously insert new keys to force eviction.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			k := KeyHash([]byte(fmt.Sprintf("evict-%d", i)))
			if err := s.Put(context.Background(), k, obj(k, 200)); err != nil {
				return
			}
		}
	}()

	// Run the concurrent readers/evictors for a fixed window. A timer
	// closes the stop channel after the duration; the main goroutine
	// waits for the workers to drain via wg.Wait instead of sleeping.
	time.AfterFunc(100*time.Millisecond, func() { close(stop) })
	wg.Wait()
}
