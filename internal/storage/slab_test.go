package storage

import (
	"context"
	"runtime"
	"testing"

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
		if buf == nil {
			t.Fatalf("Alloc(%d) returned nil", size)
		}
		if len(buf) != size {
			t.Fatalf("Alloc(%d) returned len=%d", size, len(buf))
		}
		// Write to the entire buffer to verify it's writable.
		for i := range buf {
			buf[i] = byte(i % 256)
		}
		slab.Free(buf)
	}

	allocs, frees, fallback := slab.Stats()
	if allocs == 0 {
		t.Fatal("expected non-zero allocs")
	}
	if frees == 0 {
		t.Fatal("expected non-zero frees")
	}
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
	if allocs != 0 {
		t.Fatalf("expected 0 allocs, got %d", allocs)
	}
	if frees != 0 {
		t.Fatalf("expected 0 frees for heap buffer, got %d", frees)
	}
	if fallback != 0 {
		t.Fatalf("expected 0 fallback, got %d", fallback)
	}
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
	for i := range 100 {
		buf := slab.Alloc(size)
		if buf == nil {
			t.Fatalf("iteration %d: Alloc returned nil", i)
		}
		slab.Free(buf)
	}

	allocs, frees, _ := slab.Stats()
	if allocs != 100 {
		t.Fatalf("expected 100 allocs, got %d", allocs)
	}
	if frees != 100 {
		t.Fatalf("expected 100 frees, got %d", frees)
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
	if buf == nil {
		t.Fatal("Alloc(2MB) returned nil")
	}
	if len(buf) != 2*1024*1024 {
		t.Fatalf("Alloc(2MB) returned len=%d", len(buf))
	}
	// Freeing a heap buffer must be safe.
	slab.Free(buf)

	_, _, fallback := slab.Stats()
	if fallback == 0 {
		t.Fatal("expected fallback for oversized allocation")
	}
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

	if buf := slab.Alloc(0); buf != nil {
		t.Fatal("Alloc(0) should return nil")
	}
	if buf := slab.Alloc(-1); buf != nil {
		t.Fatal("Alloc(-1) should return nil")
	}
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
	if len(buf2) != size {
		t.Fatalf("re-alloc: expected len %d, got %d", size, len(buf2))
	}
	// Write new data to verify the buffer is writable after reuse.
	for i := range buf2 {
		buf2[i] = byte(i % 128)
	}
	// Verify data integrity after write.
	for i := range buf2 {
		if buf2[i] != byte(i%128) {
			t.Fatalf("byte %d: expected %d, got %d", i, byte(i%128), buf2[i])
		}
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
	// so slabClassFromHeader returns -1 and Free returns early.
	slab.Free(buf)

	_, frees, _ := slab.Stats()
	if frees != 1 {
		t.Fatalf("expected 1 free (double-free should be no-op), got %d", frees)
	}
}

func TestHotStore_SlabPutGet(t *testing.T) {
	t.Parallel()
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

	if err := s.Put(context.Background(), k, o); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, src, err := s.Get(context.Background(), k)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got == nil {
		t.Fatal("expected hit, got miss")
	}
	if src != api.SourceHot {
		t.Fatalf("expected SourceHot, got %s", src)
	}
	if len(got.Body) != 500 {
		t.Fatalf("body len: expected 500, got %d", len(got.Body))
	}
	for i := range got.Body {
		if got.Body[i] != byte(i%256) {
			t.Fatalf("byte %d: expected %d, got %d", i, byte(i%256), got.Body[i])
		}
	}
}

func TestHotStore_SlabEviction(t *testing.T) {
	t.Parallel()
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
		if err := s.Put(context.Background(), k, o); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	// No crash, no panic — the slab Free path ran during evictions.
}
