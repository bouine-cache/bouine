//go:build !linux

package storage

import "sync/atomic"

// SlabAllocator is a no-op on non-Linux platforms. Alloc falls back to
// Go heap allocation; Free is a no-op (GC handles it).
type SlabAllocator struct {
	allocs   atomic.Int64
	frees    atomic.Int64
	fallback atomic.Int64
}

// NewSlabAllocator creates a no-op slab allocator on non-Linux platforms.
func NewSlabAllocator() (*SlabAllocator, error) {
	return &SlabAllocator{}, nil
}

// Close is a no-op on non-Linux platforms.
func (s *SlabAllocator) Close() error { return nil }

// Alloc allocates a []byte of the given size from the Go heap.
func (s *SlabAllocator) Alloc(size int) []byte {
	if size <= 0 {
		return nil
	}
	s.fallback.Add(1)
	return make([]byte, size)
}

// Free is a no-op on non-Linux platforms.
func (s *SlabAllocator) Free(_ []byte) {}

// Stats returns zero stats on non-Linux platforms.
func (s *SlabAllocator) Stats() (allocs, frees, fallback int64) {
	return s.allocs.Load(), s.frees.Load(), s.fallback.Load()
}
