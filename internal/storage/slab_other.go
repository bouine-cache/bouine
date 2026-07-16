//go:build !linux

package storage

import "sync/atomic"

// SlabAllocator is a no-op on non-Linux platforms. Alloc returns nil
// so the caller keeps the original body without a copy. Free is a
// no-op (GC handles it).
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

// Alloc returns nil on non-Linux platforms so the caller falls back
// to the original body without an unnecessary copy. The slab is a
// no-op here; pretending to allocate would force a pointless make +
// copy on every Put for zero GC benefit.
func (s *SlabAllocator) Alloc(_ int) []byte {
	return nil
}

// Free is a no-op on non-Linux platforms.
func (s *SlabAllocator) Free(_ []byte) {}

// Stats returns zero stats on non-Linux platforms.
func (s *SlabAllocator) Stats() (allocs, frees, fallback int64) {
	return s.allocs.Load(), s.frees.Load(), s.fallback.Load()
}
