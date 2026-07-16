//go:build !linux

package storage

// SlabAllocator is a no-op on non-Linux platforms. Alloc falls back to
// Go heap allocation; Free is a no-op (GC handles it).
type SlabAllocator struct{}

// NewSlabAllocator creates a no-op slab allocator on non-Linux platforms.
func NewSlabAllocator() (*SlabAllocator, error) {
	return &SlabAllocator{}, nil
}

// Alloc allocates a []byte of the given size from the Go heap.
func (s *SlabAllocator) Alloc(size int) []byte {
	if size <= 0 {
		return nil
	}
	return make([]byte, size)
}

// Free is a no-op on non-Linux platforms.
func (s *SlabAllocator) Free(_ []byte) {}

// Stats returns zero stats on non-Linux platforms.
func (s *SlabAllocator) Stats() (allocs, frees, fallback int64) {
	return 0, 0, 0
}
