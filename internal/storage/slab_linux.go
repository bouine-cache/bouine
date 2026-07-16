//go:build linux

package storage

import (
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

// slabSizeClasses defines the size classes for the slab allocator.
var slabSizeClasses = []int64{
	256,     // class 0: 0-256B
	1024,    // class 1: 257B-1KB
	4096,    // class 2: 1KB-4KB
	16384,   // class 3: 4KB-16KB
	65536,   // class 4: 16KB-64KB
	262144,  // class 5: 64KB-256KB
	1048576, // class 6: 256KB-1MB
}

const slabSlotsPerRegion = 1024

// slabRegion is a single mmap'd region for a size class.
type slabRegion struct {
	data     []byte
	slotSize int64
	freeList []int64 // offsets of free slots
}

// SlabAllocator manages mmap'd regions for hot store body allocation.
// Bodies allocated from the slab are not scanned by the Go GC, reducing
// GC pressure. Falls back to Go heap allocation if mmap fails or the
// slab is full.
type SlabAllocator struct {
	mu       sync.Mutex
	regions  [len(slabSizeClasses)]*slabRegion
	allocs   int64
	frees    int64
	fallback int64
}

// NewSlabAllocator creates a slab allocator. Each size class gets one
// mmap'd region with slabSlotsPerRegion slots.
func NewSlabAllocator() (*SlabAllocator, error) {
	s := &SlabAllocator{}
	for i, slotSize := range slabSizeClasses {
		regionSize := slotSize * slabSlotsPerRegion
		data, err := unix.Mmap(-1, 0, int(regionSize),
			unix.PROT_READ|unix.PROT_WRITE,
			unix.MAP_ANONYMOUS|unix.MAP_PRIVATE) //nolint:gosec // anonymous mmap
		if err != nil {
			continue // fall back to Go heap for this class
		}
		slots := int(regionSize / slotSize)
		freeList := make([]int64, 0, slots)
		for j := int64(0); j < int64(slots); j++ {
			freeList = append(freeList, j*slotSize)
		}
		s.regions[i] = &slabRegion{
			data:     data,
			slotSize: slotSize,
			freeList: freeList,
		}
	}
	return s, nil
}

// slabBuf is a slab-allocated buffer with metadata for freeing.
type slabBuf struct {
	class int
	data  []byte
}

// Alloc returns a []byte of the given size from the slab, or falls back
// to Go heap allocation if no slab slot is available.
func (s *SlabAllocator) Alloc(size int) []byte {
	if size <= 0 {
		return nil
	}
	class := slabClass(size)
	if class < 0 || class >= len(slabSizeClasses) {
		s.mu.Lock()
		s.fallback++
		s.mu.Unlock()
		return make([]byte, size)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	region := s.regions[class]
	if region == nil || len(region.freeList) == 0 {
		s.fallback++
		return make([]byte, size)
	}
	offset := region.freeList[len(region.freeList)-1]
	region.freeList = region.freeList[:len(region.freeList)-1]
	s.allocs++
	// Return a slice with cap = slotSize so the caller can't accidentally
	// write past the slot boundary. The extra cap bytes are zeroed (mmap).
	return region.data[offset : offset+int64(size) : offset+region.slotSize]
}

// Free returns a slab-allocated []byte to the free list.
// If the buffer was not allocated from the slab, Free is a no-op.
func (s *SlabAllocator) Free(buf []byte) {
	if buf == nil || cap(buf) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, region := range s.regions {
		if region == nil {
			continue
		}
		// Check if the buffer's backing array is within this region.
		bufPtr := uintptr(unsafe.Pointer(&buf[:1][0]))
		regionStart := uintptr(unsafe.Pointer(&region.data[0]))
		regionEnd := regionStart + uintptr(len(region.data))
		if bufPtr >= regionStart && bufPtr < regionEnd {
			offset := int64(bufPtr - regionStart)
			// Align to slot boundary.
			slotOffset := (offset / region.slotSize) * region.slotSize
			region.freeList = append(region.freeList, slotOffset)
			s.frees++
			return
		}
	}
	// Not from any slab region — GC handles it.
}

// slabClass returns the size class index for the given size, or -1.
func slabClass(size int) int {
	for i, s := range slabSizeClasses {
		if int64(size) <= s {
			return i
		}
	}
	return -1
}

// Stats returns slab allocator statistics.
func (s *SlabAllocator) Stats() (allocs, frees, fallback int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.allocs, s.frees, s.fallback
}
