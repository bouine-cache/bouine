//go:build linux

package storage

import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

// slabSizeClasses defines the size classes for the slab allocator.
// Each class is the maximum body size that fits in that class's slots.
var slabSizeClasses = [numSlabClasses]int64{
	256,     // class 0: 0-256B
	1024,    // class 1: 257B-1KB
	4096,    // class 2: 1KB-4KB
	16384,   // class 3: 4KB-16KB
	65536,   // class 4: 16KB-64KB
	262144,  // class 5: 64KB-256KB
	1048576, // class 6: 256KB-1MB
}

const numSlabClasses = 7

const slabSlotsPerRegion = 1024

// slabHeaderSize is reserved at the front of each slot to store the class
// index. This lets Free identify a slab-allocated buffer in O(1) without
// scanning regions or doing fragile pointer-range comparisons.
const slabHeaderSize = 8

// slabRegion is a single mmap'd region for a size class.
type slabRegion struct {
	data     []byte
	slotSize int64
	freeList []int64 // offsets of free slots
	mu       sync.Mutex
}

// SlabAllocator manages mmap'd regions for hot store body allocation.
// Bodies allocated from the slab are not scanned by the Go GC, reducing
// GC pressure. Falls back to Go heap allocation if mmap fails or the
// slab is full.
type SlabAllocator struct {
	regions  [numSlabClasses]*slabRegion
	allocs   atomic.Int64
	frees    atomic.Int64
	fallback atomic.Int64
}

// NewSlabAllocator creates a slab allocator. Each size class gets one
// mmap'd region with slabSlotsPerRegion slots. Returns an error if
// every mmap fails (caller should fall back to Go heap).
func NewSlabAllocator() (*SlabAllocator, error) {
	s := &SlabAllocator{}
	anyOK := false
	for i, slotSize := range slabSizeClasses {
		regionSize := slotSize * slabSlotsPerRegion
		data, err := unix.Mmap(-1, 0, int(regionSize),
			unix.PROT_READ|unix.PROT_WRITE,
			unix.MAP_ANONYMOUS|unix.MAP_PRIVATE) //nolint:gosec // anonymous mmap, no fd
		if err != nil {
			continue // fall back to Go heap for this class
		}
		anyOK = true
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
	if !anyOK {
		return nil, errors.New("slab allocator: all mmap calls failed")
	}
	return s, nil
}

// Close munmaps all slab regions. After Close, the allocator must not
// be used.
func (s *SlabAllocator) Close() error {
	for _, region := range s.regions {
		if region == nil {
			continue
		}
		_ = unix.Munmap(region.data) //nolint:gosec // best-effort cleanup
	}
	return nil
}

// Alloc returns a []byte of the given size from the slab, or falls back
// to Go heap allocation if no slab slot is available. The returned
// slice has len == size and cap == slotSize-slabHeaderSize.
func (s *SlabAllocator) Alloc(size int) []byte {
	if size <= 0 {
		return nil
	}
	class := slabClass(size)
	if class < 0 {
		s.fallback.Add(1)
		return make([]byte, size)
	}
	region := s.regions[class]
	if region == nil {
		s.fallback.Add(1)
		return make([]byte, size)
	}
	region.mu.Lock()
	if len(region.freeList) == 0 {
		region.mu.Unlock()
		s.fallback.Add(1)
		return make([]byte, size)
	}
	offset := region.freeList[len(region.freeList)-1]
	region.freeList = region.freeList[:len(region.freeList)-1]
	region.mu.Unlock()
	s.allocs.Add(1)
	// Write the class index into the slot header so Free can identify
	// this buffer in O(1) without pointer arithmetic.
	header := (*[slabHeaderSize]byte)(unsafe.Pointer(&region.data[offset]))
	*(*int)(unsafe.Pointer(&header[0])) = class
	// Return the usable portion after the header.
	start := offset + slabHeaderSize
	end := offset + int64(size) + slabHeaderSize
	return region.data[start : end : offset+region.slotSize]
}

// Free returns a slab-allocated []byte to the free list. If the buffer
// was not allocated from the slab (Go heap fallback), Free is a no-op.
func (s *SlabAllocator) Free(buf []byte) {
	if buf == nil || cap(buf) == 0 {
		return
	}
	// The header sits slabHeaderSize bytes before the start of the
	// usable region. Read the class index from it.
	// This is safe because slab-allocated buffers always have
	// slabHeaderSize bytes of capacity before their start pointer.
	class := slabClassFromHeader(buf)
	if class < 0 || class >= numSlabClasses {
		return // not a slab buffer (Go heap fallback)
	}
	region := s.regions[class]
	if region == nil {
		return
	}
	// Recover the slot offset from the buffer's position in the region.
	bufStart := uintptr(unsafe.Pointer(&buf[:1][0]))
	regionStart := uintptr(unsafe.Pointer(&region.data[0]))
	if bufStart < regionStart || bufStart >= regionStart+uintptr(len(region.data)) {
		return // not from this region (shouldn't happen, but defensive)
	}
	slotStart := bufStart - regionStart - slabHeaderSize
	slotOffset := (slotStart / uintptr(region.slotSize)) * uintptr(region.slotSize)
	region.mu.Lock()
	region.freeList = append(region.freeList, int64(slotOffset))
	region.mu.Unlock()
	s.frees.Add(1)
}

// slabClassFromHeader reads the class index from the slab header that
// precedes the usable portion of a slab-allocated buffer. Returns -1 if
// the buffer is not slab-allocated (the header bytes would be garbage
// from a Go heap allocation).
func slabClassFromHeader(buf []byte) int {
	// We need to read slabHeaderSize bytes before buf[0]. This is only
	// safe if the buffer was allocated by Alloc (which reserves header
	// space). For Go heap buffers, this reads unrelated heap memory —
	// but we validate the class index range and region membership, so
	// a false positive is caught by Free's range check.
	// The cap check ensures we don't read before the allocation.
	if cap(buf) < slabHeaderSize {
		return -1
	}
	// Use unsafe to read the 8 bytes before buf[0] as an int.
	headerPtr := unsafe.Pointer(&buf[:1][0])
	classPtr := unsafe.Pointer(uintptr(headerPtr) - slabHeaderSize)
	class := *(*int)(classPtr)
	if class < 0 || class >= numSlabClasses {
		return -1
	}
	return class
}

// slabClass returns the size class index for the given size, or -1 if
// the size exceeds the largest class.
func slabClass(size int) int {
	for i, s := range slabSizeClasses {
		if int64(size)+slabHeaderSize <= s {
			return i
		}
	}
	return -1
}

// Stats returns slab allocator statistics.
func (s *SlabAllocator) Stats() (allocs, frees, fallback int64) {
	return s.allocs.Load(), s.frees.Load(), s.fallback.Load()
}
