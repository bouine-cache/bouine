//go:build linux

package storage

import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

// slabMagic is written into the header of every slab-allocated buffer.
// It lets Free distinguish slab buffers from Go-heap buffers without
// reading garbage memory — a heap buffer's "header" bytes are random,
// so the probability of a false magic match is ~1/2^64.
const slabMagic uint64 = 0x534c4142534c4f54 // "SLABSLOT"

// slabSizeClasses defines the slot size for each class. The usable
// body capacity per class is slotSize - slabHeaderSize (16 bytes).
var slabSizeClasses = [numSlabClasses]int64{
	256,     // class 0: up to 240B
	1024,    // class 1: up to 1008B
	4096,    // class 2: up to 4080B
	16384,   // class 3: up to 16368B
	65536,   // class 4: up to 65520B
	262144,  // class 5: up to 262128B
	1048576, // class 6: up to 1048560B
}

const numSlabClasses = 7

const slabSlotsPerRegion = 1024

// slabHeader stores the magic and class index in the first 16 bytes of
// each slab slot. Free reads this to identify slab-allocated buffers in
// O(1) without scanning regions or doing fragile pointer-range
// comparisons. The magic prevents false positives from Go-heap buffers,
// whose "header" bytes are random heap memory.
type slabHeader struct {
	magic uint64
	class int64
}

const slabHeaderSize = int(unsafe.Sizeof(slabHeader{}))

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
	// Write the header under the lock so Free's header clear can't
	// race with this write on the same slot.
	header := (*slabHeader)(unsafe.Pointer(&region.data[offset]))
	header.magic = slabMagic
	header.class = int64(class)
	region.mu.Unlock()
	s.allocs.Add(1)
	// Return the usable portion after the header.
	hdrSize := int64(slabHeaderSize)
	start := offset + hdrSize
	end := offset + int64(size) + hdrSize
	return region.data[start : end : offset+region.slotSize]
}

// Free returns a slab-allocated []byte to the free list. If the buffer
// was not allocated from the slab (Go heap fallback), Free is a no-op.
func (s *SlabAllocator) Free(buf []byte) {
	if buf == nil || cap(buf) == 0 {
		return
	}
	// Validate that the buffer pointer falls within a known mmap region
	// before reading the header. This avoids reading garbage memory from
	// Go-heap buffers, which could segfault if the buffer is at a page
	// boundary.
	bufStart := uintptr(unsafe.Pointer(unsafe.SliceData(buf)))
	var regionStart uintptr
	for i, r := range s.regions {
		if r == nil {
			continue
		}
		rs := uintptr(unsafe.Pointer(&r.data[0]))
		re := rs + uintptr(len(r.data))
		if bufStart >= rs+uintptr(slabHeaderSize) && bufStart < re {
			regionStart = rs
			// Lock the region before reading the header so Alloc's
			// header write on the same region is synchronized.
			r.mu.Lock()
			// Re-validate under the lock: the slot could have been
			// recycled by Alloc between the unlocked scan and here.
			headerPtr := unsafe.Pointer(bufStart - uintptr(slabHeaderSize))
			h := (*slabHeader)(headerPtr)
			if h.magic != slabMagic {
				r.mu.Unlock()
				return // not a slab buffer or already freed
			}
			class := int(h.class)
			if class < 0 || class >= numSlabClasses || class != i {
				r.mu.Unlock()
				return // header corrupted or mismatched
			}
			// Recover the slot offset and clear the header.
			slotStart := bufStart - regionStart - uintptr(slabHeaderSize)
			slotOffset := (slotStart / uintptr(r.slotSize)) * uintptr(r.slotSize)
			hdr := (*slabHeader)(unsafe.Pointer(&r.data[slotOffset]))
			hdr.magic = 0
			hdr.class = -1
			r.freeList = append(r.freeList, int64(slotOffset))
			r.mu.Unlock()
			s.frees.Add(1)
			return
		}
	}
	// Not from any slab region — Go heap fallback, no-op.
}

// slabClass returns the size class index for the given size, or -1 if
// the size exceeds the largest class.
func slabClass(size int) int {
	for i, s := range slabSizeClasses {
		if int64(size)+int64(slabHeaderSize) <= s {
			return i
		}
	}
	return -1
}

// Stats returns slab allocator statistics.
func (s *SlabAllocator) Stats() (allocs, frees, fallback int64) {
	return s.allocs.Load(), s.frees.Load(), s.fallback.Load()
}
