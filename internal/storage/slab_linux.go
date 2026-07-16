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

const numSlabClasses = 7

const slabSlotsPerRegion = 1024

// slabMaxRegionsPerClass caps the number of mmap'd regions per size
// class. Each region holds 1024 slots, so the max capacity per class
// is slabMaxRegionsPerClass * 1024 slots. At 64 regions, the smallest
// class (256B) can hold ~67K entries and the largest (1MB) ~67M bytes.
// When the cap is hit, Alloc falls back to the Go heap — the body is
// still stored correctly, just not GC-optimized.
const slabMaxRegionsPerClass = 64

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
}

// slabClassState holds all regions for one size class. Regions grow
// on demand when the free list is empty, up to slabMaxRegionsPerClass.
type slabClassState struct {
	mu      sync.Mutex
	regions []*slabRegion
}

// SlabAllocator manages mmap'd regions for hot store body allocation.
// Bodies allocated from the slab are not scanned by the Go GC, reducing
// GC pressure. Falls back to Go heap allocation if mmap fails, the slab
// is full (all region caps reached), or the size exceeds the largest
// class.
type SlabAllocator struct {
	classes  [numSlabClasses]slabClassState
	allocs   atomic.Int64
	frees    atomic.Int64
	fallback atomic.Int64
}

// NewSlabAllocator creates a slab allocator. Each size class starts
// with one mmap'd region and grows on demand up to
// slabMaxRegionsPerClass. Returns an error if every initial mmap
// fails (caller should fall back to Go heap).
func NewSlabAllocator() (*SlabAllocator, error) {
	s := &SlabAllocator{}
	anyOK := false
	for i, slotSize := range slabSizeClasses {
		region, err := newSlabRegion(slotSize)
		if err != nil {
			continue // fall back to Go heap for this class
		}
		anyOK = true
		s.classes[i].regions = []*slabRegion{region}
	}
	if !anyOK {
		return nil, errors.New("slab allocator: all mmap calls failed")
	}
	return s, nil
}

// newSlabRegion allocates a single mmap'd region for the given slot size.
func newSlabRegion(slotSize int64) (*slabRegion, error) {
	regionSize := slotSize * slabSlotsPerRegion
	data, err := unix.Mmap(-1, 0, int(regionSize),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_ANONYMOUS|unix.MAP_PRIVATE) //nolint:gosec // anonymous mmap, no fd
	if err != nil {
		return nil, err
	}
	slots := int(regionSize / slotSize)
	freeList := make([]int64, 0, slots)
	for j := int64(0); j < int64(slots); j++ {
		freeList = append(freeList, j*slotSize)
	}
	return &slabRegion{
		data:     data,
		slotSize: slotSize,
		freeList: freeList,
	}, nil
}

// Close munmaps all slab regions. After Close, the allocator must not
// be used.
func (s *SlabAllocator) Close() error {
	for i := range s.classes {
		cs := &s.classes[i]
		cs.mu.Lock()
		for _, r := range cs.regions {
			_ = unix.Munmap(r.data) //nolint:gosec // best-effort cleanup
		}
		cs.regions = nil
		cs.mu.Unlock()
	}
	return nil
}

// Alloc returns a []byte of the given size from the slab, or falls back
// to Go heap allocation if no slab slot is available. On success the
// returned slice has len == size; the cap is the remaining usable space
// in the slot (slotSize - slabHeaderSize). On fallback, cap == size.
func (s *SlabAllocator) Alloc(size int) []byte {
	if size <= 0 {
		return nil
	}
	class := slabClass(size)
	if class < 0 {
		s.fallback.Add(1)
		return make([]byte, size)
	}
	cs := &s.classes[class]
	cs.mu.Lock()
	// Try to find a region with a free slot.
	for _, r := range cs.regions {
		if len(r.freeList) > 0 {
			offset := r.freeList[len(r.freeList)-1]
			r.freeList = r.freeList[:len(r.freeList)-1]
			// Write the header under the lock so Free's header
			// clear can't race with this write on the same slot.
			header := (*slabHeader)(unsafe.Pointer(&r.data[offset]))
			header.magic = slabMagic
			header.class = int64(class)
			cs.mu.Unlock()
			s.allocs.Add(1)
			hdrSize := int64(slabHeaderSize)
			start := offset + hdrSize
			end := offset + int64(size) + hdrSize
			return r.data[start : end : offset+r.slotSize]
		}
	}
	// All existing regions are full — try to grow.
	if len(cs.regions) < slabMaxRegionsPerClass {
		r, err := newSlabRegion(slabSizeClasses[class])
		if err == nil {
			cs.regions = append(cs.regions, r)
			offset := r.freeList[len(r.freeList)-1]
			r.freeList = r.freeList[:len(r.freeList)-1]
			header := (*slabHeader)(unsafe.Pointer(&r.data[offset]))
			header.magic = slabMagic
			header.class = int64(class)
			cs.mu.Unlock()
			s.allocs.Add(1)
			hdrSize := int64(slabHeaderSize)
			start := offset + hdrSize
			end := offset + int64(size) + hdrSize
			return r.data[start : end : offset+r.slotSize]
		}
	}
	cs.mu.Unlock()
	s.fallback.Add(1)
	return make([]byte, size)
}

// Free returns a slab-allocated []byte to the free list. If the buffer
// was not allocated from the slab (Go heap fallback), Free is a no-op.
func (s *SlabAllocator) Free(buf []byte) {
	if buf == nil || cap(buf) == 0 {
		return
	}
	// Keep the unsafe.Pointer around for safe pointer arithmetic via
	// unsafe.Add. The uintptr (bufStart) is only used for range
	// comparisons — never converted back to unsafe.Pointer.
	bufPtr := unsafe.Pointer(unsafe.SliceData(buf))
	bufStart := uintptr(bufPtr)
	for i := range s.classes {
		cs := &s.classes[i]
		cs.mu.Lock()
		for _, r := range cs.regions {
			rs := uintptr(unsafe.Pointer(&r.data[0]))
			re := rs + uintptr(len(r.data))
			if bufStart >= rs+uintptr(slabHeaderSize) && bufStart < re {
				// Lock is already held (cs.mu). Re-validate:
				// the slot could have been recycled by Alloc
				// between the unlocked scan and here.
				// unsafe.Add avoids the uintptr→Pointer
				// round-trip that violates unsafe.Pointer rule 4.
				h := (*slabHeader)(unsafe.Add(bufPtr, -slabHeaderSize))
				if h.magic != slabMagic {
					cs.mu.Unlock()
					return // not a slab buffer or already freed
				}
				class := int(h.class)
				if class < 0 || class >= numSlabClasses || class != i {
					cs.mu.Unlock()
					return // header corrupted or mismatched
				}
				// Recover the slot offset and clear the header.
				slotStart := bufStart - rs - uintptr(slabHeaderSize)
				slotOffset := (slotStart / uintptr(r.slotSize)) * uintptr(r.slotSize)
				h.magic = 0
				h.class = -1
				r.freeList = append(r.freeList, int64(slotOffset))
				cs.mu.Unlock()
				s.frees.Add(1)
				return
			}
		}
		cs.mu.Unlock()
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
