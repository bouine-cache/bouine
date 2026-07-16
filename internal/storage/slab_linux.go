//go:build linux

package storage

import (
	"math/bits"
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

// slabHeader stores the magic, class index, and region index in the
// first 16 bytes of each slab slot. Free reads this to identify
// slab-allocated buffers and jump directly to the owning region in O(1),
// without scanning all regions in the class. The magic prevents false
// positives from Go-heap buffers, whose "header" bytes are random heap
// memory.
type slabHeader struct {
	magic  uint64
	class  int32
	region int32
}

const slabHeaderSize = int(unsafe.Sizeof(slabHeader{}))

// slabRegion is a single mmap'd region for a size class.
type slabRegion struct {
	data     []byte
	slotSize int64
	// bump is the next unallocated slot offset. When bump >= len(data),
	// the region is fully allocated and only recycled slots from
	// freeList are available. This avoids pre-building a 1024-entry
	// free list at mmap time — the first 1024 allocs just bump the
	// pointer, which is a single int64 increment.
	bump     int64
	freeList []int64 // offsets of freed slots (populated on Free)
}

// slabClassState holds all regions for one size class. Regions grow
// on demand when the free list is empty, up to slabMaxRegionsPerClass.
// allocHint is the index of the last region that had a free slot,
// avoiding an O(N) scan of all regions on every Alloc.
type slabClassState struct {
	mu        sync.Mutex
	regions   []*slabRegion
	allocHint int
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

// NewSlabAllocator creates a slab allocator. Size classes are lazily
// initialized: the first Alloc for a class mmaps its first region on
// demand. This avoids committing ~1.3 GB of virtual address space at
// startup when only a few classes are actually used (typical: API
// responses fit in classes 0–2). Returns an error only if the kernel
// refuses every mmap attempt on first use (caller falls back to heap).
func NewSlabAllocator() (*SlabAllocator, error) {
	return &SlabAllocator{}, nil
}

// newSlabRegion allocates a single mmap'd region for the given slot size.
// MAP_POPULATE pre-faults all pages so the first Alloc/write doesn't
// trigger a per-page fault — critical under high traffic where the
// slab grows frequently.
func newSlabRegion(slotSize int64) (*slabRegion, error) {
	regionSize := slotSize * slabSlotsPerRegion
	data, err := unix.Mmap(-1, 0, int(regionSize),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_ANONYMOUS|unix.MAP_PRIVATE|unix.MAP_POPULATE) //nolint:gosec // anonymous mmap, no fd
	if err != nil {
		return nil, err
	}
	slots := int(regionSize / slotSize)
	return &slabRegion{
		data:     data,
		slotSize: slotSize,
		freeList: make([]int64, 0, slots), // pre-allocated but empty; filled on Free
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
	// Try to find a region with a free slot, starting from the hint
	// (the last region that had a free slot). This avoids scanning
	// full regions on every Alloc.
	for j := range cs.regions {
		idx := (cs.allocHint + j) % len(cs.regions)
		r := cs.regions[idx]
		// Check if this region can satisfy the allocation: either
		// the bump pointer has room for this size, or the free list
		// has recycled slots (which are always slotSize, so always fit).
		bumpFits := r.bump+int64(slabHeaderSize)+int64(size) <= int64(len(r.data))
		if bumpFits || len(r.freeList) > 0 {
			buf := s.allocFromRegion(cs, r, idx, class, size)
			cs.mu.Unlock()
			s.allocs.Add(1)
			return buf
		}
	}
	// All existing regions are full — try to grow.
	if len(cs.regions) < slabMaxRegionsPerClass {
		r, err := newSlabRegion(slabSizeClasses[class])
		if err == nil {
			cs.regions = append(cs.regions, r)
			idx := len(cs.regions) - 1
			cs.allocHint = idx
			buf := s.allocFromRegion(cs, r, idx, class, size)
			cs.mu.Unlock()
			s.allocs.Add(1)
			return buf
		}
	}
	cs.mu.Unlock()
	s.fallback.Add(1)
	return make([]byte, size)
}

// allocFromRegion pops a free slot from r, writes the slab header, and
// returns a slice into the slot's body area. MUST be called while holding
// cs.mu. Tries the bump pointer first (fast: single int64 increment),
// then falls back to the free list (recycled slots from Free calls).
func (s *SlabAllocator) allocFromRegion(
	cs *slabClassState,
	r *slabRegion,
	regionIdx, class, size int,
) []byte {
	var offset int64
	if r.bump+int64(slabHeaderSize)+int64(size) <= int64(len(r.data)) {
		offset = r.bump
		r.bump += r.slotSize
	} else {
		// Bump exhausted — use a recycled slot from the free list.
		offset = r.freeList[len(r.freeList)-1]
		r.freeList = r.freeList[:len(r.freeList)-1]
	}
	header := (*slabHeader)(unsafe.Pointer(&r.data[offset]))
	header.magic = slabMagic
	header.class = int32(class)
	header.region = int32(regionIdx)
	cs.allocHint = regionIdx
	hdrSize := int64(slabHeaderSize)
	start := offset + hdrSize
	end := offset + int64(size) + hdrSize
	return r.data[start : end : offset+r.slotSize]
}

// Free returns a slab-allocated []byte to the free list. If the buffer
// was not allocated from the slab (Go heap fallback), Free is a no-op.
func (s *SlabAllocator) Free(buf []byte) {
	if buf == nil || cap(buf) == 0 {
		return
	}
	// Read the header first to determine the size class and region.
	// This avoids scanning all 7 class mutexes on every Free — we jump
	// directly to the right class and region. The header sits 16 bytes
	// before the buffer start. For Go-heap buffers, those 16 bytes are
	// random heap memory, so the magic check below rejects them before
	// touching any lock.
	bufPtr := unsafe.Pointer(unsafe.SliceData(buf))
	h := (*slabHeader)(unsafe.Add(bufPtr, -slabHeaderSize))
	if h.magic != slabMagic {
		return // not a slab buffer (Go heap fallback or already freed)
	}
	class := int(h.class)
	region := int(h.region)
	if class < 0 || class >= numSlabClasses {
		return // header corrupted
	}
	cs := &s.classes[class]
	cs.mu.Lock()
	if region < 0 || region >= len(cs.regions) {
		cs.mu.Unlock()
		return // stale or corrupted region index
	}
	r := cs.regions[region]
	bufStart := uintptr(bufPtr)
	rs := uintptr(unsafe.Pointer(&r.data[0]))
	re := rs + uintptr(len(r.data))
	// Validate that the buffer pointer falls within this region. This
	// guards against a false magic match from Go-heap memory.
	if bufStart < rs+uintptr(slabHeaderSize) || bufStart >= re {
		cs.mu.Unlock()
		return // false positive: pointer outside region
	}
	if h.magic != slabMagic {
		cs.mu.Unlock()
		return // re-validate under lock: slot may have been recycled
	}
	// Recover the slot offset and clear the header.
	slotStart := bufStart - rs - uintptr(slabHeaderSize)
	slotOffset := (slotStart / uintptr(r.slotSize)) * uintptr(r.slotSize)
	h.magic = 0
	h.class = -1
	h.region = -1
	r.freeList = append(r.freeList, int64(slotOffset))
	cs.mu.Unlock()
	s.frees.Add(1)
}

// slabClass returns the size class index for the given size, or -1 if
// the size exceeds the largest class. O(1) using bit length: size classes
// are 2^8, 2^10, 2^12, ..., 2^20, so class = (bits.Len(total-1)+1)/2 - 4,
// clamped to [0, numSlabClasses-1].
func slabClass(size int) int {
	total := uint64(int64(size) + int64(slabHeaderSize))
	if total > uint64(slabSizeClasses[numSlabClasses-1]) {
		return -1
	}
	// bits.Len64(total-1) gives the position of the highest set bit.
	// For total=256: bits.Len64(255)=8, class=(8+1)/2-4=0.
	// For total=257: bits.Len64(256)=9, class=(9+1)/2-4=1.
	// For total=1048576: bits.Len64(1048575)=20, class=(20+1)/2-4=6.
	// Small totals (total<256) give negative results; clamp to class 0.
	c := int(bits.Len64(total-1)+1)/2 - 4
	if c < 0 {
		return 0
	}
	return c
}

// Stats returns slab allocator statistics.
func (s *SlabAllocator) Stats() (allocs, frees, fallback int64) {
	return s.allocs.Load(), s.frees.Load(), s.fallback.Load()
}

// FreeBatch frees multiple slab-allocated buffers. Buffers from the
// same size class + region are freed under a single lock acquisition,
// reducing lock contention under high eviction rates. Non-slab buffers
// (Go heap fallback) are silently skipped.
func (s *SlabAllocator) FreeBatch(bodies [][]byte) {
	// Sort by (class, region) so we can coalesce lock acquisitions.
	// n is small (≤ inlineEvictCap + 1 = 5), so insertion sort is fine.
	type freeEntry struct {
		class  int
		region int
		buf    []byte
	}
	var entries []freeEntry
	for _, buf := range bodies {
		if buf == nil || cap(buf) == 0 {
			continue
		}
		bufPtr := unsafe.Pointer(unsafe.SliceData(buf))
		h := (*slabHeader)(unsafe.Add(bufPtr, -slabHeaderSize))
		if h.magic != slabMagic {
			continue
		}
		class := int(h.class)
		region := int(h.region)
		if class < 0 || class >= numSlabClasses {
			continue
		}
		entries = append(entries, freeEntry{class, region, buf})
	}
	// Sort by class, then region.
	for i := 1; i < len(entries); i++ {
		for j := i; j > 0 && (entries[j].class < entries[j-1].class ||
			(entries[j].class == entries[j-1].class && entries[j].region < entries[j-1].region)); j-- {
			entries[j], entries[j-1] = entries[j-1], entries[j]
		}
	}
	// Free entries, coalescing same (class, region) under one lock.
	for i := 0; i < len(entries); {
		e := entries[i]
		cs := &s.classes[e.class]
		cs.mu.Lock()
		for i < len(entries) && entries[i].class == e.class && entries[i].region == e.region {
			s.freeLocked(cs, e.region, entries[i].buf)
			i++
		}
		cs.mu.Unlock()
	}
}

// freeLocked returns a slab buffer to its region's free list. MUST be
// called while holding cs.mu. Performs the same validation as Free
// but skips the lock/unlock since the caller already holds it.
func (s *SlabAllocator) freeLocked(cs *slabClassState, regionIdx int, buf []byte) {
	if regionIdx < 0 || regionIdx >= len(cs.regions) {
		return
	}
	r := cs.regions[regionIdx]
	bufPtr := unsafe.Pointer(unsafe.SliceData(buf))
	bufStart := uintptr(bufPtr)
	rs := uintptr(unsafe.Pointer(&r.data[0]))
	re := rs + uintptr(len(r.data))
	if bufStart < rs+uintptr(slabHeaderSize) || bufStart >= re {
		return
	}
	h := (*slabHeader)(unsafe.Add(bufPtr, -slabHeaderSize))
	if h.magic != slabMagic {
		return
	}
	slotStart := bufStart - rs - uintptr(slabHeaderSize)
	slotOffset := (slotStart / uintptr(r.slotSize)) * uintptr(r.slotSize)
	h.magic = 0
	h.class = -1
	h.region = -1
	r.freeList = append(r.freeList, int64(slotOffset))
	s.frees.Add(1)
}
