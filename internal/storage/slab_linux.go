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
// body capacity per class is slotSize - slabHeaderSize (8 bytes).
var slabSizeClasses = [numSlabClasses]int64{
	256,     // class 0: up to 248B
	1024,    // class 1: up to 1016B
	4096,    // class 2: up to 4088B
	16384,   // class 3: up to 16376B
	65536,   // class 4: up to 65528B
	262144,  // class 5: up to 262136B
	1048576, // class 6: up to 1048568B
}

// slabHeader is the 8-byte magic prefix at the start of each slab
// slot. Free reads it forward from r.data[slotOffset] (not backward
// from the buffer pointer) to avoid checkptr panics under -race.
// The magic is the sole slab-buffer identification mechanism: a heap
// buffer's bytes at the computed offset won't match (false positive
// rate ~1/2^64). Class is derived from cap(buf) via classFromCap;
// the region is found by pointer-range scan.
type slabHeader struct {
	magic uint64
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
func newSlabRegion(slotSize int64) (*slabRegion, error) {
	regionSize := slotSize * slabSlotsPerRegion
	data, err := unix.Mmap(-1, 0, int(regionSize),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_ANONYMOUS|unix.MAP_PRIVATE) //nolint:gosec // anonymous mmap, no fd
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
// The 8-byte slab header overhead means a 248B body fits in class 0
// (256B slot), not 240B as with the old 16-byte header.
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
	hdrSize := int64(slabHeaderSize)
	need := hdrSize + int64(size)
	cs.mu.Lock()
	// Fast path: try the hinted region first. Under high traffic with
	// huge route cardinality, the working set churns rapidly but the
	// hinted region usually has bump space or recycled slots.
	if h := cs.allocHint; h < len(cs.regions) {
		r := cs.regions[h]
		if r.bump+need <= int64(len(r.data)) {
			// Bump pointer fast path: single int64 increment, no
			// function call, no free-list pop.
			offset := r.bump
			r.bump += r.slotSize
			hdr := (*slabHeader)(unsafe.Pointer(&r.data[offset])) //nolint:gosec // intentional on mmap'd memory
			hdr.magic = slabMagic
			start := offset + hdrSize
			cs.mu.Unlock()
			s.allocs.Add(1)
			return r.data[start : start+int64(size) : offset+r.slotSize]
		}
		if len(r.freeList) > 0 {
			buf := s.allocFromRegion(cs, r, h, size)
			cs.mu.Unlock()
			s.allocs.Add(1)
			return buf
		}
	}
	// Slow path: scan other regions for a free slot.
	for j := range cs.regions {
		idx := (cs.allocHint + j) % len(cs.regions)
		r := cs.regions[idx]
		if r.bump+need <= int64(len(r.data)) || len(r.freeList) > 0 {
			buf := s.allocFromRegion(cs, r, idx, size)
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
			buf := s.allocFromRegion(cs, r, idx, size)
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
//
//nolint:gosec // G103/G115: unsafe pointer arithmetic on mmap'd memory is intentional
func (s *SlabAllocator) allocFromRegion(
	cs *slabClassState,
	r *slabRegion,
	regionIdx, size int,
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
	cs.allocHint = regionIdx
	hdrSize := int64(slabHeaderSize)
	start := offset + hdrSize
	end := offset + int64(size) + hdrSize
	return r.data[start : end : offset+r.slotSize]
}

// classFromCap derives the slab class from the buffer's capacity.
// The cap of a slab-allocated buffer is slotSize - slabHeaderSize,
// so total = cap + slabHeaderSize = slotSize, which is a power of two
// in [2^8, 2^20]. O(1) via bits.Len64, same formula as slabClass.
// Returns -1 if the cap doesn't match any slab class (Go-heap fallback).
func classFromCap(c int) int {
	if c <= 0 {
		return -1
	}
	total := uint64(c + slabHeaderSize)
	if total < uint64(slabSizeClasses[0]) || total > uint64(slabSizeClasses[numSlabClasses-1]) { //nolint:gosec // G115: slabSizeClasses are positive powers of two
		return -1
	}
	// Slot sizes are powers of two; a non-power-of-two total is a heap buffer.
	if total&(total-1) != 0 {
		return -1
	}
	class := int(bits.Len64(total-1)+1)/2 - 4
	if class < 0 || class >= numSlabClasses || slabSizeClasses[class] != int64(total) { //nolint:gosec // G115: total is bounded to [256, 1048576]
		return -1
	}
	return class
}

// findSlotOffset computes the slot offset within a region given the
// buffer's data pointer and the region's data pointer. Returns -1 if
// the buffer is not within this region.
//
//nolint:gosec // G103/G115: pointer arithmetic on mmap'd memory is intentional
func findSlotOffset(r *slabRegion, bufPtr unsafe.Pointer) int64 {
	rs := uintptr(unsafe.Pointer(&r.data[0]))
	re := rs + uintptr(len(r.data))
	bp := uintptr(bufPtr)
	if bp < rs+uintptr(slabHeaderSize) || bp >= re {
		return -1
	}
	slotStart := bp - rs - uintptr(slabHeaderSize)
	// Align down to slot boundary. slotSize is a power of two, so
	// bit masking is equivalent to (slotStart / slotSize) * slotSize
	// but avoids the division.
	return int64(slotStart &^ uintptr(r.slotSize-1))
}

// freeSlot returns a slab-allocated buffer to its region's free list.
// MUST be called while holding cs.mu. slotOffset is the pre-computed
// offset of the slot within r.data. Validates the magic by reading
// the header from r.data (forward access, no checkptr issue).
//
//nolint:gosec // G103/G115: pointer arithmetic on mmap'd memory is intentional
func (s *SlabAllocator) freeSlot(r *slabRegion, slotOffset int64) {
	header := (*slabHeader)(unsafe.Pointer(&r.data[slotOffset]))
	if header.magic != slabMagic {
		return // slot was already freed or recycled
	}
	header.magic = 0
	r.freeList = append(r.freeList, slotOffset)
	s.frees.Add(1)
}

// Free returns a slab-allocated []byte to the free list. If the buffer
// was not allocated from the slab (Go heap fallback), Free is a no-op.
//
//nolint:gosec // G103/G115: pointer arithmetic on mmap'd memory is intentional
func (s *SlabAllocator) Free(buf []byte) {
	if buf == nil || cap(buf) == 0 {
		return
	}
	// Derive the class from the buffer's capacity. The cap of a
	// slab-allocated buffer is slotSize - slabHeaderSize, so we can
	// determine the class without backward pointer arithmetic (which
	// would trigger checkptr under -race on mmap'd memory).
	class := classFromCap(cap(buf))
	if class < 0 {
		return // not a slab buffer (Go heap fallback)
	}
	bufPtr := unsafe.Pointer(unsafe.SliceData(buf))
	cs := &s.classes[class]
	cs.mu.Lock()
	// Find the region containing this buffer. The scan is bounded by
	// slabMaxRegionsPerClass (64) and only runs on the eviction path,
	// not the hit path. Under high traffic with 40% hit ratio, the
	// 60% miss path triggers evictions, but FreeBatch coalesces most
	// frees so the single-Free path is rarely the bottleneck.
	for _, r := range cs.regions {
		slotOffset := findSlotOffset(r, bufPtr)
		if slotOffset < 0 {
			continue
		}
		s.freeSlot(r, slotOffset)
		cs.mu.Unlock()
		return
	}
	cs.mu.Unlock()
	// Buffer's cap matched a slab class but pointer is outside all
	// regions — extremely unlikely false positive. No-op.
}

// slabClass returns the size class index for the given size, or -1 if
// the size exceeds the largest class. O(1) using bit length: size classes
// are 2^8, 2^10, 2^12, ..., 2^20, so class = (bits.Len(total-1)+1)/2 - 4,
// clamped to [0, numSlabClasses-1].
func slabClass(size int) int {
	total := uint64(int64(size) + int64(slabHeaderSize)) //nolint:gosec // G115: size+16 fits in uint64
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
	entries := s.classifyBodies(bodies)
	sortFreeEntries(entries)
	s.freeGroupedEntries(entries)
}

// freeEntry pairs a buffer pointer with its class for grouped freeing.
type freeEntry struct {
	class  int
	bufPtr unsafe.Pointer
}

// classifyBodies identifies the slab class of each buffer from its cap
// (slotSize - slabHeaderSize). Does not acquire any locks — just collects
// the class index for each valid slab buffer. Pre-allocates the entries
// slice to avoid append growth on the hot eviction path.
//
//nolint:gosec // G103: pointer read on mmap'd memory is intentional
func (s *SlabAllocator) classifyBodies(bodies [][]byte) []freeEntry {
	entries := make([]freeEntry, 0, len(bodies))
	for _, buf := range bodies {
		if buf == nil || cap(buf) == 0 {
			continue
		}
		class := classFromCap(cap(buf))
		if class < 0 {
			continue
		}
		bufPtr := unsafe.Pointer(unsafe.SliceData(buf))
		entries = append(entries, freeEntry{class: class, bufPtr: bufPtr})
	}
	return entries
}

// sortFreeEntries sorts entries by class using insertion sort.
// n is small (≤ inlineEvictCap + 1 = 5), so insertion sort is efficient.
func sortFreeEntries(entries []freeEntry) {
	for i := 1; i < len(entries); i++ {
		for j := i; j > 0 && entries[j].class < entries[j-1].class; j-- {
			entries[j], entries[j-1] = entries[j-1], entries[j]
		}
	}
}

// freeGroupedEntries frees entries grouped by class. For each class,
// acquires the lock once and frees all entries in that class by
// finding their region and slot offset. A locality cache avoids
// re-scanning the same region for consecutive entries from the same
// region — common under high-traffic eviction bursts where multiple
// evicted entries share a hot region.
//
//nolint:gosec // G103: pointer arithmetic on mmap'd memory is intentional
func (s *SlabAllocator) freeGroupedEntries(entries []freeEntry) {
	i := 0
	for i < len(entries) {
		class := entries[i].class
		cs := &s.classes[class]
		cs.mu.Lock()
		lastRegion := -1
		for i < len(entries) && entries[i].class == class {
			ptr := entries[i].bufPtr
			// Try the last matched region first (locality cache).
			if lastRegion >= 0 {
				if off := findSlotOffset(cs.regions[lastRegion], ptr); off >= 0 {
					s.freeSlot(cs.regions[lastRegion], off)
					i++
					continue
				}
			}
			// Scan all regions, skipping the cached one (already checked).
			for j, r := range cs.regions {
				if j == lastRegion {
					continue
				}
				if off := findSlotOffset(r, ptr); off >= 0 {
					s.freeSlot(r, off)
					lastRegion = j
					break
				}
			}
			i++
		}
		cs.mu.Unlock()
	}
}
