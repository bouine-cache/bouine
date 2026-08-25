//go:build linux

package warm

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

// MmapStats returns the total resident bytes from all mmap'd warm-tier
// segments and the net increase in resident pages since the last call.
// The delta is computed as the increase in total resident pages between
// consecutive calls — this measures net new residency, NOT page faults.
// It is only non-zero when the working set is growing; steady-state
// eviction+refault cycles will not be visible. Returns zeros when no
// segments are mmap'd.
//
// The caller should poll this periodically (e.g., every 15s) and update
// the bouine_warm_mmap_resident_bytes gauge and
// bouine_warm_mmap_resident_page_delta_total counter. Safe to call
// concurrently with Get (holds s.mu.RLock).
func (s *Store) MmapStats() (residentBytes int64, residentPageDelta int64) {
	pageSize := int64(unix.Getpagesize())
	s.mu.RLock()
	defer s.mu.RUnlock()
	var totalResidentPages int64
	for _, seg := range s.segs {
		ref := seg.mmap.Load()
		if ref == nil || len(ref.data) == 0 {
			continue
		}
		numPages := (len(ref.data) + int(pageSize) - 1) / int(pageSize)
		vec := make([]byte, numPages)
		//nolint:gosec // mincore requires a pointer to the mmap'd region; ref.data is a valid mmap slice
		_, _, errno := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&ref.data[0])), uintptr(len(ref.data)), uintptr(unsafe.Pointer(&vec[0])))
		if errno != 0 {
			continue
		}
		for _, b := range vec {
			if b&0x01 != 0 {
				totalResidentPages++
			}
		}
	}
	residentBytes = totalResidentPages * pageSize
	prev := s.mmapPrevResidentPages.Swap(totalResidentPages)
	if totalResidentPages > prev {
		residentPageDelta = totalResidentPages - prev
	}
	return residentBytes, residentPageDelta
}
