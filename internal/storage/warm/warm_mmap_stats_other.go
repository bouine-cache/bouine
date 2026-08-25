//go:build !linux

package warm

// MmapStats returns zeros on non-Linux platforms where mmap is not used
// for warm-tier segment reads. See warm_mmap_stats_linux.go for the
// Linux implementation.
func (s *Store) MmapStats() (residentBytes int64, newPageFaults int64) {
	return 0, 0
}
