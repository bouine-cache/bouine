//go:build !linux

package warm

// tryMmap is a no-op on non-Linux platforms. seg.mmap stays nil and
// reads fall back to pread.
func (seg *Segment) tryMmap() {}

// munmap is a no-op on non-Linux platforms.
func (seg *Segment) munmap() {}

// munmapAll is a no-op on non-Linux platforms. Iterates segments
// and calls the no-op munmap to keep the code path consistent with
// Linux and avoid unused-method warnings.
func munmapAll(segs []*Segment) {
	for _, seg := range segs {
		seg.munmap()
	}
}

// readRecordAtMmap returns nil, nil on non-Linux platforms, signaling
// the caller to fall back to the pread path.
func readRecordAtMmap(seg *Segment, offset int64, size int64) (*Record, error) {
	return nil, nil
}

// ensureMmapOpened is a no-op on non-Linux platforms.
func (seg *Segment) ensureMmapOpened(isActive bool) {}
