//go:build !linux

package platform

import (
	"runtime"
	"time"
)

// CoarseNow falls back to time.Now() on non-Linux platforms.
// CLOCK_MONOTONIC_COARSE is Linux-specific.
func CoarseNow() time.Time { return time.Now() }

// SetTCPFastOpen is a no-op on non-Linux platforms.
func SetTCPFastOpen(fd int, backlog int) error { return nil }

// SetTCPDeferAccept is a no-op on non-Linux platforms.
func SetTCPDeferAccept(fd int, seconds int) error { return nil }

// SetReusePort is a no-op on non-Linux platforms.
// SO_REUSEPORT exists on macOS/BSD but with different semantics.
func SetReusePort(fd int) error { return nil }

// SetTCPQuickAck is a no-op on non-Linux platforms.
func SetTCPQuickAck(fd int) error { return nil }

// MadviseSequential is a no-op on non-Linux platforms.
func MadviseSequential(data []byte) error { return nil }

// MmapPopulate is 0 on non-Linux (no MAP_POPULATE equivalent).
const MmapPopulate = 0

// FadviseRandom is a no-op on non-Linux platforms.
func FadviseRandom(fd int, offset int64, length int64) error { return nil }

// FadviseWillNeed is a no-op on non-Linux platforms.
func FadviseWillNeed(fd int, offset int64, length int64) error { return nil }

// EffectiveGOMAXPROCS returns runtime.NumCPU() on non-Linux platforms.
func EffectiveGOMAXPROCS() int { return runtime.NumCPU() }

// Pwritev falls back to sequential writes on non-Linux platforms.
// Callers should use it but expect it to be unused (they fall back to Write).
func Pwritev(fd int, buffers [][]byte, offset int64) (int, error) { return 0, nil }
