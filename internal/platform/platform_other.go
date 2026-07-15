//go:build !linux

package platform

import (
	"errors"
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

// ReusePortSupported is true on Linux, false on other platforms.
const ReusePortSupported = false

// SetReusePort returns an error on non-Linux platforms.
// SO_REUSEPORT exists on macOS/BSD but with different semantics
// (all listeners receive all connections, not hash-based distribution).
// Returning an error lets the listener fall back to single-listener mode.
var errReusePortUnsupported = errors.New("SO_REUSEPORT not supported on this platform")

// SetReusePort returns errReusePortUnsupported on non-Linux platforms.
func SetReusePort(fd int) error { return errReusePortUnsupported }

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

// RaiseFileLimit is a no-op on non-Linux platforms. The container
// runtime or OS manages file descriptor limits outside the process.
func RaiseFileLimit(want uint64) (uint64, error) { return 0, nil }

// Pwritev falls back to sequential writes on non-Linux platforms.
// Callers should use it but expect it to be unused (they fall back to Write).
func Pwritev(fd int, buffers [][]byte, offset int64) (int, error) { return 0, nil }
