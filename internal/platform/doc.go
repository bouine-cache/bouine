// Package platform provides Linux-specific optimizations with portable
// fallbacks for non-Linux platforms (macOS, BSD). All functions are
// no-ops or use portable alternatives on non-Linux systems.
package platform

import "errors"

// This file contains the shared interface — platform-specific
// implementations live in platform_linux.go and platform_other.go.

// ErrPwritevUnsupported is returned by Pwritev on platforms without a
// kernel scatter-gather write syscall (macOS, BSD, Windows). Callers
// detect this sentinel and fall back to sequential WriteAt calls.
// Returning a typed error instead of (0, nil) makes the contract
// explicit: a zero-length write with no error would otherwise read as
// "0 bytes written successfully," masking the fallback.
var ErrPwritevUnsupported = errors.New("pwritev not supported on this platform")
