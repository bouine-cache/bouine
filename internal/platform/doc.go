// Package platform provides Linux-specific optimizations with portable
// fallbacks for non-Linux platforms (macOS, BSD). All functions are
// no-ops or use portable alternatives on non-Linux systems.
package platform

// This file contains the shared interface — platform-specific
// implementations live in platform_linux.go and platform_other.go.
