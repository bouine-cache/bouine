//go:build !linux

package wal

import "os"

// syncFlag is O_SYNC on non-Linux platforms (macOS, BSD). O_SYNC flushes
// data + metadata on every Write(). O_DSYNC (data-only) is not available
// on macOS, so we accept the metadata flush overhead for portability.
const syncFlag = os.O_SYNC
