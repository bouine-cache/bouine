//go:build linux

package wal

import "syscall"

// syncFlag is O_DSYNC on Linux: every Write() flushes file data to disk
// without forcing a metadata flush. The kernel can coalesce adjacent
// data writes into a single flush, which is typically 2-3x faster than
// separate write() + fsync() calls.
const syncFlag = syscall.O_DSYNC
