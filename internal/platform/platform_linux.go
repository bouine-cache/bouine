//go:build linux

package platform

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// ReusePortSupported is true on Linux, false on other platforms.
// Used by config validation to reject ReusePort=true on non-Linux.
const ReusePortSupported = true

var (
	cgroupRoot         = "/sys/fs/cgroup"
	procSelfCgroupPath = "/proc/self/cgroup"
)

// CoarseNow returns a wall-clock timestamp with ~1ms resolution using
// CLOCK_REALTIME_COARSE. This is ~10-20x faster than time.Now() on the
// hot path (~2-4ns vs ~25-40ns via vDSO). The 1ms resolution is sufficient
// for cache Age headers (second granularity) and TTL evaluation.
//
// CLOCK_REALTIME_COARSE (not MONOTONIC_COARSE) is used because Age
// computation and StoredAt comparison require wall time, not monotonic
// time. MONOTONIC_COARSE returns seconds-since-boot which produces
// nonsensical Age values.
func CoarseNow() time.Time {
	var ts unix.Timespec
	_ = unix.ClockGettime(unix.CLOCK_REALTIME_COARSE, &ts)
	return time.Unix(ts.Sec, ts.Nsec)
}

// SetTCPFastOpen enables TCP Fast Open on a listener socket, allowing
// data in the SYN packet and saving 1 RTT for the first request.
func SetTCPFastOpen(fd int, backlog int) error {
	return unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_FASTOPEN, backlog)
}

// SetTCPDeferAccept tells the kernel to not wake the acceptor until
// data arrives, avoiding a wakeup for the bare SYN/ACK roundtrip.
func SetTCPDeferAccept(fd int, seconds int) error {
	return unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_DEFER_ACCEPT, seconds)
}

// SetReusePort enables SO_REUSEPORT so multiple listeners can bind
// the same port and the kernel distributes accepts across them.
func SetReusePort(fd int) error {
	return unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
}

// SetTCPQuickAck disables delayed ACK on a connection for immediate
// acknowledgment of received packets.
func SetTCPQuickAck(fd int) error {
	return unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_QUICKACK, 1)
}

// MadviseSequential hints the kernel that the mmap'd region will be
// accessed sequentially, enabling aggressive read-ahead.
func MadviseSequential(data []byte) error {
	return unix.Madvise(data, unix.MADV_SEQUENTIAL)
}

// MmapPopulate flags for eager page table population.
const MmapPopulate = unix.MAP_POPULATE

// FadviseRandom hints the kernel that the file will be accessed
// randomly, disabling read-ahead.
func FadviseRandom(fd int, offset int64, length int64) error {
	return unix.Fadvise(fd, offset, length, unix.FADV_RANDOM)
}

// FadviseWillNeed hints the kernel that the file region will be
// accessed soon, triggering read-ahead.
func FadviseWillNeed(fd int, offset int64, length int64) error {
	return unix.Fadvise(fd, offset, length, unix.FADV_WILLNEED)
}

// EffectiveGOMAXPROCS reads the cgroup CPU quota and returns the
// effective GOMAXPROCS value. Returns runtime.NumCPU() when not
// in a cgroup or when the quota is unlimited.
func EffectiveGOMAXPROCS() int {
	if n, ok := cgroupV2CPUs(); ok {
		return n
	}
	if n, ok := cgroupV1CPUs(); ok {
		return n
	}
	return runtime.NumCPU()
}

func cgroupV2CPUs() (int, bool) {
	path := filepath.Join(cgroupRoot, cgroupPath(""), "cpu.max")
	return readCPUQuota(path)
}

func cgroupV1CPUs() (int, bool) {
	path := cgroupPath("cpu")
	if path == "" {
		path = cgroupPath("cpu,cpuacct")
	}
	if path == "" {
		return 0, false
	}
	quota, err := readCgroupInt(filepath.Join(cgroupRoot, path, "cpu.cfs_quota_us"))
	if err != nil || quota <= 0 {
		return 0, false
	}
	period, err := readCgroupInt(filepath.Join(cgroupRoot, path, "cpu.cfs_period_us"))
	if err != nil || period <= 0 {
		return 0, false
	}
	return computeCPUs(quota, period), true
}

func cgroupPath(controller string) string {
	data, err := os.ReadFile(procSelfCgroupPath) //nolint:gosec // package var points to /proc/self/cgroup in production
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			continue
		}
		if controller == "" && parts[1] == "" {
			return strings.TrimPrefix(parts[2], "/")
		}
		for c := range strings.SplitSeq(parts[1], ",") {
			if c == controller {
				return strings.TrimPrefix(parts[2], "/")
			}
		}
	}
	return ""
}

func readCPUQuota(path string) (int, bool) {
	data, err := os.ReadFile(path) //nolint:gosec // path derived from cgroup root and /proc/self/cgroup
	if err != nil {
		return 0, false
	}
	parts := strings.Fields(strings.TrimSpace(string(data)))
	if len(parts) != 2 || parts[0] == "max" {
		return 0, false
	}
	quota, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || quota <= 0 {
		return 0, false
	}
	period, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || period <= 0 {
		return 0, false
	}
	return computeCPUs(quota, period), true
}

func computeCPUs(quota, period int64) int {
	n := int(quota / period)
	if quota%period != 0 {
		n++
	}
	if n < 1 {
		n = 1
	}
	return n
}

func readCgroupInt(path string) (int64, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path derived from cgroup root and /proc/self/cgroup
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
}

// Pwritev writes from multiple buffers to the file at the given offset
// in a single syscall (scatter-gather write).
func Pwritev(fd int, buffers [][]byte, offset int64) (int, error) {
	if len(buffers) == 0 {
		return 0, nil
	}
	return unix.Pwritev(fd, buffers, offset)
}
