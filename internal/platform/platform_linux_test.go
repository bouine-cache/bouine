//go:build linux

package platform

import (
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestEffectiveGOMAXPROCS_CgroupV2NestedPath(t *testing.T) {
	root, proc := setupCgroupTest(t, "0::/kubepods.slice/pod123\n")
	writeFile(t, filepath.Join(root, "kubepods.slice/pod123/cpu.max"), "250000 100000\n")

	withCgroupPaths(t, root, proc)

	got := EffectiveGOMAXPROCS()
	require.Equal(t, 3, got)
}

func TestEffectiveGOMAXPROCS_CgroupV1NestedPath(t *testing.T) {
	root, proc := setupCgroupTest(t, "2:cpu,cpuacct:/docker/abc\n")
	writeFile(t, filepath.Join(root, "docker/abc/cpu.cfs_quota_us"), "150000\n")
	writeFile(t, filepath.Join(root, "docker/abc/cpu.cfs_period_us"), "100000\n")

	withCgroupPaths(t, root, proc)

	got := EffectiveGOMAXPROCS()
	require.Equal(t, 2, got)
}

func TestEffectiveGOMAXPROCS_UnlimitedFallsBack(t *testing.T) {
	root, proc := setupCgroupTest(t, "0::/kubepods.slice/pod123\n")
	writeFile(t, filepath.Join(root, "kubepods.slice/pod123/cpu.max"), "max 100000\n")

	withCgroupPaths(t, root, proc)

	if got := EffectiveGOMAXPROCS(); got <= 0 {
		t.Fatalf("EffectiveGOMAXPROCS() = %d, want positive fallback", got)
	}
}

func setupCgroupTest(t *testing.T, procContent string) (root string, proc string) {
	t.Helper()
	root = t.TempDir()
	proc = filepath.Join(t.TempDir(), "cgroup")
	writeFile(t, proc, procContent)
	return root, proc
}

func withCgroupPaths(t *testing.T, root string, proc string) {
	t.Helper()
	oldRoot := cgroupRoot
	oldProc := procSelfCgroupPath
	cgroupRoot = root
	procSelfCgroupPath = proc
	t.Cleanup(func() {
		cgroupRoot = oldRoot
		procSelfCgroupPath = oldProc
	})
}

func writeFile(t *testing.T, path string, content string) {
	t.Helper()
	err := os.MkdirAll(filepath.Dir(path), 0o755)
	require.NoError(t, err)
	err := os.WriteFile(path, []byte(content), 0o600)
	require.NoError(t, err)
}

func TestRaiseFileLimit(t *testing.T) {
	var before unix.Rlimit
	err := unix.Getrlimit(unix.RLIMIT_NOFILE, &before)
	require.NoErrorf(t, err, "getrlimit: %v", err)

	got, err := RaiseFileLimit(65536)
	require.NoErrorf(t, err, "RaiseFileLimit: %v", err)
	if got < 65536 && got < before.Max {
		t.Fatalf("soft limit = %d, want >= 65536 (or capped at hard limit %d)", got, before.Max)
	}

	var after unix.Rlimit
	err := unix.Getrlimit(unix.RLIMIT_NOFILE, &after)
	require.NoErrorf(t, err, "getrlimit after: %v", err)
	if after.Cur < 65536 && after.Cur < before.Max {
		t.Fatalf("soft limit after = %d, want >= 65536 (or capped at hard limit %d)", after.Cur, before.Max)
	}

	// Calling again with the same value should be a no-op.
	_, err := RaiseFileLimit(65536)
	require.NoErrorf(t, err, "RaiseFileLimit idempotent: %v", err)
}
