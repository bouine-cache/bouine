//go:build linux

package platform

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEffectiveGOMAXPROCS_CgroupV2NestedPath(t *testing.T) {
	root, proc := setupCgroupTest(t, "0::/kubepods.slice/pod123\n")
	writeFile(t, filepath.Join(root, "kubepods.slice/pod123/cpu.max"), "250000 100000\n")

	withCgroupPaths(t, root, proc)

	if got := EffectiveGOMAXPROCS(); got != 3 {
		t.Fatalf("EffectiveGOMAXPROCS() = %d, want 3", got)
	}
}

func TestEffectiveGOMAXPROCS_CgroupV1NestedPath(t *testing.T) {
	root, proc := setupCgroupTest(t, "2:cpu,cpuacct:/docker/abc\n")
	writeFile(t, filepath.Join(root, "docker/abc/cpu.cfs_quota_us"), "150000\n")
	writeFile(t, filepath.Join(root, "docker/abc/cpu.cfs_period_us"), "100000\n")

	withCgroupPaths(t, root, proc)

	if got := EffectiveGOMAXPROCS(); got != 2 {
		t.Fatalf("EffectiveGOMAXPROCS() = %d, want 2", got)
	}
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
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
