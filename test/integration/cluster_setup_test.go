//go:build integration

// Package integration_test contains multi-node cluster integration tests.
// The test suite starts one 3-node Docker Compose cluster per consistency
// mode (strong, eventual, full) and shares it across all tests in that mode.
//
// Prerequisites:
//   - Docker Engine running with compose plugin v2+
//   - make integration  OR:
//     go test -race -count=1 -timeout=15m -tags=integration \
//     -run TestStrong ./test/integration/...
//
// Port layout (host-side):
//
//	origin:   18080
//	bouine-1: HTTP=18081  admin=19001
//	bouine-2: HTTP=18082  admin=19002
//	bouine-3: HTTP=18083  admin=19003
package integration_test

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/thylong/bouine/test/integration/driver"
)

// Shared cluster stacks: one per consistency mode. Each cluster is started
// the first time a test for that mode needs it and kept alive until the end
// of the test run (torn down only when os.Exit is called, not per-test).
var (
	clusterMu     sync.Mutex
	clusterStacks = map[string]*driver.ClusterStack{}
	clusterClean  []func() // teardown functions, called by TestMain
)

// sharedCluster returns the cluster for the given mode, booting it once if
// needed. The cluster is NOT torn down by individual test cleanup; instead
// TestMain tears down all stacks after m.Run() returns.
func sharedCluster(t *testing.T, mode string) *driver.ClusterStack {
	t.Helper()
	clusterMu.Lock()
	defer clusterMu.Unlock()
	if s, ok := clusterStacks[mode]; ok {
		return s
	}
	s := driver.BootCluster(t, driver.ClusterOptions{
		Mode:          mode,
		NoAutoCleanup: true,
	})
	configDir := s.ConfigDir()
	clusterClean = append(clusterClean, func() {
		s.Down()
		_ = os.RemoveAll(configDir)
	})
	clusterStacks[mode] = s
	return s
}

// TestMain manages the lifecycle of all shared cluster stacks. It also
// prevents the test binary from running when Docker Compose is unavailable.
func TestMain(m *testing.M) {
	code := m.Run()

	// Tear down any clusters that were booted during the run.
	clusterMu.Lock()
	cleanups := clusterClean
	clusterMu.Unlock()
	for _, fn := range cleanups {
		fn()
	}

	// Ensure the port is free for the next run.
	fmt.Printf("integration: %d cluster stack(s) torn down\n", len(cleanups))
	os.Exit(code)
}
