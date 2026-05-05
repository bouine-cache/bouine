//go:build integration

package integration_test

import (
	"os"
	"sync"
	"testing"

	"github.com/thylong/bouine/test/integration/driver"
)

// Shared cluster stacks: one per consistency mode, started once.
var (
	clusterMu     sync.Mutex
	clusterStacks = map[string]*driver.ClusterStack{}
	clusterClean  []func()
)

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
	clusterClean = append(clusterClean, s.Down)
	clusterStacks[mode] = s
	return s
}

func TestMain(m *testing.M) {
	code := m.Run()
	clusterMu.Lock()
	for _, fn := range clusterClean {
		fn()
	}
	clusterMu.Unlock()
	os.Exit(code)
}
