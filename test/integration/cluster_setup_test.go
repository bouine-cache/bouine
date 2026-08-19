//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/test/integration/driver"
	"go.uber.org/goleak"
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
	if code == 0 {
		// memberlist.Shutdown() returns before all internal goroutines
		// (probe, gossip, schedule) have fully exited. goleak.Find polls
		// with backoff, but memberlist's cleanup can take a few seconds
		// on slow CI runners. Give the goroutines time to drain.
		time.Sleep(3 * time.Second)
		if err := goleak.Find(); err != nil {
			fmt.Fprintf(os.Stderr, "goleak: %v\n", err)
			code = 1
		}
	}
	os.Exit(code)
}
