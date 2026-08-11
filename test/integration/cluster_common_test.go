//go:build integration

package integration_test

import (
	"testing"
	"time"
)

var clusterModes = []string{"strong", "eventual"}

// TestCluster_Formation verifies that all nodes report 3 peers for every
// cluster mode. The mode is the only variable.
func TestCluster_Formation(t *testing.T) {
	for _, mode := range clusterModes {
		t.Run(mode, func(t *testing.T) {
			s := sharedCluster(t, mode)
			for _, i := range s.AliveNodes() {
				node := s.Nodes[i]
				peers := s.Peers(t, i)
				if len(peers) != len(s.AliveNodes()) {
					t.Errorf("node %s: got %d peers, want %d", node.Name, len(peers), len(s.AliveNodes()))
				}
			}
		})
	}
}

// TestCluster_SingleNodeFailure verifies that killing one node leaves the
// surviving nodes serving 200.
//
// This test kills node 2 and does NOT restart it — memberlist rejoin
// with fresh ports is unreliable in-process. Subsequent tests must
// tolerate node 2 being absent. When run via `make integration`, this
// test executes in its own process (test-integration-cluster-common)
// so the killed node does not affect the strong/eventual test runs
// which get fresh clusters in separate processes.
func TestCluster_SingleNodeFailure(t *testing.T) {
	for _, mode := range clusterModes {
		t.Run(mode, func(t *testing.T) {
			s := sharedCluster(t, mode)
			path := "/hit?x=" + mode + "-failure"

			if mode == "eventual" {
				// Eventual mode: each node caches independently. Prime
				// the survivor nodes (0 and 1) so they have a cached
				// copy that survives the kill without an origin fetch.
				for _, n := range []int{0, 1} {
					s.Get(t, n, path)
					time.Sleep(100 * time.Millisecond)
					s.Get(t, n, path)
				}
			} else {
				// Strong mode: prime via node 0 (owner caches it).
				s.Get(t, 0, path)
				time.Sleep(200 * time.Millisecond)
			}

			s.KillNode(t, 2)
			time.Sleep(2 * time.Second)

			for _, n := range []int{0, 1} {
				resp := s.Get(t, n, path)
				if resp.StatusCode != 200 {
					t.Errorf("node %d after kill: status = %d", n, resp.StatusCode)
				}
			}

			// Node 2 is intentionally left killed. Subsequent tests must
			// tolerate its absence (only hit nodes 0 and 1).
		})
	}
}
