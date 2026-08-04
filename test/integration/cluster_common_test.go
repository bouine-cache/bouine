//go:build integration

package integration_test

import (
	"github.com/stretchr/testify/assert"
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
			for i, node := range s.Nodes {
				peers := s.Peers(t, i)
				if len(peers) != 3 {
					t.Errorf("node %s: got %d peers, want 3", node.Name, len(peers))
				}
			}
		})
	}
}

// TestCluster_SingleNodeFailure verifies that killing one node leaves the
// surviving nodes serving 200.
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
		})
	}
}
