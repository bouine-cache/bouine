//go:build integration

package integration_test

import (
	"testing"
	"time"

	"github.com/thylong/bouine/test/integration/driver"
)

// Eventual mode: every node caches independently, no peer fetch, gossip invalidation.

func TestEventual_ClusterFormation(t *testing.T) {
	s := sharedCluster(t, "eventual")
	for i, node := range s.Nodes {
		peers := s.Peers(t, i)
		if len(peers) != 3 {
			t.Errorf("node %s: got %d peers, want 3", node.Name, len(peers))
		}
	}
}

func TestEventual_IndependentCaching(t *testing.T) {
	s := sharedCluster(t, "eventual")
	path := "/hit?x=eventual-independent"

	r := s.Get(t, 0, path)
	if got := r.Header.Get("X-Cache"); got != "MISS" {
		t.Fatalf("node 0 first: X-Cache = %q, want MISS", got)
	}
	r = s.Get(t, 0, path)
	if got := r.Header.Get("X-Cache"); got != "HIT" {
		t.Fatalf("node 0 second: X-Cache = %q, want HIT", got)
	}
	r = s.Get(t, 1, path)
	if got := r.Header.Get("X-Cache"); got != "MISS" {
		t.Fatalf("node 1 (eventual, no replication): X-Cache = %q, want MISS", got)
	}
}

func TestEventual_NoPeerFetch(t *testing.T) {
	s := sharedCluster(t, "eventual")
	for i := range s.Nodes {
		s.Get(t, i, "/hit?x=eventual-nopf")
	}
	for i := range s.Nodes {
		hits := s.MetricValue(t, i, "bouine_peer_fetch_hits_total")
		if hits != 0 {
			t.Errorf("node %d: peer_fetch_hits_total = %.0f, want 0", i, hits)
		}
	}
}

const crossNodeHost = driver.CrossNodeHost

func TestEventual_PurgePropagationGossip(t *testing.T) {
	s := sharedCluster(t, "eventual")
	path := "/hit?x=eventual-purge"

	// Warm all nodes — retry until every node reports a HIT.
	for i := range s.Nodes {
		s.GetWithHost(t, i, path, crossNodeHost)
		driver.RetryUntil(t, 5*time.Second, 200*time.Millisecond, func() bool {
			resp := s.GetWithHost(t, i, path, crossNodeHost)
			return resp.Header.Get("X-Cache") == "HIT"
		})
	}
	s.Purge(t, 0, "http://"+driver.CrossNodeHost+path)
	driver.RetryUntil(t, driver.GossipConvergence, 500*time.Millisecond, func() bool {
		for i := range s.Nodes {
			resp := s.GetWithHost(t, i, path, crossNodeHost)
			if resp.Header.Get("X-Cache") == "HIT" {
				return false
			}
		}
		return true
	})
}

func TestEventual_BanPropagationGossip(t *testing.T) {
	s := sharedCluster(t, "eventual")
	path := "/hit?x=eventual-ban"

	for i := range s.Nodes {
		s.GetWithHost(t, i, path, crossNodeHost)
		time.Sleep(100 * time.Millisecond)
		s.GetWithHost(t, i, path, crossNodeHost)
	}
	s.Ban(t, 0, ".*", "")
	driver.RetryUntil(t, driver.GossipConvergence, 500*time.Millisecond, func() bool {
		for i := range s.Nodes {
			resp := s.GetWithHost(t, i, path, crossNodeHost)
			if resp.Header.Get("X-Cache") == "HIT" {
				return false
			}
		}
		return true
	})
}

func TestEventual_StaleDuringConvergence(t *testing.T) {
	s := sharedCluster(t, "eventual")
	path := "/hit?x=eventual-stale"

	s.GetWithHost(t, 1, path, crossNodeHost)
	time.Sleep(100 * time.Millisecond)
	s.GetWithHost(t, 1, path, crossNodeHost)
	s.Purge(t, 0, "http://"+driver.CrossNodeHost+path)
	driver.RetryUntil(t, driver.GossipConvergence, 500*time.Millisecond, func() bool {
		resp := s.GetWithHost(t, 1, path, crossNodeHost)
		return resp.Header.Get("X-Cache") != "HIT"
	})
}

func TestEventual_SingleNodeFailure(t *testing.T) {
	s := sharedCluster(t, "eventual")
	path := "/hit?x=eventual-failure"
	for _, n := range []int{1, 2} {
		s.Get(t, n, path)
		time.Sleep(100 * time.Millisecond)
		s.Get(t, n, path)
	}
	s.KillNode(t, 2)
	time.Sleep(2 * time.Second)
	for _, n := range []int{0, 1} {
		resp := s.Get(t, n, path)
		if resp.StatusCode != 200 {
			t.Errorf("node %d after kill node 2: status = %d", n, resp.StatusCode)
		}
	}
}
