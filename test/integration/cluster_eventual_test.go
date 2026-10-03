//go:build integration

package integration_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/test/integration/driver"
)

// Eventual mode: every node caches independently, no peer fetch, gossip invalidation.

func TestEventual_IndependentCaching(t *testing.T) {
	s := sharedCluster(t, "eventual")
	path := "/hit?x=eventual-independent"

	r := s.Get(t, 0, path)
	got := r.Header.Get("X-Cache")
	require.Equal(t, "MISS", got)
	r = s.Get(t, 0, path)
	got = r.Header.Get("X-Cache")
	require.Equal(t, "HIT", got)
	r = s.Get(t, 1, path)
	got = r.Header.Get("X-Cache")
	require.Equal(t, "MISS", got)
}

func TestEventual_NoPeerFetch(t *testing.T) {
	s := sharedCluster(t, "eventual")
	for _, i := range s.AliveNodes() {
		s.Get(t, i, "/hit?x=eventual-nopf")
	}
	for _, i := range s.AliveNodes() {
		hits := s.MetricValue(t, i, "bouine_peer_fetch_hits_total")
		assert.Equal(t, float64(0), hits)
	}
}

const crossNodeHost = driver.CrossNodeHost

func TestEventual_PurgePropagationGossip(t *testing.T) {
	s := sharedCluster(t, "eventual")
	path := "/hit?x=eventual-purge"

	// Warm all alive nodes — retry until every node reports a HIT.
	for _, i := range s.AliveNodes() {
		s.GetWithHost(t, i, path, crossNodeHost)
		driver.RetryUntil(t, 5*time.Second, 200*time.Millisecond, func() bool {
			resp := s.GetWithHost(t, i, path, crossNodeHost)
			return resp.Header.Get("X-Cache") == "HIT"
		})
	}
	s.Purge(t, 0, "http://"+driver.CrossNodeHost+path)
	driver.RetryUntil(t, driver.GossipConvergence, 500*time.Millisecond, func() bool {
		for _, i := range s.AliveNodes() {
			resp := s.GetWithHost(t, i, path, crossNodeHost)
			if resp.Header.Get("X-Cache") == "HIT" {
				return false
			}
		}
		return true
	})
}

// TestEventual_PurgeBatchPropagationGossip verifies that a large
// admin purge batch propagates via gossip in eventual mode (issue
// #754): a single oversized batch frame cannot fit memberlist's UDP
// gossip window, so it must be split into budget-sized frames that
// every peer applies.
//
// NOTE: keep this test above TestEventual_BanPropagationGossip in the
// file: that test issues a `.*` host ban on the shared stack, and bans
// stay active for the whole suite, so nothing can HIT afterwards.
func TestEventual_PurgeBatchPropagationGossip(t *testing.T) {
	s := sharedCluster(t, "eventual")

	paths := make([]string, 100)
	urls := make([]string, len(paths))
	for i := range paths {
		paths[i] = fmt.Sprintf("/hit?x=eventual-purge-batch-%d", i)
		urls[i] = "http://" + driver.CrossNodeHost + paths[i]
	}

	// Warm every node for every path until all report HIT.
	for _, i := range s.AliveNodes() {
		for _, p := range paths {
			s.GetWithHost(t, i, p, crossNodeHost)
			driver.RetryUntil(t, 5*time.Second, 200*time.Millisecond, func() bool {
				resp := s.GetWithHost(t, i, p, crossNodeHost)
				return resp.Header.Get("X-Cache") == "HIT"
			})
		}
	}

	// One 100-key batched purge on node 0: gossip is the sole
	// invalidation path in eventual mode, so this fails without
	// frame splitting.
	s.PurgeBatch(t, 0, urls)

	for _, i := range s.AliveNodes() {
		for _, p := range paths {
			driver.RetryUntil(t, driver.GossipConvergence, 500*time.Millisecond, func() bool {
				resp := s.GetWithHost(t, i, p, crossNodeHost)
				return resp.Header.Get("X-Cache") != "HIT"
			})
		}
	}
}

func TestEventual_BanPropagationGossip(t *testing.T) {
	s := sharedCluster(t, "eventual")
	path := "/hit?x=eventual-ban"

	for _, i := range s.AliveNodes() {
		s.GetWithHost(t, i, path, crossNodeHost)
		time.Sleep(100 * time.Millisecond)
		s.GetWithHost(t, i, path, crossNodeHost)
	}
	s.Ban(t, 0, ".*", "")
	driver.RetryUntil(t, driver.GossipConvergence, 500*time.Millisecond, func() bool {
		for _, i := range s.AliveNodes() {
			resp := s.GetWithHost(t, i, path, crossNodeHost)
			if resp.Header.Get("X-Cache") == "HIT" {
				return false
			}
		}
		return true
	})
}

// TestEventual_StaleDuringConvergence verifies that a node that
// fetched during the convergence window does not serve stale content
// once the purge gossip lands. It runs after the ban test, but its
// assertion (!= "HIT") holds trivially under the suite's `.*` ban.
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
