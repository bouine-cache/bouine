//go:build integration

package integration_test

import (
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
	got := r.Header.Get("X-Cache")
	require.Equal(t, "HIT", got)
	r = s.Get(t, 1, path)
	got := r.Header.Get("X-Cache")
	require.Equal(t, "MISS", got)
}

func TestEventual_NoPeerFetch(t *testing.T) {
	s := sharedCluster(t, "eventual")
	for i := range s.Nodes {
		s.Get(t, i, "/hit?x=eventual-nopf")
	}
	for i := range s.Nodes {
		hits := s.MetricValue(t, i, "bouine_peer_fetch_hits_total")
		assert.Equal(t, 0, hits)
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
