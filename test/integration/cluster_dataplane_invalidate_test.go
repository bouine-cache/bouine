//go:build integration

package integration_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/test/integration/driver"
)

// Data-plane invalidation propagation (issue #753): a POST/PUT/DELETE
// through the data plane must invalidate the cached representation on
// the cluster, not just the receiving node's local store. CrossNodeHost
// makes every node derive the same cache key, so the broadcast matches.

// TestStrong_DataPlaneInvalidatePropagates verifies the strong-mode fix:
// an invalidating POST landing on a NON-owner node must purge the owner's
// copy. Before the fix the non-owner purged its (empty) local store and
// the owner kept serving the stale object until TTL.
func TestStrong_DataPlaneInvalidatePropagates(t *testing.T) {
	s := sharedCluster(t, "strong")
	path := "/hit?x=strong-dataplane-invalidate"

	// Prime the key through node 0: the owner stores the object (either
	// directly or via the write-to-owner RPC).
	s.GetWithHost(t, 0, path, crossNodeHost)
	driver.RetryUntil(t, 5*time.Second, 200*time.Millisecond, func() bool {
		resp := s.GetWithHost(t, 0, path, crossNodeHost)
		return resp.Header.Get("X-Cache") == "HIT"
	})

	// Warm every node and identify the owner: in strong mode only the
	// owner stores the object, so its GET is served from the local store
	// (X-Cache-Source: hot) while non-owners peer-fetch (X-Cache-Source:
	// peer). Record each node's pre-invalidation body.
	preBody := make(map[int]string)
	owner := -1
	for _, i := range s.AliveNodes() {
		resp := s.GetWithHost(t, i, path, crossNodeHost)
		require.Equal(t, "HIT", resp.Header.Get("X-Cache"), "node %d must serve a HIT after warm-up", i)
		preBody[i] = string(resp.Body)
		if resp.Header.Get("X-Cache-Source") == "hot" {
			owner = i
		}
	}
	require.NotEqual(t, -1, owner, "exactly one node must own the key")

	// Pick a non-owner to send the invalidating POST through — the exact
	// production shape of the bug: the request lands behind a load
	// balancer on a node that does not hold the object.
	poster := -1
	for _, i := range s.AliveNodes() {
		if i != owner {
			poster = i
			break
		}
	}
	require.NotEqual(t, -1, poster, "a 3-node cluster always has a non-owner")

	resp := s.PostWithHost(t, poster, path, crossNodeHost)
	require.Equal(t, 200, resp.StatusCode)

	// After the broadcast, every node must observe a fresh origin fetch:
	// the body differs from the pre-invalidation cached copy. The GETs
	// re-populate the cache, so one observed change per node is proof.
	for _, i := range s.AliveNodes() {
		driver.RetryUntil(t, driver.GossipConvergence, 200*time.Millisecond, func() bool {
			resp := s.GetWithHost(t, i, path, crossNodeHost)
			return string(resp.Body) != preBody[i]
		})
	}
}

// TestEventual_DataPlaneInvalidatePropagates verifies the eventual-mode
// fix: every node caches independently, so a data-plane invalidation on
// one node must reach every node via gossip.
func TestEventual_DataPlaneInvalidatePropagates(t *testing.T) {
	s := sharedCluster(t, "eventual")
	path := "/hit?x=eventual-dataplane-invalidate"

	// Warm every node's independent copy and record its body. Origin
	// bodies embed a timestamp, so each node's copy is distinct.
	preBody := make(map[int]string)
	for _, i := range s.AliveNodes() {
		s.GetWithHost(t, i, path, crossNodeHost)
		driver.RetryUntil(t, 5*time.Second, 200*time.Millisecond, func() bool {
			resp := s.GetWithHost(t, i, path, crossNodeHost)
			return resp.Header.Get("X-Cache") == "HIT"
		})
		preBody[i] = string(s.GetWithHost(t, i, path, crossNodeHost).Body)
	}

	// Invalidate through node 0's data plane.
	resp := s.PostWithHost(t, 0, path, crossNodeHost)
	require.Equal(t, 200, resp.StatusCode)

	// Every node must converge to a fresh origin fetch: body differs
	// from its pre-invalidation cached copy.
	for _, i := range s.AliveNodes() {
		driver.RetryUntil(t, driver.GossipConvergence, 200*time.Millisecond, func() bool {
			resp := s.GetWithHost(t, i, path, crossNodeHost)
			return string(resp.Body) != preBody[i]
		})
	}
}
