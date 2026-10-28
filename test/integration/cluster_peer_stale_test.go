//go:build integration

package integration_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/test/integration/driver"
)

// TestStrong_PeerServedStaleIsRevalidated pins issue #857: a stale object
// that only non-owner nodes keep serving (as STALE from the owner over
// peer fetch) must still be refreshed. The origin's /stale endpoint is
// fresh for 1s with a one-hour stale-while-revalidate window and a new
// body on every fetch, so a refresh is visible as a body change.
//
// Before the fix, no node ever revalidated: the owner never learns that a
// peer served its stale copy, and the non-owner deferred revalidation to
// the owner. The body stayed the same for the whole SWR window.
func TestStrong_PeerServedStaleIsRevalidated(t *testing.T) {
	s := sharedCluster(t, "strong")
	// One Host for every node: the cache key includes it.
	const host = "peer-stale.test"
	path := fmt.Sprintf("/stale?x=peer-stale-%d", time.Now().UnixNano())

	first := s.GetWithHost(t, 0, path, host)
	require.Equal(t, 200, first.StatusCode)
	primed := string(first.Body)

	nonOwner := nonOwnerNode(t, s, path, host)

	// Let the object go stale (max-age=1), then only ever ask the
	// non-owner: the owner sees no client request for this key.
	time.Sleep(1500 * time.Millisecond)
	resp := s.GetWithHost(t, nonOwner, path, host)
	require.Equal(t, "STALE", resp.Header.Get("X-Cache"))
	require.Equal(t, "peer", resp.Header.Get("X-Cache-Source"))
	require.Equal(t, primed, string(resp.Body), "the stale copy is served while it is refreshed")

	// The refresh runs in the background and is forwarded to the owner;
	// the next peer fetches return the new body.
	require.Eventually(t, func() bool {
		return string(s.GetWithHost(t, nonOwner, path, host).Body) != primed
	}, 5*time.Second, 100*time.Millisecond,
		"a peer-served stale object must be refreshed (issue #857)")
}

// nonOwnerNode returns an alive node that serves path from a peer, i.e.
// a node that does not own the key.
func nonOwnerNode(t *testing.T, s *driver.ClusterStack, path, host string) int {
	t.Helper()
	for _, i := range s.AliveNodes() {
		if s.GetWithHost(t, i, path, host).Header.Get("X-Cache-Source") == "peer" {
			return i
		}
	}
	require.FailNow(t, "no alive node serves the key from a peer")
	return -1
}
