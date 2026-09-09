package cluster

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

// TestMergeRemoteState_PrunesStalePeerOnDigestMatch feeds a cluster its
// own digest while a stale peer (not in memberlist's live set) sits in
// the ring — reproducing the #648 equilibrium where every peer holds
// the same stale ring, without needing a second stale node. Asserts the
// stale peer is pruned and the legitimate memberlist peer survives.
func TestMergeRemoteState_PrunesStalePeerOnDigestMatch(t *testing.T) {
	t.Parallel()

	cfg1 := defaultConfig(t, "survivor-a", "127.0.0.1:17961")
	cfg1.PushPullInterval = 100 * time.Hour
	c1, err := New(cfg1)
	require.NoError(t, err)
	defer func() { _ = c1.Leave(t.Context()) }()

	cfg2 := defaultConfig(t, "survivor-b", "127.0.0.1:17962")
	cfg2.PushPullInterval = 100 * time.Hour
	c2, err := New(cfg2)
	require.NoError(t, err)
	defer func() { _ = c2.Leave(t.Context()) }()

	_, err = c2.Join([]string{"127.0.0.1:17961"})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(c1.Members()) == 2 && len(c2.Members()) == 2
	}, 5*time.Second, 50*time.Millisecond, "peers did not converge")

	// Simulate the stale state on c1: a peer in the ring that is NOT
	// in memberlist's live member set.
	c1.addPeer("stale-pod", api.PeerInfo{Name: "stale-pod", Addr: "10.0.0.99:9000"})
	require.Len(t, c1.Members(), 3)

	// Feed c1 its own digest, so local and remote hashes match — the
	// equilibrium from issue #648.
	localDigest := c1.Digest()
	buf, err := json.Marshal(localDigest)
	require.NoError(t, err)

	// Before the fix, the digest shortcut returned early and the stale
	// peer survived forever.
	c1.MergeRemoteState(buf, false)

	members := c1.Members()
	assert.Len(t, members, 2, "stale peer should be pruned even when digests match")
	for _, m := range members {
		assert.NotEqual(t, "stale-pod", m.Name, "stale peer must be evicted")
	}
	names := map[string]bool{}
	for _, m := range members {
		names[m.Name] = true
	}
	assert.True(t, names["survivor-b"], "legitimate memberlist peer must survive pruning")
}

// TestMergeRemoteState_SameHashStillPrunesStalePeer is a focused test
// that verifies the digest shortcut no longer gates pruning: a single
// node with a stale peer in its ring, receiving its own digest, must
// still prune the stale peer.
func TestMergeRemoteState_SameHashStillPrunesStalePeer(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig(t, "solo-prune", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	// Add a stale peer that is not in memberlist.
	c.addPeer("ghost-peer", api.PeerInfo{Name: "ghost-peer", Addr: "127.0.0.1:5555"})
	require.Len(t, c.Members(), 2)

	// Marshal our own digest — local and remote hashes will be identical.
	local := c.Digest()
	buf, _ := json.Marshal(local)

	c.MergeRemoteState(buf, false)

	members := c.Members()
	assert.Len(t, members, 1, "stale peer must be pruned even when digests match")
	assert.Equal(t, "solo-prune", members[0].Name)
}

// TestAddPeer_DoesNotDuplicateVnodes verifies that calling addPeer
// multiple times for the same peer name does not accumulate duplicate
// vnodes in the ring (issue #648 Bug 2). Over HPA scale-up/down cycles,
// duplicate vnodes would grow r.nodes unboundedly, slowing ring.get()
// and wasting memory.
func TestAddPeer_DoesNotDuplicateVnodes(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig(t, "vnode-test", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	// Add a peer, then simulate a resurrection cycle by adding it again
	// multiple times (as would happen via NotifyJoin during push/pull).
	peerName := "resurrected-peer"
	peerInfo := api.PeerInfo{Name: peerName, Addr: "127.0.0.1:9999"}

	c.addPeer(peerName, peerInfo)
	c.addPeer(peerName, peerInfo)
	c.addPeer(peerName, peerInfo)

	// Count vnodes owned by this peer in the ring.
	c.mu.RLock()
	count := 0
	for _, owner := range c.ring.owners {
		if owner == peerName {
			count++
		}
	}
	totalNodes := len(c.ring.nodes)
	c.mu.RUnlock()

	assert.Equal(t, c.cfg.VirtualNodes, count,
		"peer should have exactly VirtualNodes vnodes, not duplicates")
	assert.Equal(t, c.cfg.VirtualNodes*2, totalNodes,
		"ring should have exactly VirtualNodes*(local+peer) total vnodes")
}

// TestAddPeer_RingGetRoutesAfterReAdd verifies that ring.get() routes
// correctly after multiple re-adds: both the local node and the
// re-added peer must receive traffic (no functional regression from
// the dedup fix).
func TestAddPeer_RingGetRoutesAfterReAdd(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig(t, "ring-get-test", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	peerName := "readded-peer"
	peerInfo := api.PeerInfo{Name: peerName, Addr: "127.0.0.1:8888"}

	c.addPeer(peerName, peerInfo)
	c.addPeer(peerName, peerInfo)

	c.mu.RLock()
	seen := map[string]int{}
	step := uint64(^uint64(0) / 1000)
	for i := range 1000 {
		seen[c.ring.get(testkey.Key(uint64(i)*step))]++
	}
	c.mu.RUnlock()

	assert.Greater(t, seen[peerName], 0, "re-added peer must still own vnodes")
	assert.Greater(t, seen[c.cfg.NodeName], 0, "local node must still own vnodes")
}
