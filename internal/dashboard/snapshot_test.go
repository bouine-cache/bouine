package dashboard

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/api"
)

// TestLocalInsightSnapshot_NoPanic pins the nil-safety contract for
// the admin /v1/status default mode: a bare handler (no config, no
// peers, empty rings) must evaluate without panicking.
func TestLocalInsightSnapshot_NoPanic(t *testing.T) {
	t.Parallel()
	_, h := New(Config{Rings: observability.NewRings("bouine-0")})
	ins := h.LocalInsightSnapshot(t.Context())
	require.NotNil(t, ins, "Evaluate always returns a slice")
}

// TestLocalPeerResults_MarksStaleFromPeerRing pins the stale
// derivation for node-local status: a peer below full health in the
// local ring is reported stale without any network fan-out.
func TestLocalPeerResults_MarksStaleFromPeerRing(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("bouine-0")
	rings.Peer.Record("bouine-1", false)
	_, h := New(Config{
		Rings: rings,
		PeersFn: func() []api.PeerInfo {
			return []api.PeerInfo{
				{Name: "bouine-0"},
				{Name: "bouine-1"},
				{Name: "bouine-2"},
			}
		},
	})
	peers := h.localPeerResults()
	require.Len(t, peers, 3)
	byName := map[string]PeerResult{}
	for _, p := range peers {
		byName[p.NodeName] = p
	}
	assert.True(t, byName["bouine-1"].Stale, "degraded peer is stale")
	assert.False(t, byName["bouine-2"].Stale, "peer with no samples is not stale")
}

// TestClusterMetricsSnapshot_SingleNode exercises the cluster-mode
// fan-out with no remote peers: the aggregator returns the local
// node as the only result.
func TestClusterMetricsSnapshot_SingleNode(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("bouine-0")
	rings.Request.RecordRequest("HIT", 200, 5)
	_, h := New(Config{Rings: rings, PeersFn: func() []api.PeerInfo {
		return []api.PeerInfo{{Name: "bouine-0", AdminAddr: "127.0.0.1:9000"}}
	}, SelfAddr: "127.0.0.1:9000"})
	peers := h.ClusterMetricsSnapshot(t.Context())
	require.Len(t, peers, 1, "self only")
	assert.Equal(t, "bouine-0", peers[0].NodeName)
	assert.False(t, peers[0].Stale)
}
