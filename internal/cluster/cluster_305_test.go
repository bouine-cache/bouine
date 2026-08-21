package cluster

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

func TestOwner_EmptyRingReturnsZeroPeerInfo(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "solo", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	c.SetMetrics(m)

	c.removePeer("solo")

	owner := c.Owner(testkey.Key(42))
	assert.Equal(t, api.PeerInfo{}, owner)
	assert.Equal(t, "", owner.Name)

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "bouine_cluster_ring_empty_total" {
			require.Len(t, f.GetMetric(), 1)
			assert.Equal(t, 1.0, f.GetMetric()[0].GetCounter().GetValue())
			return
		}
	}
	t.Fatal("bouine_cluster_ring_empty_total not registered or not incremented")
}

func TestOwner_EmptyRingNilMetricsSafe(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "solo-nil", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	c.removePeer("solo-nil")

	owner := c.Owner(testkey.Key(1))
	assert.Equal(t, api.PeerInfo{}, owner)
}

func TestIsLocal_EmptyRingReturnsFalse(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "solo-il", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	c.removePeer("solo-il")
	assert.False(t, c.IsLocal(testkey.Key(99)))
}

func TestOwner_SingleNodeReturnsLocal(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "solo-ok", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	owner := c.Owner(testkey.Key(7))
	assert.Equal(t, "solo-ok", owner.Name)
}

func TestOwner_RingHasNameNotInPeersReturnsLocal(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "fallback", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	c.mu.Lock()
	c.ring.add("stray", c.cfg.VirtualNodes)
	c.mu.Unlock()

	owner := c.Owner(testkey.Key(7))
	assert.Equal(t, "fallback", owner.Name)
}

func TestNotifyLeave_IgnoresSelf(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "self-leave", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	c.NotifyLeave(&memberlist.Node{Name: "self-leave"})

	owner := c.Owner(testkey.Key(5))
	assert.Equal(t, "self-leave", owner.Name)
	assert.True(t, c.IsLocal(testkey.Key(5)))
}

func TestNotifyLeave_RemovesOtherPeer(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "survivor", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	c.addPeer("dead-peer", api.PeerInfo{Name: "dead-peer", Addr: "127.0.0.1:9999"})
	require.Contains(t, c.Members(), api.PeerInfo{Name: "dead-peer", Addr: "127.0.0.1:9999"})

	c.NotifyLeave(&memberlist.Node{Name: "dead-peer"})

	members := c.Members()
	for _, m := range members {
		assert.NotEqual(t, "dead-peer", m.Name)
	}
}

func TestMergeRemoteState_PrunesDeadPeers(t *testing.T) {
	t.Parallel()

	cfg1 := defaultConfig(t, "node-a", "127.0.0.1:17941")
	cfg1.PushPullInterval = 100 * time.Hour
	c1, err := New(cfg1)
	require.NoError(t, err)
	defer func() { _ = c1.Leave(t.Context()) }()

	cfg2 := defaultConfig(t, "node-b", "127.0.0.1:17942")
	cfg2.PushPullInterval = 100 * time.Hour
	c2, err := New(cfg2)
	require.NoError(t, err)
	defer func() { _ = c2.Leave(t.Context()) }()

	_, err = c2.Join([]string{"127.0.0.1:17941"})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(c1.Members()) == 2 && len(c2.Members()) == 2
	}, 5*time.Second, 50*time.Millisecond, "peers did not converge")

	c2.addPeer("ghost", api.PeerInfo{Name: "ghost", Addr: "127.0.0.1:5555"})
	c2.addPeer("phantom", api.PeerInfo{Name: "phantom", Addr: "127.0.0.1:6666"})
	require.Len(t, c2.Members(), 4)

	remote := c1.Digest()
	buf, err := json.Marshal(remote)
	require.NoError(t, err)

	c2.MergeRemoteState(buf, false)

	members := c2.Members()
	assert.Len(t, members, 2)
	names := map[string]bool{}
	for _, m := range members {
		names[m.Name] = true
	}
	assert.True(t, names["node-a"])
	assert.True(t, names["node-b"])
	assert.False(t, names["ghost"])
	assert.False(t, names["phantom"])
}

func TestMergeRemoteState_AddsMissingPeers(t *testing.T) {
	t.Parallel()

	cfg1 := defaultConfig(t, "ms-node-a", "127.0.0.1:17951")
	cfg1.PushPullInterval = 100 * time.Hour
	c1, err := New(cfg1)
	require.NoError(t, err)
	defer func() { _ = c1.Leave(t.Context()) }()

	cfg2 := defaultConfig(t, "ms-node-b", "127.0.0.1:17952")
	cfg2.PushPullInterval = 100 * time.Hour
	c2, err := New(cfg2)
	require.NoError(t, err)
	defer func() { _ = c2.Leave(t.Context()) }()

	_, err = c2.Join([]string{"127.0.0.1:17951"})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(c1.Members()) == 2 && len(c2.Members()) == 2
	}, 5*time.Second, 50*time.Millisecond, "peers did not converge")

	remote := c2.Digest()
	c2.removePeer("ms-node-a")
	require.Len(t, c2.Members(), 1)

	buf, err := json.Marshal(remote)
	require.NoError(t, err)

	c2.MergeRemoteState(buf, false)

	require.Eventually(t, func() bool {
		return len(c2.Members()) == 2
	}, 2*time.Second, 50*time.Millisecond)
}

func TestMergeRemoteState_EmptyBufferNoOp(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "empty-buf", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	c.MergeRemoteState(nil, false)
	require.Len(t, c.Members(), 1)
}

func TestMergeRemoteState_BadJSONNoOp(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "bad-json", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	c.MergeRemoteState([]byte("{not json}"), false)
	require.Len(t, c.Members(), 1)
}

func TestMergeRemoteState_SameHashNoOp(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "same-hash", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	local := c.Digest()
	buf, _ := json.Marshal(local)
	c.MergeRemoteState(buf, false)
	require.Len(t, c.Members(), 1)
}

func TestIncRingEmpty_IncrementsCounter(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	m.IncRingEmpty()
	m.IncRingEmpty()

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "bouine_cluster_ring_empty_total" {
			require.Len(t, f.GetMetric(), 1)
			assert.Equal(t, 2.0, f.GetMetric()[0].GetCounter().GetValue())
			return
		}
	}
	t.Fatal("bouine_cluster_ring_empty_total not registered")
}

func TestIncRingEmpty_NilMetricsSafe(t *testing.T) {
	t.Parallel()
	var m *Metrics
	m.IncRingEmpty()
}

func TestRingEmpty_DoesNotFailOpenToSingleNode(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "no-failopen", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	c.removePeer("no-failopen")

	owner := c.Owner(testkey.Key(100))
	if owner.Name == "no-failopen" {
		t.Fatal("Owner returned local node on empty ring — fail-open regression")
	}
	assert.Equal(t, "", owner.Name)
}

func TestOwner_LoggingWarnsOnEmptyRing(t *testing.T) {
	t.Parallel()
	logger, mu, buf := captureLogger(t)
	cfg := Config{
		NodeName: "log-test",
		BindAddr: "127.0.0.1:0",
		Mode:     "strong",
		Logger:   logger,
		PeerInfo: api.PeerInfo{
			Name:     "log-test",
			Addr:     "127.0.0.1:0",
			DataAddr: "127.0.0.1:0",
		},
	}
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	c.removePeer("log-test")
	c.Owner(testkey.Key(1))

	records := parseAdapterRecords(t, mu, buf)
	found := false
	for _, rec := range records {
		if rec["msg"] == "cluster: ring empty, cannot determine owner" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected warn log for empty ring")
}
