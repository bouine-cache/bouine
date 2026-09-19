package cluster

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

func defaultConfig(t *testing.T, name, addr string) Config {
	t.Helper()
	return Config{
		NodeName: name,
		BindAddr: addr,
		Mode:     "strong",
		Logger:   observability.NoopLogger{},
		PeerInfo: api.PeerInfo{
			Name:      name,
			Addr:      addr,
			DataAddr:  "127.0.0.1:0",
			AdminAddr: "127.0.0.1:0",
			Weight:    1.0,
		},
	}
}

func TestRing_AddGet(t *testing.T) {
	t.Parallel()
	r := newRing(256)
	r.add("alpha", 256)
	r.add("beta", 256)
	r.add("gamma", 256)

	owners := map[string]int{}
	// Use sequential keys spread across the full uint64 range.
	step := uint64(^uint64(0) / 1000)
	for i := range 1000 {
		key := testkey.Key(uint64(i) * step)
		owners[r.get(key)]++
	}
	for _, name := range []string{"alpha", "beta", "gamma"} {
		assert.NotEqual(t, 0, owners[name])
	}
}

func TestRing_RemoveRedistributes(t *testing.T) {
	t.Parallel()
	r := newRing(64)
	r.add("a", 64)
	r.add("b", 64)

	key := testkey.Key(12345678)
	owner := r.get(key)

	r.remove(owner)
	newOwner := r.get(key)
	require.NotEqual(t, owner, newOwner)
	require.NotEqual(t, "", newOwner)
}

func TestRing_Digest_Changes(t *testing.T) {
	t.Parallel()
	r := newRing(16)
	r.add("node1", 16)
	d1 := r.digest()

	r.add("node2", 16)
	d2 := r.digest()

	require.NotEqual(t, d2.Hash, d1.Hash)
	require.Equal(t, 2, d2.Size)
}

func TestRing_SingleNode(t *testing.T) {
	t.Parallel()
	r := newRing(64)
	r.add("only", 64)
	for i := range 10 {
		require.Equal(t, "only", r.get(testkey.Key(uint64(i))))
	}
}

func TestCluster_LocalMode(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	members := c.Members()
	require.Len(t, members, 1)
	require.Equal(t, "local", members[0].Name)
	key := testkey.Key(999)
	require.True(t, c.IsLocal(key))
}

func TestCluster_TwoNodeJoin(t *testing.T) {
	t.Parallel()
	c1, err := New(defaultConfig(t, "node1", "127.0.0.1:17900"))
	require.NoError(t, err, "c1")
	defer func() { _ = c1.Leave(t.Context()) }()

	c2, err := New(defaultConfig(t, "node2", "127.0.0.1:17901"))
	require.NoError(t, err, "c2")
	defer func() { _ = c2.Leave(t.Context()) }()

	_, err = c2.Join([]string{"127.0.0.1:17900"})
	require.NoError(t, err, "join")

	// Wait for gossip to propagate.
	for range 50 {
		if len(c1.ml.Members()) == 2 {
			break
		}
		// slight pause
		select {}
	}
}

func TestNotifyMsg_PurgeEvent(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	var called atomic.Int32
	c.SetInvalidator(Invalidator{
		PurgeFn: func(_ context.Context, _ api.PurgeEvent) error {
			called.Add(1)
			return nil
		},
	})

	evt := api.PurgeEvent{Key: testkey.Key(42), VaryKey: "v1", Issuer: "local"}
	msg, _ := EncodePurgeGossip(evt)
	c.NotifyMsg(msg)

	got := called.Load()
	require.Equal(t, int32(1), got)
}

func TestNotifyMsg_BanEvent(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	var called atomic.Int32
	c.SetInvalidator(Invalidator{
		BanFn: func(_ context.Context, _ api.BanEvent) error {
			called.Add(1)
			return nil
		},
	})

	evt := api.BanEvent{Predicate: api.BanExpr{HostRegex: "example\\.com"}, Issuer: "local"}
	msg, _ := EncodeBanGossip(evt)
	c.NotifyMsg(msg)

	got := called.Load()
	require.Equal(t, int32(1), got)
}

func TestNotifyMsg_RefreshEvent(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	var called atomic.Int32
	var receivedKey api.Key
	c.SetInvalidator(Invalidator{
		RefreshFn: func(_ context.Context, evt api.RefreshEvent) error {
			called.Add(1)
			receivedKey = evt.Key
			return nil
		},
	})

	evt := api.RefreshEvent{Key: testkey.Key(77), Issuer: "local"}
	msg, _ := EncodeRefreshGossip(evt)
	c.NotifyMsg(msg)

	require.Equal(t, int32(1), called.Load())
	require.Equal(t, testkey.Key(77), receivedKey)
}

func TestNotifyMsg_RefreshEvent_NoCallback(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	// No RefreshFn set — should not panic.
	evt := api.RefreshEvent{Key: testkey.Key(1), Issuer: "local"}
	msg, _ := EncodeRefreshGossip(evt)
	c.NotifyMsg(msg)
}

func TestNotifyMsg_RefreshEvent_ApplyError(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	c.SetInvalidator(Invalidator{
		RefreshFn: func(_ context.Context, _ api.RefreshEvent) error {
			return errors.New("apply failed")
		},
	})

	// Should not panic on apply error.
	evt := api.RefreshEvent{Key: testkey.Key(1), Issuer: "local"}
	msg, _ := EncodeRefreshGossip(evt)
	c.NotifyMsg(msg)
}

func TestNotifyMsg_MalformedPayload(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	// Should not panic on invalid data.
	c.NotifyMsg([]byte("{not json}"))
	c.NotifyMsg([]byte(""))
	// An empty JSON object has no "type" field and should be silently ignored.
	c.NotifyMsg([]byte("{}"))
}

func TestNotifyMsg_WhenNoCallbacks(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	// Should not panic when no invalidator is set.
	evt := api.PurgeEvent{Key: testkey.Key(42)}
	msg, _ := EncodePurgeGossip(evt)
	c.NotifyMsg(msg)
}

func TestNotifyMsg_PurgeCtxHasDeadline(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	cfg.GossipApplyTimeout = 50 * time.Millisecond
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	var got atomic.Pointer[context.Context]
	c.SetInvalidator(Invalidator{
		PurgeFn: func(ctx context.Context, _ api.PurgeEvent) error {
			got.Store(&ctx)
			return nil
		},
	})
	evt := api.PurgeEvent{Key: testkey.Key(7), Issuer: "local"}
	msg, _ := EncodePurgeGossip(evt)
	c.NotifyMsg(msg)

	ctx := *got.Load()
	require.NotNil(t, ctx)
	_, ok := ctx.Deadline()
	require.True(t, ok)
}

func TestNotifyMsg_BanCtxHasDeadline(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	var got atomic.Pointer[context.Context]
	c.SetInvalidator(Invalidator{
		BanFn: func(ctx context.Context, _ api.BanEvent) error {
			got.Store(&ctx)
			return nil
		},
	})
	evt := api.BanEvent{Predicate: api.BanExpr{HostRegex: "example\\.com"}, Issuer: "local"}
	msg, _ := EncodeBanGossip(evt)
	c.NotifyMsg(msg)

	ctx := *got.Load()
	require.NotNil(t, ctx)
	_, ok := ctx.Deadline()
	require.True(t, ok)
}

func TestNotifyMsg_DefaultApplyTimeout(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	require.Equal(t, 100*time.Millisecond, c.cfg.GossipApplyTimeout)
}

func TestNew_DefaultHandoffQueueDepth(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	require.Equal(t, defaultHandoffQueueDepth, c.cfg.HandoffQueueDepth)
}

func TestNew_CustomHandoffQueueDepth(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	cfg.HandoffQueueDepth = 8192
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	require.Equal(t, 8192, c.cfg.HandoffQueueDepth)
}

func TestNotifyMsg_PurgeTimeoutAbortsApply(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	cfg.GossipApplyTimeout = 10 * time.Millisecond
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	c.SetInvalidator(Invalidator{
		PurgeFn: func(ctx context.Context, _ api.PurgeEvent) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})
	evt := api.PurgeEvent{Key: testkey.Key(1), Issuer: "local"}
	msg, _ := EncodePurgeGossip(evt)
	start := time.Now()
	c.NotifyMsg(msg)
	elapsed := time.Since(start)
	if elapsed > 200*time.Millisecond {
		t.Fatalf("NotifyMsg blocked for %v; apply was not bounded by timeout", elapsed)
	}
}

func TestNotifyMsg_FailedApplySkipsMetric(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	cfg.GossipApplyTimeout = 10 * time.Millisecond
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	c.SetMetrics(m)

	c.SetInvalidator(Invalidator{
		PurgeFn: func(ctx context.Context, _ api.PurgeEvent) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})
	evt := api.PurgeEvent{Key: testkey.Key(1), Issuer: "local"}
	msg, _ := EncodePurgeGossip(evt)
	c.NotifyMsg(msg)

	metrics, err := reg.Gather()
	require.NoError(t, err, "gather")
	for _, mf := range metrics {
		if mf.GetName() != "bouine_cluster_invalidations_gossip_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetValue() == "purge" {
					require.Equal(t, 0, m.GetCounter().GetValue())
				}
			}
		}
	}
}

func TestNew_NegativeHandoffQueueDepthRejected(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	cfg.HandoffQueueDepth = -1
	_, err := New(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "HandoffQueueDepth")
}

func TestNew_HandoffQueueDepthExceedsUpperBoundRejected(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	cfg.HandoffQueueDepth = MaxHandoffQueueDepth + 1
	_, err := New(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "HandoffQueueDepth")
	require.Contains(t, err.Error(), "must be <=")
}

func TestNew_HandoffQueueDepthAtUpperBoundAccepted(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	cfg.HandoffQueueDepth = MaxHandoffQueueDepth
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	require.Equal(t, MaxHandoffQueueDepth, c.cfg.HandoffQueueDepth)
}

func TestIncGossipDrop_IncrementsCounter(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	m.IncGossipDrop()
	m.IncGossipDrop()

	families, err := reg.Gather()
	require.NoError(t, err, "gather")
	found := false
	for _, f := range families {
		if f.GetName() != "bouine_cluster_gossip_drops_total" {
			continue
		}
		found = true
		require.Len(t, f.GetMetric(), 1)
		require.Equal(t, 2.0, f.GetMetric()[0].GetCounter().GetValue())
	}
	require.True(t, found, "bouine_cluster_gossip_drops_total not registered")
}

func TestIncGossipDrop_NilMetricsSafe(t *testing.T) {
	t.Parallel()
	var m *Metrics
	m.IncGossipDrop()
}

// TestGossipDrop_EndToEndWiring exercises the full chain: a real
// *Cluster's slogAdapter receives a "handler queue full" log line via
// Write, parses it, and increments the Prometheus counter through the
// atomic metrics pointer wired by SetMetrics.
func TestGossipDrop_EndToEndWiring(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	// SetMetrics is called after New but before Join (production wiring
	// in engine.go:386-388). The adapter stores the pointer atomically.
	c.SetMetrics(m)

	// Write two "handler queue full" log lines to the adapter — this is
	// what memberlist's logging goroutine does in production.
	_, err = c.adapter.Write([]byte(
		"2026/07/03 23:15:00 [WARN] memberlist: handler queue full, dropping message 8\n"))
	require.NoError(t, err, "Write 1")
	_, err = c.adapter.Write([]byte(
		"2026/07/03 23:15:01 [WARN] memberlist: handler queue full, dropping message 8\n"))
	require.NoError(t, err, "Write 2")

	families, err := reg.Gather()
	require.NoError(t, err, "gather")
	for _, f := range families {
		if f.GetName() != "bouine_cluster_gossip_drops_total" {
			continue
		}
		require.Len(t, f.GetMetric(), 1)
		require.Equal(t, 2.0, f.GetMetric()[0].GetCounter().GetValue())
		return
	}
	t.Fatal("bouine_cluster_gossip_drops_total not registered")
}

// TestGossipDrop_BeforeSetMetricsNoPanic verifies that writing a
// "handler queue full" log line to the adapter before SetMetrics has
// been called does not panic (the metrics pointer is nil and handled
// gracefully). This mirrors the production window where memberlist
// starts logging inside Create before the engine calls SetMetrics.
func TestGossipDrop_BeforeSetMetricsNoPanic(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	// adapter.metrics is nil at this point — must not panic.
	_, err = c.adapter.Write([]byte(
		"2026/07/03 23:15:00 [WARN] memberlist: handler queue full, dropping message 8\n"))
	require.NoError(t, err, "Write")
}

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

// TestMergeRemoteState_PrunesDeadPeers feeds node-b the digest of node-a
// while node-b carries ghost peers that node-a does not know about — the
// remote hash differs, so the merge must replace the ring and drop them.
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
	buf := EncodeRingDigestState(remote)
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
	buf := EncodeRingDigestState(localDigest)

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

	buf := EncodeRingDigestState(remote)
	c2.MergeRemoteState(buf, false)

	require.Eventually(t, func() bool {
		return len(c2.Members()) == 2
	}, 2*time.Second, 50*time.Millisecond)
}

// TestMergeRemoteState_SameHashStillPrunesStalePeer verifies the digest
// shortcut no longer gates pruning: a single node with a stale peer in
// its ring, receiving its own digest (identical hashes), must still
// prune the stale peer and keep the live local member.
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
	buf := EncodeRingDigestState(local)

	c.MergeRemoteState(buf, false)

	members := c.Members()
	assert.Len(t, members, 1, "stale peer must be pruned even when digests match")
	assert.Equal(t, "solo-prune", members[0].Name)
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

func TestMergeRemoteState_BadFrameNoOp(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "bad-frame", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	c.MergeRemoteState([]byte("{not a binary frame}"), false)
	require.Len(t, c.Members(), 1)
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

func TestRing_Segments(t *testing.T) {
	t.Parallel()
	r := newRing(256)
	r.add("alpha", 256)
	r.add("beta", 256)
	segs := r.segments()
	require.Len(t, segs, 2)
	var total float64
	for _, s := range segs {
		total += s.Frac
	}
	assert.InDelta(t, 1.0, total, 0.01)
}

func TestRing_Segments_Empty(t *testing.T) {
	t.Parallel()
	r := newRing(256)
	assert.Nil(t, r.segments())
}

func TestCluster_RingSegments(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "ring-test", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	segs := c.RingSegments()
	require.Len(t, segs, 1)
	assert.Equal(t, "ring-test", segs[0].NodeName)
}

func TestCluster_Config(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "cfg-test", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	got := c.Config()
	assert.Equal(t, "cfg-test", got.NodeName)
	assert.Equal(t, "strong", got.Mode)
}

func TestCluster_Mode(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "mode-test", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	assert.Equal(t, "strong", c.Mode())
}

func TestMetrics_SetMode(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	m.SetMode("strong")
	m.SetMode("eventual")
	m.SetMode("unknown")
}

func TestMetrics_BroadcastFailuresCount(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	require.Equal(t, int64(0), m.BroadcastFailuresCount())
	m.IncBroadcastFailure("purge", "dial")
	m.IncBroadcastFailure("ban", "timeout")
	assert.Equal(t, int64(2), m.BroadcastFailuresCount())
}

func TestNotifyUpdate_DelegatesToNotifyJoin(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "notify-test", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	info := api.PeerInfo{Name: "new-node", Addr: "127.0.0.1:1234"}
	meta, _ := EncodePeerInfoMeta(info)

	node := &memberlist.Node{Name: "new-node", Meta: meta}
	c.NotifyUpdate(node)

	members := c.Members()
	found := false
	for _, m := range members {
		if m.Name == "new-node" {
			found = true
			break
		}
	}
	assert.True(t, found, "NotifyUpdate should add the peer")
}

func TestNodeMeta_RoundTrip(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "meta-test", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	meta := c.NodeMeta(512)
	require.NotEmpty(t, meta)

	info, err := DecodePeerInfoMeta(meta)
	require.NoError(t, err)
	assert.Equal(t, "meta-test", info.Name)
}

// TestMetrics_NilReceiverSafe exercises every Metrics method on a nil
// receiver: the engine wires metrics after New, so these must be no-ops
// during that window, not panics.
func TestMetrics_NilReceiverSafe(t *testing.T) {
	t.Parallel()
	var m *Metrics
	assert.Equal(t, int64(0), m.BroadcastFailuresCount())
	m.SetMode("strong")
	m.IncRingEmpty()
	m.IncGossipDrop()
	m.IncGossipInvalidation("purge")
	m.IncHTTPInvalidation("ban")
	m.IncBroadcastFailure("purge", "dial")
}
