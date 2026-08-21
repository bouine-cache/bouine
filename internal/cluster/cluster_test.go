package cluster

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/client_golang/prometheus"

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
	m.IncGossipQueueDropped()
	m.SetGossipQueueDepth(42)
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

// TestQueueBroadcast_CapsGossipQueue verifies that QueueBroadcast drops
// new messages when the gossip queue is full (drop-newest policy) and
// increments the gossip_queue_dropped counter. Without the cap, the
// queue grows unboundedly under purge storms (issue #297).
func TestQueueBroadcast_CapsGossipQueue(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	cfg.GossipQueueDepth = 4
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	c.SetMetrics(m)

	msg := []byte("x")
	for range 10 {
		c.QueueBroadcast(msg)
	}

	require.Equal(t, 4, len(c.gossipQueue), "queue should be capped at GossipQueueDepth")

	families, err := reg.Gather()
	require.NoError(t, err, "gather")
	for _, f := range families {
		if f.GetName() != "bouine_cluster_gossip_queue_dropped_total" {
			continue
		}
		require.Len(t, f.GetMetric(), 1, "dropped counter should exist")
		require.Equal(t, 6.0, f.GetMetric()[0].GetCounter().GetValue(),
			"6 messages should have been dropped (10 enqueued, cap 4)")
		return
	}
	t.Fatal("bouine_cluster_gossip_queue_dropped_total not registered")
}

// TestQueueBroadcast_DefaultGossipQueueDepth verifies that the default
// gossip queue depth is applied when GossipQueueDepth is zero.
func TestQueueBroadcast_DefaultGossipQueueDepth(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	require.Equal(t, defaultGossipQueueDepth, c.cfg.GossipQueueDepth)
}

// TestQueueBroadcast_EventualModeSendsBestEffort verifies that in
// eventual mode, QueueBroadcast fires SendBestEffort to peers even when
// the queue is full — the direct send is the primary delivery path and
// must not be skipped on overflow.
func TestQueueBroadcast_EventualModeSendsBestEffort(t *testing.T) {
	t.Parallel()
	cfg1 := defaultConfig(t, "node1", "127.0.0.1:17930")
	cfg1.Mode = "eventual"
	cfg1.GossipQueueDepth = 2
	c1, err := New(cfg1)
	require.NoError(t, err, "c1")
	defer func() { _ = c1.Leave(t.Context()) }()

	cfg2 := defaultConfig(t, "node2", "127.0.0.1:17931")
	cfg2.Mode = "eventual"
	c2, err := New(cfg2)
	require.NoError(t, err, "c2")
	defer func() { _ = c2.Leave(t.Context()) }()

	_, err = c2.Join([]string{"127.0.0.1:17930"})
	require.NoError(t, err, "join")

	// Wait for both nodes to see each other.
	for range 50 {
		if len(c1.ml.Members()) == 2 && len(c2.ml.Members()) == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.Len(t, c1.ml.Members(), 2, "nodes should discover each other")

	var received atomic.Int32
	c2.SetInvalidator(Invalidator{
		PurgeFn: func(_ context.Context, _ api.PurgeEvent) error {
			received.Add(1)
			return nil
		},
	})

	// Enqueue more than the cap — all should still be delivered via
	// SendBestEffort even though the gossip queue drops the excess.
	evt := api.PurgeEvent{Key: testkey.Key(1), Issuer: "node1", Seq: 1}
	msg, err := EncodePurgeGossip(evt)
	require.NoError(t, err, "encode")
	for range 5 {
		c1.QueueBroadcast(msg)
	}

	// Wait for delivery via SendBestEffort.
	require.Eventually(t, func() bool {
		return received.Load() > 0
	}, 2*time.Second, 10*time.Millisecond, "purge should be delivered via SendBestEffort")

	// Queue should be capped.
	c1.gossipMu.Lock()
	require.Equal(t, 2, len(c1.gossipQueue), "queue should be capped at GossipQueueDepth")
	c1.gossipMu.Unlock()
}

// TestGetBroadcasts_UpdatesDepthGauge verifies that GetBroadcasts
// updates the gossip_queue_depth gauge to reflect the remaining queue
// depth after draining.
func TestGetBroadcasts_UpdatesDepthGauge(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	cfg.GossipQueueDepth = 8
	c, err := New(cfg)
	require.NoError(t, err, "New")
	defer func() { _ = c.Leave(t.Context()) }()

	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	c.SetMetrics(m)

	msg := []byte("hello")
	for range 5 {
		c.QueueBroadcast(msg)
	}
	// Drain 3 messages (each is 5 bytes + overhead).
	out := c.GetBroadcasts(0, 15)
	require.Len(t, out, 3, "should drain 3 messages")

	families, err := reg.Gather()
	require.NoError(t, err, "gather")
	for _, f := range families {
		if f.GetName() != "bouine_cluster_gossip_queue_depth" {
			continue
		}
		require.Len(t, f.GetMetric(), 1, "depth gauge should exist")
		require.Equal(t, 2.0, f.GetMetric()[0].GetGauge().GetValue(),
			"depth gauge should show 2 remaining after draining 3 of 5")
		return
	}
	t.Fatal("bouine_cluster_gossip_queue_depth not registered")
}

// TestNew_NegativeGossipQueueDepthRejected verifies that a negative
// GossipQueueDepth is rejected at construction time.
func TestNew_NegativeGossipQueueDepthRejected(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	cfg.GossipQueueDepth = -1
	_, err := New(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "GossipQueueDepth")
}

// TestNew_GossipQueueDepthExceedsUpperBoundRejected verifies that
// GossipQueueDepth above MaxGossipQueueDepth is rejected.
func TestNew_GossipQueueDepthExceedsUpperBoundRejected(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	cfg.GossipQueueDepth = MaxGossipQueueDepth + 1
	_, err := New(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "GossipQueueDepth")
	require.Contains(t, err.Error(), "must be <=")
}
