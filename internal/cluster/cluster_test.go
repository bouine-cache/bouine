package cluster

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/bouine-cache/bouine/internal/observability"
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
		key := api.Key(uint64(i) * step)
		owners[r.get(key)]++
	}
	for _, name := range []string{"alpha", "beta", "gamma"} {
		if owners[name] == 0 {
			t.Errorf("node %s got no keys in 1000-key distribution (dist: %v)", name, owners)
		}
	}
}

func TestRing_RemoveRedistributes(t *testing.T) {
	t.Parallel()
	r := newRing(64)
	r.add("a", 64)
	r.add("b", 64)

	key := api.Key(12345678)
	owner := r.get(key)

	r.remove(owner)
	newOwner := r.get(key)
	if newOwner == owner {
		t.Fatalf("after removing owner %s, key still routes to same node", owner)
	}
	if newOwner == "" {
		t.Fatal("after remove, key routes to empty node")
	}
}

func TestRing_Digest_Changes(t *testing.T) {
	t.Parallel()
	r := newRing(16)
	r.add("node1", 16)
	d1 := r.digest()

	r.add("node2", 16)
	d2 := r.digest()

	if d1.Hash == d2.Hash {
		t.Fatal("digest should change when ring membership changes")
	}
	if d2.Size != 2 {
		t.Fatalf("size = %d, want 2", d2.Size)
	}
}

func TestRing_SingleNode(t *testing.T) {
	t.Parallel()
	r := newRing(64)
	r.add("only", 64)
	for i := range 10 {
		if r.get(api.Key(i)) != "only" {
			t.Fatalf("single node should own all keys")
		}
	}
}

func TestCluster_LocalMode(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = c.Leave(t.Context()) }()

	members := c.Members()
	if len(members) != 1 {
		t.Fatalf("expected 1 member (self), got %d", len(members))
	}
	if members[0].Name != "local" {
		t.Fatalf("member name = %q", members[0].Name)
	}
	key := api.Key(999)
	if !c.IsLocal(key) {
		t.Fatal("single-node cluster should own all keys")
	}
}

func TestCluster_TwoNodeJoin(t *testing.T) {
	t.Parallel()
	c1, err := New(defaultConfig(t, "node1", "127.0.0.1:17900"))
	if err != nil {
		t.Fatalf("c1: %v", err)
	}
	defer func() { _ = c1.Leave(t.Context()) }()

	c2, err := New(defaultConfig(t, "node2", "127.0.0.1:17901"))
	if err != nil {
		t.Fatalf("c2: %v", err)
	}
	defer func() { _ = c2.Leave(t.Context()) }()

	if _, err := c2.Join([]string{"127.0.0.1:17900"}); err != nil {
		t.Fatalf("join: %v", err)
	}

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
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = c.Leave(t.Context()) }()

	var called atomic.Int32
	c.SetInvalidator(Invalidator{
		PurgeFn: func(_ context.Context, _ api.PurgeEvent) error {
			called.Add(1)
			return nil
		},
	})

	evt := api.PurgeEvent{Key: 42, VaryKey: "v1", Issuer: "local"}
	msg, _ := EncodePurgeGossip(evt)
	c.NotifyMsg(msg)

	if got := called.Load(); got != 1 {
		t.Fatalf("PurgeFn called %d times, want 1", got)
	}
}

func TestNotifyMsg_BanEvent(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
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

	if got := called.Load(); got != 1 {
		t.Fatalf("BanFn called %d times, want 1", got)
	}
}

func TestNotifyMsg_ReplicationEvent(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = c.Leave(t.Context()) }()

	var called atomic.Int32
	c.SetReplicator(Replicator{
		StoreObject: func(_ context.Context, _ *api.Object) error {
			called.Add(1)
			return nil
		},
	})

	evt := api.ReplicationEvent{Type: api.GossipTypeReplication, Method: "GET", Issuer: "local"}
	msg, _ := json.Marshal(evt)
	c.NotifyMsg(msg)

	if got := called.Load(); got != 1 {
		t.Fatalf("StoreObject called %d times, want 1", got)
	}
}

func TestNotifyMsg_ReplicationTakesPrecedenceOverPurge(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = c.Leave(t.Context()) }()

	var purgeCalled, replCalled atomic.Int32
	c.SetInvalidator(Invalidator{
		PurgeFn: func(_ context.Context, _ api.PurgeEvent) error {
			purgeCalled.Add(1)
			return nil
		},
	})
	c.SetReplicator(Replicator{
		StoreObject: func(_ context.Context, _ *api.Object) error {
			replCalled.Add(1)
			return nil
		},
	})

	// A Type-based replication event should dispatch to the replication handler
	// even when both Invalidator and Replicator are configured.
	evt := api.ReplicationEvent{Type: api.GossipTypeReplication, Method: "GET", Issuer: "local"}
	msg, _ := json.Marshal(evt)
	c.NotifyMsg(msg)

	if purgeCalled.Load() != 0 {
		t.Fatal("PurgeFn should not be called for replication event")
	}
	if replCalled.Load() != 1 {
		t.Fatal("StoreObject should be called once")
	}
}

func TestNotifyMsg_MalformedPayload(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
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
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = c.Leave(t.Context()) }()

	// Should not panic when no invalidator or replicator is set.
	evt := api.PurgeEvent{Key: 42}
	msg, _ := EncodePurgeGossip(evt)
	c.NotifyMsg(msg)
}

func TestNotifyMsg_PurgeCtxHasDeadline(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	cfg.GossipApplyTimeout = 50 * time.Millisecond
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = c.Leave(t.Context()) }()

	var got atomic.Pointer[context.Context]
	c.SetInvalidator(Invalidator{
		PurgeFn: func(ctx context.Context, _ api.PurgeEvent) error {
			got.Store(&ctx)
			return nil
		},
	})
	evt := api.PurgeEvent{Key: 7, Issuer: "local"}
	msg, _ := EncodePurgeGossip(evt)
	c.NotifyMsg(msg)

	ctx := *got.Load()
	if ctx == nil {
		t.Fatal("PurgeFn not called")
	}
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("context passed to PurgeFn has no deadline")
	}
}

func TestNotifyMsg_BanCtxHasDeadline(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
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
	if ctx == nil {
		t.Fatal("BanFn not called")
	}
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("context passed to BanFn has no deadline")
	}
}

func TestNotifyMsg_ReplicationCtxHasDeadline(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = c.Leave(t.Context()) }()

	var got atomic.Pointer[context.Context]
	c.SetReplicator(Replicator{
		StoreObject: func(ctx context.Context, _ *api.Object) error {
			got.Store(&ctx)
			return nil
		},
	})
	evt := api.ReplicationEvent{Type: api.GossipTypeReplication, Method: "GET", Issuer: "local"}
	msg, _ := json.Marshal(evt)
	c.NotifyMsg(msg)

	ctx := *got.Load()
	if ctx == nil {
		t.Fatal("StoreObject not called")
	}
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("context passed to StoreObject has no deadline")
	}
}

func TestNotifyMsg_DefaultApplyTimeout(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = c.Leave(t.Context()) }()

	if c.cfg.GossipApplyTimeout != 100*time.Millisecond {
		t.Fatalf("default GossipApplyTimeout = %v, want 100ms", c.cfg.GossipApplyTimeout)
	}
}

func TestNotifyMsg_PurgeTimeoutAbortsApply(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "local", "127.0.0.1:0")
	cfg.GossipApplyTimeout = 10 * time.Millisecond
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = c.Leave(t.Context()) }()

	c.SetInvalidator(Invalidator{
		PurgeFn: func(ctx context.Context, _ api.PurgeEvent) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})
	evt := api.PurgeEvent{Key: 1, Issuer: "local"}
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
	if err != nil {
		t.Fatalf("New: %v", err)
	}
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
	evt := api.PurgeEvent{Key: 1, Issuer: "local"}
	msg, _ := EncodePurgeGossip(evt)
	c.NotifyMsg(msg)

	metrics, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range metrics {
		if mf.GetName() != "bouine_cluster_invalidations_gossip_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetValue() == "purge" {
					if m.GetCounter().GetValue() != 0 {
						t.Fatalf("invalidation counter = %v, want 0 on failed apply", m.GetCounter().GetValue())
					}
				}
			}
		}
	}
}
