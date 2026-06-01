package cluster

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/thylong/bouine/pkg/api"
)

func TestBroadcaster_BroadcastPurge(t *testing.T) {
	t.Parallel()
	var received []api.PurgeEvent
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/peer/purge" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		var evt api.PurgeEvent
		if err := json.NewDecoder(r.Body).Decode(&evt); err != nil {
			t.Errorf("decode: %v", err)
		}
		received = append(received, evt)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Listener.Addr().String(),
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastPurge(context.Background(), api.Key(42), "")

	if len(received) != 1 {
		t.Fatalf("expected 1 purge, got %d", len(received))
	}
	if received[0].Key != 42 {
		t.Errorf("key = %d, want 42", received[0].Key)
	}
}

func TestBroadcaster_BroadcastBan(t *testing.T) {
	t.Parallel()
	var received []api.BanEvent
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/peer/ban" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		var evt api.BanEvent
		if err := json.NewDecoder(r.Body).Decode(&evt); err != nil {
			t.Errorf("decode: %v", err)
		}
		received = append(received, evt)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Listener.Addr().String(),
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastBan(context.Background(), api.BanExpr{
		HostRegex: "example.com",
		CreatedAt: time.Now(),
	})

	if len(received) != 1 {
		t.Fatalf("expected 1 ban, got %d", len(received))
	}
	if received[0].Predicate.HostRegex != "example.com" {
		t.Errorf("host_regex = %q, want example.com", received[0].Predicate.HostRegex)
	}
}

func TestBroadcaster_SkipsSelf(t *testing.T) {
	t.Parallel()
	called := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called++
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	// Add self — should be skipped
	c.peers["node-0"] = &Member{Info: api.PeerInfo{
		Name:      "node-0",
		AdminAddr: srv.Listener.Addr().String(),
	}}
	// Add real peer
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Listener.Addr().String(),
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastPurge(context.Background(), api.Key(1), "")

	if called != 1 {
		t.Errorf("expected 1 call (self skipped), got %d", called)
	}
}

func TestBroadcastPurge_Eventual_NoHTTPFanout(t *testing.T) {
	t.Parallel()
	httpCalled := 0
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		httpCalled++
	}))
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.cfg.Mode = "eventual"
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Listener.Addr().String(),
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastPurge(context.Background(), api.Key(99), "/v")

	if httpCalled != 0 {
		t.Fatalf("eventual mode should not POST to peers, got %d HTTP calls", httpCalled)
	}
}

func TestBroadcastPurge_Strong_DoesHTTPFanout(t *testing.T) {
	t.Parallel()
	httpCalled := 0
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		httpCalled++
	}))
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.cfg.Mode = "strong"
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Listener.Addr().String(),
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastPurge(context.Background(), api.Key(7), "")

	if httpCalled != 1 {
		t.Fatalf("strong mode should POST to peer, got %d HTTP calls", httpCalled)
	}
}

func TestBroadcastReplicate_Full_EnqueuesGossip(t *testing.T) {
	t.Parallel()
	c := minimalCluster(t, "node-0")
	c.cfg.Mode = "full"

	b := NewBroadcaster(c, nil)
	obj := &api.Object{Key: api.Key(42)}
	b.BroadcastReplicate(context.Background(), obj)

	// Verify the gossip queue has a replication event.
	c.gossipMu.Lock()
	msgs := len(c.gossipQueue)
	c.gossipMu.Unlock()
	if msgs != 1 {
		t.Fatalf("expected 1 gossip message, got %d", msgs)
	}
}

func TestBroadcastReplicate_Eventual_Noop(t *testing.T) {
	t.Parallel()
	c := minimalCluster(t, "node-0")
	c.cfg.Mode = "eventual"

	b := NewBroadcaster(c, nil)
	b.BroadcastReplicate(context.Background(), &api.Object{Key: api.Key(42)})

	c.gossipMu.Lock()
	msgs := len(c.gossipQueue)
	c.gossipMu.Unlock()
	if msgs != 0 {
		t.Fatalf("eventual mode should not replicate, got %d gossip messages", msgs)
	}
}

func TestBroadcastReplicate_Strong_Noop(t *testing.T) {
	t.Parallel()
	c := minimalCluster(t, "node-0")
	c.cfg.Mode = "strong"

	b := NewBroadcaster(c, nil)
	b.BroadcastReplicate(context.Background(), &api.Object{Key: api.Key(42)})

	c.gossipMu.Lock()
	msgs := len(c.gossipQueue)
	c.gossipMu.Unlock()
	if msgs != 0 {
		t.Fatalf("strong mode should not replicate, got %d gossip messages", msgs)
	}
}

func TestBroadcastBan_Eventual_NoHTTPFanout(t *testing.T) {
	t.Parallel()
	httpCalled := 0
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		httpCalled++
	}))
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.cfg.Mode = "eventual"
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Listener.Addr().String(),
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastBan(context.Background(), api.BanExpr{HostRegex: "test\\.com"})

	if httpCalled != 0 {
		t.Fatalf("eventual mode should not POST bans, got %d HTTP calls", httpCalled)
	}
}

func TestBroadcastPurge_IncrementsBroadcastFailureCounter(t *testing.T) {
	t.Parallel()
	// Start a server that returns 500 to force a failure.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	c := minimalCluster(t, "node-0")
	c.metrics = m
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Listener.Addr().String(),
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastPurge(context.Background(), api.Key(1), "")

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var got float64
	for _, f := range families {
		if f.GetName() != "bouine_cluster_broadcast_failures_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			got += m.GetCounter().GetValue()
		}
	}
	if got != 1 {
		t.Errorf("expected 1 broadcast failure, got %v", got)
	}
}

func TestBroadcastPurge_DialErrorIncrementsDial(t *testing.T) {
	t.Parallel()
	// Use an unreachable address to get a dial error.
	c := minimalCluster(t, "node-0")
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	c.metrics = m
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: "127.0.0.1:1", // nothing listening
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastPurge(context.Background(), api.Key(77), "")

	families, _ := reg.Gather()
	var reason string
	for _, f := range families {
		if f.GetName() != "bouine_cluster_broadcast_failures_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "reason" {
					reason = lp.GetValue()
				}
			}
		}
	}
	if reason != "dial" && reason != "timeout" {
		t.Errorf("expected dial or timeout reason, got %q", reason)
	}
}

// minimalCluster builds a Cluster with no memberlist for unit testing.
func minimalCluster(_ *testing.T, _ string) *Cluster {
	return &Cluster{
		cfg:     Config{NodeName: "node-0", Mode: "strong"},
		peers:   make(map[string]*Member),
		ring:    newRing(256),
		logger:  nil,
		metrics: &Metrics{},
	}
}
