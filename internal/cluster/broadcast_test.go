package cluster

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/thylong/bouine/pkg/api"
)

func TestBroadcaster_BroadcastPurge(t *testing.T) {
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

// minimalCluster builds a Cluster with no memberlist for unit testing.
func minimalCluster(_ *testing.T, nodeName string) *Cluster {
	return &Cluster{
		cfg:    Config{NodeName: nodeName},
		peers:  make(map[string]*Member),
		ring:   newRing(256),
		logger: nil,
	}
}
