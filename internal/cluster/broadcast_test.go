package cluster

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestBroadcaster_BroadcastPurge(t *testing.T) {
	t.Parallel()
	var received []api.PurgeEvent
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/peer/purge" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		evt, err := DecodePurgeHTTP(body)
		if err != nil {
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
		body, _ := io.ReadAll(r.Body)
		evt, err := DecodeBanHTTP(body)
		if err != nil {
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

func TestBroadcastReplicate_Full_SendsHTTP(t *testing.T) {
	t.Parallel()

	var receivedBodies [][]byte
	var receivedHeaders http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/peer/replicate" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		receivedBodies = append(receivedBodies, body)
		receivedHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.cfg.Mode = "full"
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Listener.Addr().String(),
	}}

	b := NewBroadcaster(c, nil)
	obj := &api.Object{Key: api.Key(42), StatusCode: 200, Body: []byte("hello")}
	b.BroadcastReplicate(context.Background(), obj)

	// Wait for the async goroutine to complete.
	b.waitForReplicationGoroutines()

	if len(receivedBodies) != 1 {
		t.Fatalf("expected 1 HTTP POST, got %d", len(receivedBodies))
	}
	if receivedHeaders.Get("Content-Type") != "application/octet-stream" {
		t.Errorf("Content-Type = %q, want application/octet-stream", receivedHeaders.Get("Content-Type"))
	}
	if receivedHeaders.Get(header.BouineIssuer) != "node-0" {
		t.Errorf("Bouine-Issuer = %q, want node-0", receivedHeaders.Get(header.BouineIssuer))
	}

	// Verify the gossip queue is NOT used for replication.
	c.gossipMu.Lock()
	msgs := len(c.gossipQueue)
	c.gossipMu.Unlock()
	if msgs != 0 {
		t.Fatalf("replication should not use gossip queue, got %d messages", msgs)
	}

	// Verify the body is a valid storage.EncodeObject blob.
	decoded, err := storage.DecodeObject(receivedBodies[0])
	if err != nil {
		t.Fatalf("decode replicated body: %v", err)
	}
	if decoded.Key != api.Key(42) {
		t.Errorf("decoded key = %d, want 42", decoded.Key)
	}
	if string(decoded.Body) != "hello" {
		t.Errorf("decoded body = %q, want %q", decoded.Body, "hello")
	}
}

func TestBroadcastReplicate_Eventual_Noop(t *testing.T) {
	t.Parallel()
	httpCalled := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpCalled = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.cfg.Mode = "eventual"
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Listener.Addr().String(),
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastReplicate(context.Background(), &api.Object{Key: api.Key(42)})
	b.waitForReplicationGoroutines()

	if httpCalled {
		t.Fatal("eventual mode should not replicate via HTTP")
	}
}

func TestBroadcastReplicate_Strong_Noop(t *testing.T) {
	t.Parallel()
	httpCalled := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpCalled = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.cfg.Mode = "strong"
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Listener.Addr().String(),
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastReplicate(context.Background(), &api.Object{Key: api.Key(42)})
	b.waitForReplicationGoroutines()

	if httpCalled {
		t.Fatal("strong mode should not replicate via HTTP")
	}
}

func TestBroadcastReplicate_Full_BodyCopiedNotAliased(t *testing.T) {
	t.Parallel()

	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.cfg.Mode = "full"
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Listener.Addr().String(),
	}}

	b := NewBroadcaster(c, nil)

	originalBody := []byte("the quick brown fox jumps over the lazy dog")
	obj := &api.Object{
		Key:           api.Key(42),
		StatusCode:    200,
		Body:          originalBody,
		Header:        header.FromHTTP(http.Header{"X-Test": []string{"v1"}}),
		SurrogateKeys: []string{"tag-a", "tag-b"},
	}
	b.BroadcastReplicate(context.Background(), obj)
	b.waitForReplicationGoroutines()

	if receivedBody == nil {
		t.Fatal("expected HTTP POST body, got nil")
	}

	// Decode the binary body and verify the copy is independent of the caller's slices.
	decoded, err := storage.DecodeObject(receivedBody)
	if err != nil {
		t.Fatalf("decode replicated body: %v", err)
	}
	if string(decoded.Body) != string(originalBody) {
		t.Fatalf("body mismatch after encode: got %q want %q", decoded.Body, originalBody)
	}
	if decoded.Header.Get("X-Test") != "v1" {
		t.Fatalf("header mismatch: got %q", decoded.Header.Get("X-Test"))
	}

	// Mutating the caller's body and header after BroadcastReplicate returns
	// must not change the already-sent bytes. The defensive copy inside the
	// function guarantees this even if the caller's Body aliases a sync.Pool
	// buffer that could be reused.
	for i := range originalBody {
		originalBody[i] = 'X'
	}
	obj.Header.Set("X-Test", "v2")
	obj.SurrogateKeys[0] = "mutated"

	// Re-decode the already-received body — it must not have changed.
	decoded2, err := storage.DecodeObject(receivedBody)
	if err != nil {
		t.Fatalf("re-decode after mutation: %v", err)
	}
	if string(decoded2.Body) != "the quick brown fox jumps over the lazy dog" {
		t.Fatalf("sent payload observed caller body mutation: got %q", decoded2.Body)
	}
	if decoded2.Header.Get("X-Test") != "v1" {
		t.Fatalf("sent payload observed header mutation: got %q", decoded2.Header.Get("X-Test"))
	}
	if len(decoded2.SurrogateKeys) != 2 ||
		decoded2.SurrogateKeys[0] != "tag-a" ||
		decoded2.SurrogateKeys[1] != "tag-b" {
		t.Fatalf("sent payload observed surrogate-keys mutation: got %v", decoded2.SurrogateKeys)
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
		logger:  observability.NoopLogger{},
		metrics: &Metrics{},
	}
}

// waitForReplicationGoroutines blocks until all in-flight replication
// goroutines have completed. Uses the WaitGroup tracking in Broadcaster.
func (b *Broadcaster) waitForReplicationGoroutines() {
	b.replWG.Wait()
}
