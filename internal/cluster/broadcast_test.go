package cluster

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
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
	b.BroadcastPurge(context.Background(), testkey.Key(42), "")

	require.Len(t, received, 1)
	assert.Equal(t, testkey.Key(42), received[0].Key)
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

	require.Len(t, received, 1)
	assert.Equal(t, "example.com", received[0].Predicate.HostRegex)
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
	c.peers["node-0"] = &Member{Info: api.PeerInfo{
		Name:      "node-0",
		AdminAddr: srv.Listener.Addr().String(),
	}}
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Listener.Addr().String(),
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastPurge(context.Background(), testkey.Key(1), "")

	assert.Equal(t, 1, called)
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
	b.BroadcastPurge(context.Background(), testkey.Key(99), "/v")

	require.Equal(t, 0, httpCalled)
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
	b.BroadcastPurge(context.Background(), testkey.Key(7), "")

	require.Equal(t, 1, httpCalled)
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

	require.Equal(t, 0, httpCalled)
}

func TestBroadcastPurge_IncrementsBroadcastFailureCounter(t *testing.T) {
	t.Parallel()
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
	b.BroadcastPurge(context.Background(), testkey.Key(1), "")

	families, err := reg.Gather()
	require.NoError(t, err, "gather")
	var got float64
	for _, f := range families {
		if f.GetName() != "bouine_cluster_broadcast_failures_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			got += m.GetCounter().GetValue()
		}
	}
	assert.Equal(t, float64(1), got)
}

func TestBroadcastPurge_DialErrorIncrementsDial(t *testing.T) {
	t.Parallel()
	c := minimalCluster(t, "node-0")
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	c.metrics = m
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: "127.0.0.1:1",
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastPurge(context.Background(), testkey.Key(77), "")

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

func minimalCluster(_ *testing.T, _ string) *Cluster {
	return &Cluster{
		cfg:     Config{NodeName: "node-0", Mode: "strong"},
		peers:   make(map[string]*Member),
		ring:    newRing(256),
		logger:  observability.NoopLogger{},
		metrics: &Metrics{},
	}
}

func TestBroadcastPurge_NotCancelledByParentContext(t *testing.T) {
	t.Parallel()
	var received atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Listener.Addr().String(),
	}}

	b := NewBroadcaster(c, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	b.BroadcastPurge(ctx, testkey.Key(42), "")

	if got := received.Load(); got != 1 {
		t.Fatalf("expected 1 peer to receive purge despite cancelled parent ctx, got %d", got)
	}
}

func TestBroadcastBan_NotCancelledByParentContext(t *testing.T) {
	t.Parallel()
	var received atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Listener.Addr().String(),
	}}

	b := NewBroadcaster(c, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	b.BroadcastBan(ctx, api.BanExpr{HostRegex: "test\\.com"})

	if got := received.Load(); got != 1 {
		t.Fatalf("expected 1 peer to receive ban despite cancelled parent ctx, got %d", got)
	}
}

func TestBroadcaster_UsesHTTPWhenNoTLS(t *testing.T) {
	t.Parallel()
	var gotTLS bool
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		gotTLS = r.TLS != nil
	}))
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Listener.Addr().String(),
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastPurge(context.Background(), testkey.Key(1), "")

	if gotTLS {
		t.Fatal("expected plaintext HTTP with nil fetcher, got TLS")
	}
}

func TestBroadcaster_UsesHTTPSWhenFetcherHasTLS(t *testing.T) {
	t.Parallel()
	var gotTLS bool
	srv := httptest.NewTLSServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		gotTLS = r.TLS != nil
	}))
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Listener.Addr().String(),
	}}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // G402: test-only
	}
	fetcher := &PeerFetcher{
		client: &http.Client{Transport: transport},
		useTLS: true,
	}

	b := NewBroadcaster(c, fetcher)
	b.BroadcastPurge(context.Background(), testkey.Key(1), "")

	if !gotTLS {
		t.Fatal("expected HTTPS with TLS fetcher, got plaintext")
	}

	// Verify the broadcaster reuses the fetcher's transport (connection
	// pool sharing), not a standalone copy.
	if b.client.Transport != transport {
		t.Fatal("broadcaster must share the fetcher's transport, not create its own")
	}
}

func TestBroadcaster_SendsAuthToken(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		op   func(b *Broadcaster)
	}{
		{"purge", func(b *Broadcaster) { b.BroadcastPurge(context.Background(), testkey.Key(1), "") }},
		{"ban", func(b *Broadcaster) { b.BroadcastBan(context.Background(), api.BanExpr{HostRegex: "test\\.com"}) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var authHeader string
			srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				authHeader = r.Header.Get("Authorization")
			}))
			defer srv.Close()

			c := minimalCluster(t, "node-0")
			c.peers["node-1"] = &Member{Info: api.PeerInfo{
				Name:      "node-1",
				AdminAddr: srv.Listener.Addr().String(),
			}}

			b := NewBroadcaster(c, nil, "secret-token")
			tc.op(b)

			if authHeader != "Bearer secret-token" {
				t.Fatalf("expected Authorization header %q, got %q", "Bearer secret-token", authHeader)
			}
		})
	}
}
