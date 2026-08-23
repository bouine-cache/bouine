package cluster

import (
	"context"
	"crypto/tls"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/internal/testutil/tlsutil"
	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

func TestBroadcaster_BroadcastPurge(t *testing.T) {
	t.Parallel()
	var received []api.PurgeEvent
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		if string(ctx.Path()) != "/v1/peer/purge" {
			t.Errorf("unexpected path: %s", string(ctx.Path()))
		}
		evt, err := DecodePurgeHTTP(ctx.PostBody())
		if err != nil {
			t.Errorf("decode: %v", err)
		}
		received = append(received, evt)
		ctx.SetStatusCode(fasthttp.StatusOK)
	})
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Addr,
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastPurge(context.Background(), testkey.Key(42), "")

	require.Len(t, received, 1)
	assert.Equal(t, testkey.Key(42), received[0].Key)
}

func TestBroadcaster_BroadcastBan(t *testing.T) {
	t.Parallel()
	var received []api.BanEvent
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		if string(ctx.Path()) != "/v1/peer/ban" {
			t.Errorf("unexpected path: %s", string(ctx.Path()))
		}
		evt, err := DecodeBanHTTP(ctx.PostBody())
		if err != nil {
			t.Errorf("decode: %v", err)
		}
		received = append(received, evt)
		ctx.SetStatusCode(fasthttp.StatusOK)
	})
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Addr,
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
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		called++
		ctx.SetStatusCode(fasthttp.StatusOK)
	})
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.peers["node-0"] = &Member{Info: api.PeerInfo{
		Name:      "node-0",
		AdminAddr: srv.Addr,
	}}
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Addr,
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastPurge(context.Background(), testkey.Key(1), "")

	assert.Equal(t, 1, called)
}

func TestBroadcastPurge_Eventual_NoHTTPFanout(t *testing.T) {
	t.Parallel()
	httpCalled := 0
	srv := fasthttptest.NewServer(t, func(_ *fasthttp.RequestCtx) {
		httpCalled++
	})
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.cfg.Mode = "eventual"
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Addr,
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastPurge(context.Background(), testkey.Key(99), "/v")

	require.Equal(t, 0, httpCalled)
}

func TestBroadcastPurge_Strong_DoesHTTPFanout(t *testing.T) {
	t.Parallel()
	httpCalled := 0
	srv := fasthttptest.NewServer(t, func(_ *fasthttp.RequestCtx) {
		httpCalled++
	})
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.cfg.Mode = "strong"
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Addr,
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastPurge(context.Background(), testkey.Key(7), "")

	require.Equal(t, 1, httpCalled)
}

func TestBroadcastBan_Eventual_NoHTTPFanout(t *testing.T) {
	t.Parallel()
	httpCalled := 0
	srv := fasthttptest.NewServer(t, func(_ *fasthttp.RequestCtx) {
		httpCalled++
	})
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.cfg.Mode = "eventual"
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Addr,
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastBan(context.Background(), api.BanExpr{HostRegex: "test\\.com"})

	require.Equal(t, 0, httpCalled)
}

func TestBroadcastPurge_IncrementsBroadcastFailureCounter(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
	})
	defer srv.Close()

	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	c := minimalCluster(t, "node-0")
	c.metrics = m
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Addr,
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
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		received.Add(1)
		ctx.SetStatusCode(fasthttp.StatusOK)
	})
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Addr,
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
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		received.Add(1)
		ctx.SetStatusCode(fasthttp.StatusOK)
	})
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Addr,
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
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		gotTLS = ctx.IsTLS()
	})
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Addr,
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
	srv := fasthttptest.NewTLSServer(t, func(ctx *fasthttp.RequestCtx) {
		gotTLS = ctx.IsTLS()
	}, tlsutil.ServerConfig(t))
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Addr,
	}}

	fetcher := &PeerFetcher{
		useTLS:    true,
		tlsConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // G402: test-only
	}

	b := NewBroadcaster(c, fetcher)
	b.BroadcastPurge(context.Background(), testkey.Key(1), "")

	if !gotTLS {
		t.Fatal("expected HTTPS with TLS fetcher, got plaintext")
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
			srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
				authHeader = string(ctx.Request.Header.Peek("Authorization"))
			})
			defer srv.Close()

			c := minimalCluster(t, "node-0")
			c.peers["node-1"] = &Member{Info: api.PeerInfo{
				Name:      "node-1",
				AdminAddr: srv.Addr,
			}}

			b := NewBroadcaster(c, nil, "secret-token")
			tc.op(b)

			if authHeader != "Bearer secret-token" {
				t.Fatalf("expected Authorization header %q, got %q", "Bearer secret-token", authHeader)
			}
		})
	}
}

func TestBroadcaster_BroadcastRefresh(t *testing.T) {
	t.Parallel()
	var received []api.RefreshEvent
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		if string(ctx.Path()) != "/v1/peer/refresh" {
			t.Errorf("unexpected path: %s", string(ctx.Path()))
		}
		evt, err := DecodeRefreshHTTP(ctx.PostBody())
		if err != nil {
			t.Errorf("decode: %v", err)
		}
		received = append(received, evt)
		ctx.SetStatusCode(fasthttp.StatusOK)
	})
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Addr,
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastRefresh(context.Background(), testkey.Key(42))

	require.Len(t, received, 1)
	assert.Equal(t, testkey.Key(42), received[0].Key)
	assert.Equal(t, "node-0", received[0].Issuer)
}

func TestBroadcastRefresh_Eventual_NoHTTPFanout(t *testing.T) {
	t.Parallel()
	called := 0
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		called++
		ctx.SetStatusCode(fasthttp.StatusOK)
	})
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.cfg.Mode = "eventual"
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Addr,
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastRefresh(context.Background(), testkey.Key(1))

	require.Equal(t, 0, called, "eventual mode should not use HTTP fan-out")
}

func TestBroadcastRefresh_Strong_DoesHTTPFanout(t *testing.T) {
	t.Parallel()
	called := 0
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		called++
		ctx.SetStatusCode(fasthttp.StatusOK)
	})
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.cfg.Mode = "strong"
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Addr,
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastRefresh(context.Background(), testkey.Key(1))

	require.Equal(t, 1, called, "strong mode should use HTTP fan-out")
}

func TestBroadcastRefresh_SkipsSelf(t *testing.T) {
	t.Parallel()
	called := 0
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		called++
		ctx.SetStatusCode(fasthttp.StatusOK)
	})
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.peers["node-0"] = &Member{Info: api.PeerInfo{
		Name:      "node-0",
		AdminAddr: srv.Addr,
	}}
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Addr,
	}}

	b := NewBroadcaster(c, nil)
	b.BroadcastRefresh(context.Background(), testkey.Key(1))

	require.Equal(t, 1, called, "should skip self, only contact node-1")
}

func TestBroadcastRefresh_NotCancelledByParentContext(t *testing.T) {
	t.Parallel()
	var hits atomic.Int32
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		hits.Add(1)
		ctx.SetStatusCode(fasthttp.StatusOK)
	})
	defer srv.Close()

	c := minimalCluster(t, "node-0")
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Addr,
	}}

	b := NewBroadcaster(c, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	b.BroadcastRefresh(ctx, testkey.Key(1))
	require.Equal(t, int32(1), hits.Load(), "refresh broadcast must not be cancelled by parent context")
}
