package cmd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/runtime/shutdown"
	"github.com/bouine-cache/bouine/internal/runtime/supervised"
	"github.com/bouine-cache/bouine/internal/server"
	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/internal/testutil/poll"
	"github.com/bouine-cache/bouine/pkg/api"
)

// TestFastPathPoolAttribution is the end-to-end regression test for
// issue #696: with experimental.h1_fast_path enabled, a fast-path
// served hit must carry the route's configured upstream_pool, not
// "_default". It runs the real engine wiring (initSubsystems +
// buildDataPlane + startListeners with a bound HTTP listener), warms
// the cache over a real connection, then asserts attribution at three
// levels: the per-route handler the router holds, the routed wrapper
// the parser receives, and the metric series RecordHit produces.
func TestFastPathPoolAttribution(t *testing.T) {
	t.Parallel()
	originSrv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("Cache-Control", "max-age=60")
		ctx.Response.Header.Set("ETag", `"v1"`)
		ctx.SetStatusCode(fasthttp.StatusOK)
		_, _ = ctx.Write([]byte("body"))
	})
	defer originSrv.Close()

	e := &engine{
		cfg: &config.Config{
			// :0 — the bound address is read back from the listener
			// (rs.listeners), per the repo's no-fixed-port test rule.
			Listen: config.Listen{HTTP: "127.0.0.1:0"},
			// A zero budget would admit-then-evict every object (the
			// sweeper reclaims to perShardMax=0); the loader's GOMEMLIMIT
			// derivation never runs for hand-built configs.
			Storage: config.Storage{HotMaxBytes: 64 << 20},
			UpstreamPools: []config.UpstreamPool{
				{Name: "review-service", Targets: []string{originSrv.Addr}},
			},
			Routes: []config.Route{
				{Name: "api", Pool: "review-service", Cache: config.RouteCache{TTLDefault: 60 * time.Second}},
			},
			Experimental: config.ExperimentalConfig{H1FastPath: true},
		},
		logger:  newTestLogger(),
		metrics: observability.NewMetrics(),
	}
	seq := shutdown.NewSequencer(newTestLogger())
	rs, _, err := e.initSubsystems(context.Background(), seq)
	require.NoError(t, err)
	handler := e.buildDataPlane(rs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := supervised.NewGroup(ctx, newTestLogger())
	e.startListeners(g, handler, rs)
	require.NotEmpty(t, rs.listeners)
	// The listener stores its resolved address when Serve binds —
	// poll for it (the port must be read back, never fixed).
	var addr string
	poll.Eventually(t, 3*time.Second, 5*time.Millisecond, func() bool {
		addr = rs.listeners[0].Addr()
		return !strings.HasSuffix(addr, ":0")
	})
	require.NotContains(t, addr, ":0", "listener must have resolved its bound port")

	// Warm the cache over a real connection (a MISS fetches and stores
	// inside the streaming-tee writer, which requires a live client).
	resp, err := fastGet(t, "http://"+addr+"/api/x")
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode())
	require.Equal(t, "MISS", string(resp.Header.Peek("X-Cache")))
	fasthttp.ReleaseResponse(resp)

	// Second request over the wire: served by the H1 fast path (the
	// listener's parser) — the production path whose metrics the issue
	// is about.
	resp, err = fastGet(t, "http://"+addr+"/api/x")
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode())
	require.Equal(t, "HIT", string(resp.Header.Peek("X-Cache")), "second request must be a fast-path hit")
	fasthttp.ReleaseResponse(resp)

	// The per-route handler the router holds must attribute the pool.
	require.NotEmpty(t, rs.fastPathHandlers)
	require.NotNil(t, rs.router)
	fp := rs.fastPathHandlers[0]
	req := &api.RawRequest{Method: "GET", Path: "/api/x", Host: addr, Scheme: "http"}
	fResp, ok := fp.TryHit(req, time.Now())
	require.True(t, ok, "per-route fast path must hit after warm-up")
	assert.Equal(t, "review-service", fResp.Pool,
		"fast-path hits must carry the route's configured upstream_pool (issue #696)")
	assert.Equal(t, "HIT", fResp.CacheResult)
	fp.Release(fResp)

	// The routed wrapper (what the parser actually holds) must resolve
	// the same route and delegate — with the same pool attribution.
	rfp := server.NewRoutedFastPath(rs.router, fp)
	fResp, ok = rfp.TryHit(req, time.Now())
	require.True(t, ok)
	assert.Equal(t, "review-service", fResp.Pool)
	assert.Equal(t, "HIT", fResp.CacheResult)
	rfp.Release(fResp)
}

// TestFastPathPoolAttribution_Metrics checks the metric series the
// h1parser hook produces: RecordHit with the route's pool must land in
// the pre-resolved per-pool series, not the "_default" fallback — the
// exact hook call the parser makes for a fast-path hit.
func TestFastPathPoolAttribution_Metrics(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := observability.NewDataPlaneMetrics(reg)
	m.PreResolveRoutes([]string{"review-service"})

	m.RecordHit("review-service", "csr", "HIT", "cache", 200, 13, 500*time.Microsecond)

	families, err := reg.Gather()
	require.NoError(t, err)
	var found bool
	for _, mf := range families {
		if mf.GetName() != "bouine_requests_total" {
			continue
		}
		for _, mtr := range mf.GetMetric() {
			var pool, result string
			for _, lp := range mtr.GetLabel() {
				switch lp.GetName() {
				case "upstream_pool":
					pool = lp.GetValue()
				case "cache_result":
					result = lp.GetValue()
				}
			}
			if pool == "review-service" && result == "HIT" {
				found = true
			}
			assert.NotEqual(t, "_default", pool, "hits must not collapse into _default")
		}
	}
	assert.True(t, found, "review-service HIT series must exist")
}

// TestFastPathPoolAttribution_NoneEnabledRoute pins that a deployment
// without cache-enabled routes registers no fast paths at all — the
// store-level handler that attributed everything to "_default" is gone
// (issue #696's ghost-hit path: hits for requests the router would
// not serve).
func TestFastPathPoolAttribution_NoneEnabledRoute(t *testing.T) {
	t.Parallel()
	metrics := observability.NewMetrics()
	e := &engine{
		cfg: &config.Config{
			Experimental: config.ExperimentalConfig{H1FastPath: true},
		},
		logger:  newTestLogger(),
		metrics: metrics,
	}
	seq := shutdown.NewSequencer(newTestLogger())
	rs, _, err := e.initSubsystems(context.Background(), seq)
	require.NoError(t, err)
	_ = e.buildDataPlane(rs)
	assert.Empty(t, rs.fastPathHandlers,
		"routes without cache handlers must not register fast paths")
}
