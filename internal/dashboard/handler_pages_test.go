package dashboard

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/dashboard/insights"
	"github.com/bouine-cache/bouine/internal/dashboard/templates"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/origin"
	"github.com/bouine-cache/bouine/pkg/api"
)

func newTestHandlerWithRings() *Handler {
	rings := observability.NewRings("self")
	return &Handler{
		cfg: Config{
			Token:   "test",
			Rings:   rings,
			Logger:  observability.NoopLogger{},
			PeersFn: func() []api.PeerInfo { return []api.PeerInfo{{Name: "self"}} },
			StoreFn: func() api.Stats {
				return api.Stats{HotBytes: 100, HotEntries: 5, WarmBytes: 200, WarmEntries: 10, Evictions: 3}
			},
			HotMaxBytes:  1000,
			WarmMaxBytes: 2000,
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}
}

func TestHandler_Overview(t *testing.T) {
	t.Parallel()
	h := newTestHandlerWithRings()
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/?range=1h")
	h.overview(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
	assert.Contains(t, string(ctx.Response.Body()), "overview")
}

func TestHandler_Overview_WithConfig(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:  "test",
			Rings:  rings,
			Logger: observability.NoopLogger{},
			Config: &config.Config{
				Routes: []config.Route{{Name: "api", Pool: "origin1"}},
			},
			RingFn: func() []api.RingSegment { return []api.RingSegment{{NodeName: "self", Frac: 1.0}} },
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/")
	h.overview(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
}

func TestHandler_Performance(t *testing.T) {
	t.Parallel()
	h := newTestHandlerWithRings()
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/performance?range=24h")
	h.performance(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
	// Legacy 24h URLs fall back to the 6h view; the ring only holds 6h.
	require.Contains(t, string(ctx.Response.Body()), "latency over time (6h)")
}

func TestHandler_Routes(t *testing.T) {
	t.Parallel()
	h := newTestHandlerWithRings()
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/routes")
	h.routes(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
}

func TestHandler_Routes_WithConfig(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:  "test",
			Rings:  rings,
			Logger: observability.NoopLogger{},
			Config: &config.Config{
				Routes: []config.Route{{Name: "api", Pool: "origin1"}},
			},
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/routes")
	h.routes(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
}

func TestHandler_Invalidation(t *testing.T) {
	t.Parallel()
	h := newTestHandlerWithRings()
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/invalidation")
	h.invalidation(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
}

func TestHandler_Config(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:      "test",
			Rings:      rings,
			Logger:     observability.NoopLogger{},
			Config:     &config.Config{Routes: []config.Route{{Name: "api"}}},
			ConfigPath: "/etc/bouine/config.yaml",
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/config")
	h.config(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
}

func TestHandler_Config_NilConfig(t *testing.T) {
	t.Parallel()
	h := newTestHandlerWithRings()
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/config")
	h.config(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
}

func TestHandler_Insights(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:  "test",
			Rings:  rings,
			Logger: observability.NoopLogger{},
			Config: &config.Config{},
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/insights")
	h.insights(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
}

func TestHandler_Insights_WithAllClosures(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:   "test",
			Rings:   rings,
			Logger:  observability.NoopLogger{},
			PeersFn: func() []api.PeerInfo { return []api.PeerInfo{{Name: "self"}} },
			StoreFn: func() api.Stats { return api.Stats{HotBytes: 100} },
			PoolHealthFn: func() map[string][]origin.TargetStatus {
				return map[string][]origin.TargetStatus{"origin1": {{Healthy: true}}}
			},
			OriginHeaderAuditFn: func() map[string]observability.HeaderAuditSummary {
				return map[string]observability.HeaderAuditSummary{"origin1": {}}
			},
			VaryCapHitsFn:       func() int64 { return 5 },
			BroadcastFailuresFn: func() int64 { return 2 },
			CFPurgeSkippedFn:    func() int64 { return 1 },
			CFStatusFn: func() templates.CFStatusCard {
				return templates.CFStatusCard{Enabled: true, ZoneID: "zone1"}
			},
			Config: &config.Config{Routes: []config.Route{{Name: "api", Pool: "origin1"}}},
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/insights")
	h.insights(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
}

// TestHandler_CFStatusWired asserts the Cloudflare card the engine
// supplies actually reaches both surfaces that consume it: the
// invalidation page card (last error / last success / circuit / DLQ)
// and the insight engine input that drives ruleCDNLastError. Before
// this wiring the engine dropped every status field except
// enabled/zone/async/lag, so the CDN error insight could never fire.
func TestHandler_CFStatusWired(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:  "test",
			Rings:  rings,
			Logger: observability.NoopLogger{},
			CFStatusFn: func() templates.CFStatusCard {
				return templates.CFStatusCard{
					Enabled:       true,
					ZoneID:        "zone1",
					Async:         true,
					LastError:     "429 too many requests",
					LastSuccessAt: "2026-09-09T12:00:00Z",
					LastLagMs:     42,
					CircuitState:  "open",
					DLQDepth:      3,
				}
			},
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/invalidation")
	h.invalidation(ctx)
	body := string(ctx.Response.Body())
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
	assert.Contains(t, body, "429 too many requests")
	assert.Contains(t, body, "2026-09-09T12:00:00Z")
	assert.Contains(t, body, "open")
	assert.Contains(t, body, "3")

	merged, peers := h.agg.Collect(context.Background())
	data := h.collectInsightData(merged, peers)
	assert.Equal(t, "429 too many requests", data.CFStatus.LastError,
		"ruleCDNLastError input must see the last error")
}

// TestHandler_Insights_Polling asserts the insights page self-polls like
// the other live pages, and that the polling swap re-applies the
// operator's filter/focus instead of resetting the view.
func TestHandler_Insights_Polling(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:  "test",
			Rings:  rings,
			Logger: observability.NoopLogger{},
			Config: &config.Config{Routes: []config.Route{{Name: "api", Pool: "origin1"}}},
		},
		auth:          newSessionAuth("test"),
		agg:           NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
		insightEngine: insights.New(),
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/insights")
	h.insights(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())

	body := string(ctx.Response.Body())
	assert.Contains(t, body, `hx-get="/dashboard/insights"`, "insights page must self-poll")
	assert.Contains(t, body, "every 15s")
	assert.Contains(t, body, `hx-select="#insights-container"`)
	// Poll-safety: document keydown guarded, filter/focus state kept on
	// window and re-applied after each swap.
	assert.Contains(t, body, "__insightsKeyBound")
	assert.Contains(t, body, "__insightsState")
}

// TestHandler_PeerFetchStatsAvgLatency asserts the cluster page shows
// the average peer-fetch RPC latency: the engine adapter computes it
// from the fetcher's cumulative lat_sum/lat_n, which it previously
// discarded, leaving the "avg peer latency" row permanently empty.
func TestHandler_PeerFetchStatsAvgLatency(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:       "test",
			Rings:       rings,
			Logger:      observability.NoopLogger{},
			ClusterMeta: templates.ClusterMeta{Mode: "strong"},
			PeerFetchStatsFn: func() templates.PeerFetchStats {
				return templates.PeerFetchStats{
					HitsTotal:    7,
					MissesTotal:  3,
					AvgLatMs:     2.5,
					HopLimitHits: 1,
				}
			},
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/cluster")
	h.cluster(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
	body := string(ctx.Response.Body())
	assert.Contains(t, body, "2.50ms", "avg peer latency must render")
	assert.Contains(t, body, "peer hits (total)")
	assert.Contains(t, body, "peer misses (total)")
}

func TestHandler_APIPurge_NotConfigured(t *testing.T) {
	t.Parallel()
	h := newTestHandlerWithRings()
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/purge")
	h.apiPurge(ctx)
	assert.Contains(t, string(ctx.Response.Body()), "purge not configured")
}

func TestHandler_APIPurge_Valid(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:   "test",
			Rings:   rings,
			Logger:  observability.NoopLogger{},
			PurgeFn: func(_ context.Context, _ string) error { return nil },
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/purge")
	ctx.Request.Header.SetContentType("application/x-www-form-urlencoded")
	ctx.PostArgs().Set("url", "https://example.com/path")
	h.apiPurge(ctx)
	assert.Contains(t, string(ctx.Response.Body()), "purged")
}

func TestHandler_APIPurge_Error(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:   "test",
			Rings:   rings,
			Logger:  observability.NoopLogger{},
			PurgeFn: func(_ context.Context, _ string) error { return assertErr("fetch failed") },
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/purge")
	ctx.Request.Header.SetContentType("application/x-www-form-urlencoded")
	ctx.PostArgs().Set("url", "https://example.com/path")
	h.apiPurge(ctx)
	assert.Contains(t, string(ctx.Response.Body()), "fetch failed")
}

func TestHandler_APIPurge_JSONBody(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:   "test",
			Rings:   rings,
			Logger:  observability.NoopLogger{},
			PurgeFn: func(_ context.Context, _ string) error { return nil },
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/purge")
	ctx.Request.Header.SetContentType("application/json")
	ctx.Request.SetBody([]byte(`{"url":"https://example.com/path"}`))
	h.apiPurge(ctx)
	assert.Contains(t, string(ctx.Response.Body()), "purged")
}

func TestHandler_APIPurge_InvalidURL(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:   "test",
			Rings:   rings,
			Logger:  observability.NoopLogger{},
			PurgeFn: func(_ context.Context, _ string) error { return nil },
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/purge")
	h.apiPurge(ctx)
	assert.Contains(t, string(ctx.Response.Body()), "URL is required")
}

func TestHandler_APIBan_NotConfigured(t *testing.T) {
	t.Parallel()
	h := newTestHandlerWithRings()
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/ban")
	h.apiBan(ctx)
	assert.Contains(t, string(ctx.Response.Body()), "ban not configured")
}

func TestHandler_APIBan_Valid(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:  "test",
			Rings:  rings,
			Logger: observability.NoopLogger{},
			BanFn:  func(_ context.Context, _, _ string) (int, error) { return 5, nil },
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/ban")
	ctx.Request.Header.SetContentType("application/x-www-form-urlencoded")
	ctx.PostArgs().Set("host_regex", "example.com")
	ctx.PostArgs().Set("path_regex", "^/api/")
	h.apiBan(ctx)
	assert.Contains(t, string(ctx.Response.Body()), "banned")
}

func TestHandler_APIBan_BothEmpty(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:  "test",
			Rings:  rings,
			Logger: observability.NoopLogger{},
			BanFn:  func(_ context.Context, _, _ string) (int, error) { return 0, nil },
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/ban")
	h.apiBan(ctx)
	assert.Contains(t, string(ctx.Response.Body()), "provide at least one")
}

func TestHandler_APIBan_Error(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:  "test",
			Rings:  rings,
			Logger: observability.NoopLogger{},
			BanFn:  func(_ context.Context, _, _ string) (int, error) { return 0, assertErr("ban failed") },
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/ban")
	ctx.Request.Header.SetContentType("application/x-www-form-urlencoded")
	ctx.PostArgs().Set("host_regex", "example.com")
	h.apiBan(ctx)
	assert.Contains(t, string(ctx.Response.Body()), "ban failed")
}

func TestHandler_APIRefresh_NotConfigured(t *testing.T) {
	t.Parallel()
	h := newTestHandlerWithRings()
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/refresh")
	h.apiRefresh(ctx)
	assert.Contains(t, string(ctx.Response.Body()), "refresh not configured")
}

func TestHandler_APIRefresh_Valid(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:     "test",
			Rings:     rings,
			Logger:    observability.NoopLogger{},
			RefreshFn: func(_ context.Context, _ string) error { return nil },
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/refresh")
	ctx.Request.Header.SetContentType("application/x-www-form-urlencoded")
	ctx.PostArgs().Set("url", "https://example.com/path")
	h.apiRefresh(ctx)
	assert.Contains(t, string(ctx.Response.Body()), "refreshed")
}

func TestHandler_APIRefresh_Error(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:     "test",
			Rings:     rings,
			Logger:    observability.NoopLogger{},
			RefreshFn: func(_ context.Context, _ string) error { return assertErr("refresh failed") },
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/refresh")
	ctx.Request.Header.SetContentType("application/x-www-form-urlencoded")
	ctx.PostArgs().Set("url", "https://example.com/path")
	h.apiRefresh(ctx)
	assert.Contains(t, string(ctx.Response.Body()), "refresh failed")
}

func TestHandler_APIRefresh_JSONBody(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:     "test",
			Rings:     rings,
			Logger:    observability.NoopLogger{},
			RefreshFn: func(_ context.Context, _ string) error { return nil },
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/refresh")
	ctx.Request.Header.SetContentType("application/json")
	ctx.Request.SetBody([]byte(`{"url":"https://example.com/path"}`))
	h.apiRefresh(ctx)
	assert.Contains(t, string(ctx.Response.Body()), "refreshed")
}

func TestHandler_Render_Error(t *testing.T) {
	t.Parallel()
	h := newTestHandlerWithRings()
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/")
	h.render(ctx, errorComponent{})
	_ = string(ctx.Response.Body())
}

func TestHandler_CFStatusCard_WithFn(t *testing.T) {
	t.Parallel()
	h := &Handler{
		cfg: Config{
			CFStatusFn: func() templates.CFStatusCard {
				return templates.CFStatusCard{Enabled: true, ZoneID: "z1", Async: true}
			},
		},
	}
	card := h.cfStatusCard()
	require.NotNil(t, card)
	assert.Equal(t, "z1", card.ZoneID)
}

func TestHandler_BuildArchNodes(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Config: &config.Config{
				Routes: []config.Route{
					{Name: "api", Pool: "origin1"},
					{Name: "static", Pool: "origin2"},
				},
			},
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}
	peers := []PeerResult{{NodeName: "self"}}
	nodes := h.buildArchNodes(
		map[string][]origin.TargetStatus{"origin1": {{Healthy: true}, {Healthy: false}}},
		&templates.CFStatusCard{Enabled: true, ZoneID: "z1"},
		api.Stats{HotBytes: 100, WarmBytes: 200},
		peers,
	)
	assert.NotEmpty(t, nodes)
	assert.Equal(t, "client", nodes[0].ID)
	assert.Equal(t, "cdn", nodes[1].ID)
}

func TestHandler_BuildArchNodes_NoCF(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg:  Config{},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}
	nodes := h.buildArchNodes(nil, nil, api.Stats{}, nil)
	assert.NotEmpty(t, nodes)
	assert.Equal(t, "client", nodes[0].ID)
	assert.Equal(t, "bouine", nodes[1].ID)
}

func TestHandler_SidebarProps_WithRings(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{cfg: Config{Rings: rings}}
	_, _, peerCount, live := h.sidebarProps("6h")
	assert.Equal(t, 1, peerCount)
	assert.Equal(t, 1, live)
}

func TestHandler_StoreStats_WithStoreFn(t *testing.T) {
	t.Parallel()
	h := &Handler{
		cfg: Config{
			StoreFn: func() api.Stats {
				return api.Stats{HotBytes: 100, HotEntries: 5, WarmBytes: 200, WarmEntries: 10, Evictions: 3}
			},
			HotMaxBytes:  1000,
			WarmMaxBytes: 2000,
		},
	}
	hotBytes, hotEntries, hotMax, warmBytes, warmEntries, warmMax, evictions := h.storeStats()
	assert.Equal(t, int64(100), hotBytes)
	assert.Equal(t, int64(5), hotEntries)
	assert.Equal(t, int64(1000), hotMax)
	assert.Equal(t, int64(200), warmBytes)
	assert.Equal(t, int64(10), warmEntries)
	assert.Equal(t, int64(2000), warmMax)
	assert.Equal(t, int64(3), evictions)
}

func TestHandler_LayoutProps(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{cfg: Config{Rings: rings, Version: "v1.0.0"}}
	lp := h.layoutProps("overview", "Overview", "6h")
	assert.Equal(t, "overview", lp.Page)
	assert.Equal(t, "Overview", lp.PageTitle)
	assert.Equal(t, "self", lp.NodeName)
	assert.Equal(t, "v1.0.0", lp.Version)
	assert.Equal(t, "6h", lp.TimeRange)
}

func TestHandler_Cluster_WithRingFn(t *testing.T) {
	t.Parallel()
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:            "test",
			Rings:            rings,
			Logger:           observability.NoopLogger{},
			RingFn:           func() []api.RingSegment { return []api.RingSegment{{NodeName: "self", Frac: 1.0}} },
			PeerFetchStatsFn: func() templates.PeerFetchStats { return templates.PeerFetchStats{HitsTotal: 10} },
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", observability.NoopLogger{}),
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/cluster")
	h.cluster(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
}

// errorComponent is a templ.Component that always returns an error on Render.
type errorComponent struct{}

func (errorComponent) Render(_ context.Context, _ io.Writer) error {
	return assertErr("render error")
}

type errString string

func (e errString) Error() string { return string(e) }

func assertErr(s string) error { return errString(s) }
