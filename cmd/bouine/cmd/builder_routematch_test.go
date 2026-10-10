package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/origin"
	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/pkg/header"
)

// buildTestRouter assembles a minimal engine around one origin and its
// route table, mirroring the wiring TestPolicyForURL uses: the pools
// resolve, so every route registers.
func buildTestRouter(t *testing.T, log observability.Logger, routes ...config.Route) *runState {
	t.Helper()
	originSrv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("Cache-Control", "max-age=60")
		ctx.SetStatusCode(fasthttp.StatusOK)
	})
	t.Cleanup(originSrv.Close)

	e := &engine{
		cfg: &config.Config{
			UpstreamPools: []config.UpstreamPool{
				{Name: "app", Targets: []string{originSrv.Addr}},
			},
			Routes: routes,
		},
		logger:  log,
		metrics: observability.NewMetrics(),
	}
	store, err := e.buildStore(nil, nil, nil)
	require.NoError(t, err)
	m := origin.RegisterMetrics(e.metrics.Registry)
	pools, err := e.buildPools(m)
	require.NoError(t, err)
	rs := &runState{
		store:     store,
		pools:     pools,
		dpMetrics: observability.NewDataPlaneMetrics(e.metrics.Registry),
	}
	router := e.buildRouter(rs)
	require.NotNil(t, router)
	rs.router = router
	return rs
}

func serveHTTP(t *testing.T, rs *runState, host, path string) *fasthttp.RequestCtx {
	t.Helper()
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	// Origin-form request-target plus Host header, the way the wire
	// delivers requests: an absolute-form URI here would leak into the
	// origin-bound request and the pool client would dial it.
	ctx.Request.SetRequestURI(path)
	ctx.Request.Header.SetHost(host)
	rs.router.ServeRequest(ctx)
	return ctx
}

// TestBuildRouter_WiresExtendedPredicates proves the builder lowers
// wildcard-host and regex-path routes onto the router (issue #772) and
// they resolve end-to-end on the slow path, with first-match precedence
// over a later catch-all.
func TestBuildRouter_WiresExtendedPredicates(t *testing.T) {
	t.Parallel()
	rs := buildTestRouter(t, newTestLogger(),
		config.Route{
			Name: "staging",
			Pool: "app",
			Match: config.RouteMatch{
				Host: "*.staging.example.com",
			},
			Cache: config.RouteCache{Enabled: boolPtr(false)},
		},
		config.Route{
			Name: "campaign",
			Pool: "app",
			Match: config.RouteMatch{
				Host: "www.example.com",
				Path: `^/[a-z]{2}-[a-z]{2}/l/campaign-.*$`,
			},
		},
		config.Route{Name: "root", Pool: "app"},
	)
	require.Len(t, rs.handlers, 3)

	ctx := serveHTTP(t, rs, "pr-42.staging.example.com", "/anything")
	assert.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
	assert.Equal(t, "staging", ctx.UserValue(header.XBouineRoute))

	ctx = serveHTTP(t, rs, "www.example.com", "/fr-fr/l/campaign-summer")
	assert.Equal(t, "campaign", ctx.UserValue(header.XBouineRoute))

	// Outside the regex class, still on www: the catch-all takes it.
	ctx = serveHTTP(t, rs, "www.example.com", "/fr-fr/l/other")
	assert.Equal(t, "root", ctx.UserValue(header.XBouineRoute))

	// The admin plane resolves the same routes (purge key building).
	assert.Equal(t, "staging", rs.router.MatchByHostPath("pr-42.staging.example.com", "/x"))
	assert.Equal(t, "campaign", rs.router.MatchByHostPath("www.example.com", "/fr-fr/l/campaign-x"))
	assert.Equal(t, "root", rs.router.MatchByHostPath("www.example.com", "/other"))
}

// TestBuildRouter_LogsShadowedRoutes proves the wiring, not just the
// detector: a config whose later route is fully shadowed by an earlier
// one must surface at Error level at boot while boot still proceeds
// with declaration-order precedence (issue #772, mirroring the
// traffic-class report of ADR-0047).
func TestBuildRouter_LogsShadowedRoutes(t *testing.T) {
	t.Parallel()
	log := &captureLogger{}
	rs := buildTestRouter(t, log,
		config.Route{Name: "catch-all", Pool: "app"},
		config.Route{Name: "api", Pool: "app", Match: config.RouteMatch{PathPrefix: "/api/"}},
	)
	require.Len(t, rs.handlers, 2)

	require.Len(t, log.errors, 1)
	assert.Contains(t, log.errors[0], `"api"`)
	assert.Contains(t, log.errors[0], `"catch-all"`)
	assert.Contains(t, log.errors[0], "declaration order is precedence")

	// Soft failure: the router stays fully functional and keeps
	// first-match precedence — /api/x lands on the earlier route.
	ctx := serveHTTP(t, rs, "example.com", "/api/v1")
	assert.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
	assert.Equal(t, "catch-all", ctx.UserValue(header.XBouineRoute))
}

// TestBuildRouter_CleanRouteTableLogsNoShadowFindings pins the quiet
// path: exact/wildcard/regex routes that only partially overlap log no
// Error records at boot.
func TestBuildRouter_CleanRouteTableLogsNoShadowFindings(t *testing.T) {
	t.Parallel()
	log := &captureLogger{}
	rs := buildTestRouter(t, log,
		config.Route{Name: "api", Pool: "app", Match: config.RouteMatch{Host: "api.example.com", PathPrefix: "/v1/"}},
		config.Route{Name: "staging", Pool: "app", Match: config.RouteMatch{Host: "*.staging.example.com"}},
		config.Route{Name: "campaign", Pool: "app", Match: config.RouteMatch{Path: `^/[a-z]{2}/l/campaign-.*$`}},
		config.Route{Name: "root", Pool: "app"},
	)
	require.Len(t, rs.handlers, 4)
	assert.Empty(t, log.errors)
}
