package server

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/header"
)

// TestRouter_SetsRouteUserValues is the wiring proof: the router must
// expose the matched route's label (for the dashboard rings) and its
// upstream pool (for the Prometheus upstream_pool metric label) via the
// UserValues the metrics middleware reads, so routed traffic is
// attributed instead of collapsing onto _default.
func TestRouter_SetsRouteUserValues(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "/api/", "api", "app", nil, ok200("api"))
	rt.AddRoute("", "/", "root", "edge", nil, ok200("root"))

	ctx := serveRoute(t, rt, "GET", "example.com", "/api/v1/foo")
	require.Equal(t, "api", string(ctx.Response.Body()))
	assert.Equal(t, "api", ctx.UserValue(header.XBouineRoute),
		"router must set the route UserValue for the dashboard rings")
	assert.Equal(t, "app", ctx.UserValue(header.XBouinePool),
		"router must set the pool UserValue for the metrics middleware")

	ctx2 := serveRoute(t, rt, "GET", "example.com", "/other")
	require.Equal(t, "root", string(ctx2.Response.Body()))
	assert.Equal(t, "root", ctx2.UserValue(header.XBouineRoute))
	assert.Equal(t, "edge", ctx2.UserValue(header.XBouinePool))
}

// TestRouter_NoRouteNoUserValue pins the no-match shape: without a route
// the middleware's _default fallback must apply, and the inbound spoofed
// headers must not leak into the UserValues.
func TestRouter_NoRouteNoUserValue(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("only.com", "", "", "only-pool", nil, ok200("x"))

	ctx := serveRoute(t, rt, "GET", "other.com", "/")
	require.Equal(t, fasthttp.StatusNotFound, ctx.Response.StatusCode())
	assert.Nil(t, ctx.UserValue(header.XBouineRoute),
		"no-match must not set a route UserValue")
	assert.Nil(t, ctx.UserValue(header.XBouinePool),
		"no-match must not set a pool UserValue")
}

// TestRouter_PoolLessRouteLeavesPoolUnset pins the pool-less shape: a
// route without an upstream pool (static-file routes) must not set the
// pool UserValue at all, so the metrics middleware keeps it on the
// _default Prometheus bucket instead of leaking the route label into the
// upstream_pool label space.
func TestRouter_PoolLessRouteLeavesPoolUnset(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "/s/", "static-route", "", nil, ok200("s"))

	ctx := serveRoute(t, rt, "GET", "example.com", "/s/x")
	require.Equal(t, "s", string(ctx.Response.Body()))
	assert.Equal(t, "static-route", ctx.UserValue(header.XBouineRoute),
		"rings must still see the per-route label")
	assert.Nil(t, ctx.UserValue(header.XBouinePool),
		"pool-less route must not set the pool UserValue")
}

// TestRouter_UpstreamPoolLabelStaysBounded is the end-to-end regression
// for the static-route leak: upstream_pool values on Prometheus must be
// configured pool names plus "_default" only, no matter how route labels
// are named. Runs the real router under the real metrics middleware.
func TestRouter_UpstreamPoolLabelStaysBounded(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	dm := observability.NewDataPlaneMetrics(reg)
	dm.PreResolveRoutes([]string{"proxy-pool"})

	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "/p/", "proxy-route", "proxy-pool", nil, ok200("p"))
	rt.AddRoute("", "/s/", "static-route", "", nil, ok200("s"))
	rt.AddRoute("", "/u/", "", "", nil, ok200("u"))

	mw := dm.FastHTTPMiddleware(rt.ServeRequest)
	for _, path := range []string{"/p/x", "/s/x", "/u/x"} {
		ctx := &fasthttp.RequestCtx{}
		ctx.Request.SetRequestURI(path)
		mw(ctx)
	}

	mfs, err := reg.Gather()
	require.NoError(t, err)
	pools := map[string]bool{}
	for _, mf := range mfs {
		if mf.GetName() != "bouine_requests_total" {
			continue
		}
		for _, met := range mf.GetMetric() {
			for _, l := range met.GetLabel() {
				if l.GetName() == "upstream_pool" {
					pools[l.GetValue()] = true
				}
			}
		}
	}
	assert.Equal(t, map[string]bool{"proxy-pool": true, "_default": true},
		pools, "upstream_pool must contain only configured pools and _default")
}
