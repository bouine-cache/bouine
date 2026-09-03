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

// TestRouter_UserValues is the wiring proof: the router exposes the
// matched route's label (for the dashboard rings) and its upstream pool
// (for the upstream_pool metric label) via the UserValues the metrics
// middleware reads. Pool-less routes and no-match traffic must leave
// the pool UserValue unset so the middleware's _default fallback
// applies and route labels never leak into upstream_pool.
func TestRouter_UserValues(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "/api/", "api", "app", nil, ok200("api"))
	rt.AddRoute("", "/s/", "static-route", "", nil, ok200("s"))
	rt.AddRoute("only.com", "", "host-route", "only-pool", nil, ok200("x"))
	rt.AddRoute("", "/", "root", "edge", nil, ok200("root"))

	tests := []struct {
		name       string
		host, path string
		wantStatus int
		wantRoute  any
		wantPool   any
	}{
		{"pooled route", "example.com", "/api/v1/foo", 200, "api", "app"},
		{"pool-less route", "example.com", "/s/x", 200, "static-route", nil},
		{"other pooled route", "example.com", "/other", 200, "root", "edge"},
		{"no match", "example.com", "/nope", 404, nil, nil},
	}
	rtNoMatch := NewRouter(RouterConfig{})
	for _, tt := range tests {
		rt2 := rt
		if tt.wantStatus == 404 {
			rt2 = rtNoMatch
		}
		ctx := serveRoute(t, rt2, "GET", tt.host, tt.path)
		require.Equal(t, tt.wantStatus, ctx.Response.StatusCode(), tt.name)
		assert.Equal(t, tt.wantRoute, ctx.UserValue(header.XBouineRoute), tt.name+": route UserValue")
		assert.Equal(t, tt.wantPool, ctx.UserValue(header.XBouinePool), tt.name+": pool UserValue")
	}
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
