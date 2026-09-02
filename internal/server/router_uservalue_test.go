package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

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
