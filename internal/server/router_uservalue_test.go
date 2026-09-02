package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/pkg/header"
)

// TestRouter_SetsRouteUserValue is the phase-0 wiring proof for issue
// #607: the router must expose the matched route via the UserValue the
// metrics middleware reads, so routed traffic is attributed to its route
// label instead of collapsing onto _default.
func TestRouter_SetsRouteUserValue(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "/api/", "api", nil, ok200("api"))
	rt.AddRoute("", "/", "root", nil, ok200("root"))

	ctx := serveRoute(t, rt, "GET", "example.com", "/api/v1/foo")
	require.Equal(t, "api", string(ctx.Response.Body()))
	assert.Equal(t, "api", ctx.UserValue(header.XBouineRoute),
		"router must set the route UserValue for the metrics middleware")

	ctx2 := serveRoute(t, rt, "GET", "example.com", "/other")
	require.Equal(t, "root", string(ctx2.Response.Body()))
	assert.Equal(t, "root", ctx2.UserValue(header.XBouineRoute))
}

// TestRouter_NoRouteNoUserValue pins the no-match shape: without a route
// the middleware's _default fallback must apply, and the inbound spoofed
// header must not leak into the UserValue.
func TestRouter_NoRouteNoUserValue(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("only.com", "", "", nil, ok200("x"))

	ctx := serveRoute(t, rt, "GET", "other.com", "/")
	require.Equal(t, fasthttp.StatusNotFound, ctx.Response.StatusCode())
	assert.Nil(t, ctx.UserValue(header.XBouineRoute),
		"no-match must not set a route UserValue")
}
