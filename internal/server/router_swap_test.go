package server

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

func hitHandler(body string) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) { ctx.SetBodyString(body) }
}

// TestRouterSetTableSwapUnderTraffic: concurrent matchRoute readers
// during SetTable swaps — issue #951: one atomic load per request,
// in-flight requests keep their *routeEntry.
func TestRouterSetTableSwapUnderTraffic(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	require.NoError(t, rt.AddRouteSpec(RouteSpec{Host: "a.example.com", PathPrefix: "/v1", Label: "a", Handler: hitHandler("a")}))
	rt.Freeze()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					re := rt.matchRoute("a.example.com", "/v1/x", "GET")
					require.NotNil(t, re)
				}
			}
		}()
	}
	for i := range 100 {
		tb := NewTableBuilder()
		label := "a"
		if i%2 == 1 {
			label = "a2"
		}
		require.NoError(t, tb.WithIdentity("name:a").Add(RouteSpec{Host: "a.example.com", PathPrefix: "/v1", Label: label, Handler: hitHandler(label)}))
		rt.SetTable(tb.Table())
	}
	close(stop)
	wg.Wait()
	assert.Equal(t, 1, rt.RouteCount())
}

// TestRouterSetTableRecomputesShadows: shadow findings move into the
// snapshot and are recomputed per swap (issue #951 proposal 1).
func TestRouterSetTableRecomputesShadows(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	require.NoError(t, rt.AddRouteSpec(RouteSpec{PathPrefix: "/", Label: "catch", Handler: hitHandler("c")}))
	require.NoError(t, rt.AddRouteSpec(RouteSpec{PathPrefix: "/api", Label: "api", Handler: hitHandler("a")}))
	rt.Freeze()
	assert.Len(t, rt.ShadowedRoutes(), 1)

	tb := NewTableBuilder()
	require.NoError(t, tb.WithIdentity("name:api").Add(RouteSpec{PathPrefix: "/api", Label: "api", Handler: hitHandler("a")}))
	rt.SetTable(tb.Table())
	assert.Empty(t, rt.ShadowedRoutes(), "swapped table has no catch-all, so no shadow")
}
