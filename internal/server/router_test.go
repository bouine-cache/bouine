package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/valyala/fasthttp"
)

func ok200(body string) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString(body)
	}
}

func serveRoute(t *testing.T, rt *Router, method, host, path string) *fasthttp.RequestCtx {
	t.Helper()
	var ctx fasthttp.RequestCtx
	ctx.Request.Header.SetMethod(method)
	ctx.Request.SetRequestURI("http://" + host + path)
	ctx.Request.Header.SetHost(host)
	rt.ServeRequest(&ctx)
	return &ctx
}

func TestRouter_FirstMatchWins(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "/a", "", nil, ok200("first"))
	rt.AddRoute("", "/a", "", nil, ok200("second"))

	ctx := serveRoute(t, rt, "GET", "example.com", "/a/b")
	require.Equal(t, "first", string(ctx.Response.Body()))
}

func TestRouter_HostMatch(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("api.example.com", "", "", nil, ok200("api"))
	rt.AddRoute("", "", "", nil, ok200("default"))

	ctx := serveRoute(t, rt, "GET", "api.example.com", "/")
	require.Equal(t, "api", string(ctx.Response.Body()))
}

func TestRouter_HostWithPort(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("api.example.com", "", "", nil, ok200("api"))

	ctx := serveRoute(t, rt, "GET", "api.example.com:443", "/")
	require.Equal(t, "api", string(ctx.Response.Body()))
}

func TestRouter_NoRoute(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("only.com", "", "", nil, ok200("x"))

	ctx := serveRoute(t, rt, "GET", "other.com", "/")
	require.Equal(t, fasthttp.StatusNotFound, ctx.Response.StatusCode())
}

func TestRouter_PathPrefix(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "/api/", "", nil, ok200("api"))
	rt.AddRoute("", "/", "", nil, ok200("root"))

	ctx := serveRoute(t, rt, "GET", "example.com", "/api/v1/foo")
	require.Equal(t, "api", string(ctx.Response.Body()))

	ctx2 := serveRoute(t, rt, "GET", "example.com", "/other")
	require.Equal(t, "root", string(ctx2.Response.Body()))
}

func TestRouter_CatchAll(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "", "", nil, ok200("all"))

	ctx := serveRoute(t, rt, "GET", "example.com", "/anything")
	require.Equal(t, "all", string(ctx.Response.Body()))
}

func TestRouter_MethodMatch(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "/api/", "", []string{"GET", "HEAD"}, ok200("read"))
	rt.AddRoute("", "/api/", "", []string{"POST", "PUT"}, ok200("write"))

	ctx := serveRoute(t, rt, "GET", "example.com", "/api/v1/foo")
	require.Equal(t, "read", string(ctx.Response.Body()))

	ctx2 := serveRoute(t, rt, "POST", "example.com", "/api/v1/foo")
	require.Equal(t, "write", string(ctx2.Response.Body()))
}

func TestRouter_MethodNoMatch(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "/api/", "", []string{"GET"}, ok200("get-only"))

	ctx := serveRoute(t, rt, "DELETE", "example.com", "/api/v1/foo")
	require.Equal(t, fasthttp.StatusNotFound, ctx.Response.StatusCode())
}

func TestRouter_MethodFallthrough(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "/", "", []string{"GET", "HEAD"}, ok200("cached"))
	rt.AddRoute("", "/", "", nil, ok200("passthrough"))

	ctx := serveRoute(t, rt, "GET", "example.com", "/page")
	require.Equal(t, "cached", string(ctx.Response.Body()))

	ctx2 := serveRoute(t, rt, "POST", "example.com", "/page")
	require.Equal(t, "passthrough", string(ctx2.Response.Body()))
}

func TestRouter_NilMethodsMatchAll(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "/", "", nil, ok200("any"))

	for _, m := range []string{"GET", "HEAD", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"} {
		ctx := serveRoute(t, rt, m, "example.com", "/x")
		assert.Equal(t, "any", string(ctx.Response.Body()))
	}
}

func BenchmarkRouter_Match(b *testing.B) {
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "/api/", "api", nil, ok200("api"))
	rt.AddRoute("", "/static/", "static", nil, ok200("static"))
	rt.AddRoute("", "/", "root", nil, ok200("root"))

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var ctx fasthttp.RequestCtx
		ctx.Request.Header.SetMethod("GET")
		ctx.Request.SetRequestURI("http://example.com/api/v1/users")
		ctx.Request.Header.SetHost("example.com")
		rt.ServeRequest(&ctx)
	}
}
