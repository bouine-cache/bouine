package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// addSpec is the AddRouteSpec form of the router helpers: wildcard
// hosts and regex paths are only expressible through a RouteSpec.
func addSpec(t *testing.T, rt *Router, spec RouteSpec) {
	t.Helper()
	require.NoError(t, rt.AddRouteSpec(spec))
}

func TestRouter_WildcardHost(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	addSpec(t, rt, RouteSpec{
		Host: "*.staging.example.com", Label: "staging",
		Handler: ok200("staging"),
	})
	addSpec(t, rt, RouteSpec{Label: "root", Handler: ok200("root")})

	// Any host strictly under the suffix — one label or several.
	for _, host := range []string{
		"pr-42.staging.example.com",
		"a.b.staging.example.com",
		"pr-42.staging.example.com:8443", // port stripped before matching
	} {
		ctx := serveRoute(t, rt, "GET", host, "/anything")
		require.Equal(t, "staging", string(ctx.Response.Body()), host)
	}
	// Case-insensitive, like exact-host matching.
	ctx := serveRoute(t, rt, "GET", "PR-42.Staging.Example.COM", "/x")
	require.Equal(t, "staging", string(ctx.Response.Body()))

	// The bare suffix host itself never matches the wildcard...
	ctx = serveRoute(t, rt, "GET", "staging.example.com", "/x")
	require.Equal(t, "root", string(ctx.Response.Body()))
	// ...and neither does a host that merely ends with the same bytes.
	for _, host := range []string{
		"notstaging.example.com",
		"staging.example.com.evil.io",
	} {
		ctx := serveRoute(t, rt, "GET", host, "/x")
		require.Equal(t, "root", string(ctx.Response.Body()), host)
	}
}

func TestRouter_WildcardHostPrecedence(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	// Declaration order is precedence: exact first, then wildcard, then
	// catch-all — the documented ordering (issue #772).
	addSpec(t, rt, RouteSpec{Host: "api.staging.example.com", PathPrefix: "/", Label: "exact", Handler: ok200("exact")})
	addSpec(t, rt, RouteSpec{Host: "*.staging.example.com", PathPrefix: "/", Label: "wild", Handler: ok200("wild")})
	addSpec(t, rt, RouteSpec{Label: "root", Handler: ok200("root")})

	ctx := serveRoute(t, rt, "GET", "api.staging.example.com", "/x")
	require.Equal(t, "exact", string(ctx.Response.Body()))
	ctx = serveRoute(t, rt, "GET", "other.staging.example.com", "/x")
	require.Equal(t, "wild", string(ctx.Response.Body()))
	ctx = serveRoute(t, rt, "GET", "www.example.com", "/x")
	require.Equal(t, "root", string(ctx.Response.Body()))
}

func TestRouter_RegexPath(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	addSpec(t, rt, RouteSpec{
		Path: `^/[a-z]{2}-[a-z]{2}/l/campaign-.*$`, Label: "campaign",
		Handler: ok200("campaign"),
	})
	addSpec(t, rt, RouteSpec{PathPrefix: "/", Label: "root", Handler: ok200("root")})

	for _, path := range []string{
		"/fr-fr/l/campaign-summer",
		"/en-us/l/campaign-black-friday/extra",
		"/fr-fr/l/campaign-", // ".*" allows the empty tail
	} {
		ctx := serveRoute(t, rt, "GET", "www.example.com", path)
		require.Equal(t, "campaign", string(ctx.Response.Body()), path)
	}
	for _, path := range []string{
		"/fr-FR/l/campaign-x", // case-sensitive class, as written
		"/frfr/l/campaign-x",  // missing separator
		"/fr-fr/l/other-page", // same prefix, outside the class
	} {
		ctx := serveRoute(t, rt, "GET", "www.example.com", path)
		require.Equal(t, "root", string(ctx.Response.Body()), path)
	}
}

func TestRouter_RegexPathWithHostAndMethods(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	addSpec(t, rt, RouteSpec{
		Host:    "www.example.com",
		Path:    `^/[a-z]{2}-[a-z]{2}/l/.*$`,
		Methods: []string{"GET", "HEAD"},
		Label:   "landing",
		Handler: ok200("landing"),
	})
	addSpec(t, rt, RouteSpec{Label: "root", Handler: ok200("root")})

	ctx := serveRoute(t, rt, "GET", "www.example.com", "/fr-fr/l/x")
	require.Equal(t, "landing", string(ctx.Response.Body()))
	// Method outside the set falls through to the next route.
	ctx = serveRoute(t, rt, "POST", "www.example.com", "/fr-fr/l/x")
	require.Equal(t, "root", string(ctx.Response.Body()))
	// Host outside the predicate falls through too.
	ctx = serveRoute(t, rt, "GET", "shop.example.com", "/fr-fr/l/x")
	require.Equal(t, "root", string(ctx.Response.Body()))
}

func TestRouter_MixedExactWildcardRegexTable(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	addSpec(t, rt, RouteSpec{Host: "api.example.com", PathPrefix: "/v1/", Label: "api", Handler: ok200("api")})
	addSpec(t, rt, RouteSpec{Host: "*.staging.example.com", Label: "staging", Handler: ok200("staging")})
	addSpec(t, rt, RouteSpec{Path: `^/static/.*\.(css|js)$`, Label: "assets", Handler: ok200("assets")})
	addSpec(t, rt, RouteSpec{Label: "root", Handler: ok200("root")})

	tests := []struct {
		host, path, want string
	}{
		{"api.example.com", "/v1/users", "api"},
		{"api.example.com", "/v2/users", "root"},             // prefix must hold
		{"pr-1.staging.example.com", "/v1/users", "staging"}, // wildcard beats later routes
		{"www.example.com", "/static/app.css", "assets"},
		{"www.example.com", "/static/app.map", "root"}, // regex must hold
		{"www.example.com", "/anything", "root"},
	}
	for _, tt := range tests {
		ctx := serveRoute(t, rt, "GET", tt.host, tt.path)
		assert.Equal(t, tt.want, string(ctx.Response.Body()), tt.host+tt.path)
	}
}

func TestRouter_MatchByHostPath_ExtendedPredicates(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	addSpec(t, rt, RouteSpec{Host: "*.staging.example.com", PathPrefix: "/api/", Label: "staging-api", Handler: ok200("s")})
	addSpec(t, rt, RouteSpec{Path: `^/[a-z]{2}/l/campaign-.*$`, Label: "campaign", Handler: ok200("c")})

	// The admin key builder resolves the same routes the data plane
	// does — wildcard and regex included, port stripped, method-agnostic.
	assert.Equal(t, "staging-api", rt.MatchByHostPath("pr-9.staging.example.com:443", "/api/v1"))
	assert.Equal(t, "", rt.MatchByHostPath("staging.example.com", "/api/v1"))
	assert.Equal(t, "campaign", rt.MatchByHostPath("www.example.com", "/fr/l/campaign-x"))
	assert.Equal(t, "", rt.MatchByHostPath("www.example.com", "/fr/l/other"))
}

func TestRouter_AddRouteSpec_InvalidRegexRejected(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	err := rt.AddRouteSpec(RouteSpec{Path: `^/([unclosed$`, Label: "bad", Handler: ok200("x")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad")
	assert.Contains(t, err.Error(), "match.path")
	// The failed route is not registered: nothing matches it.
	assert.Empty(t, rt.routes)
}

func TestRouter_RouteLabelAuto_ExtendedPredicates(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	addSpec(t, rt, RouteSpec{Host: "*.staging.example.com", Label: "", Handler: ok200("s")})
	addSpec(t, rt, RouteSpec{Path: `^/l/.*$`, Label: "", Handler: ok200("l")})
	assert.Equal(t, "*.staging.example.com:", rt.routes[0].label)
	assert.Equal(t, `^/l/.*$`, rt.routes[1].label)
}

func TestRouter_ShadowedRoutes_CatchAllFirst(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "", "catch-all", "", nil, ok200("all"), nil)
	rt.AddRoute("", "/api/", "api", "", nil, ok200("api"), nil)
	addSpec(t, rt, RouteSpec{Host: "*.example.com", Label: "wild", Handler: ok200("w")})
	addSpec(t, rt, RouteSpec{Path: `^/x.*$`, Label: "re", Handler: ok200("r")})

	findings := rt.ShadowedRoutes()
	require.Len(t, findings, 3)
	for _, msg := range findings {
		assert.Contains(t, msg, `"catch-all"`)
	}
}

func TestRouter_ShadowedRoutes_HostAndPathForms(t *testing.T) {
	t.Parallel()
	t.Run("exact host shadowed by earlier wildcard", func(t *testing.T) {
		t.Parallel()
		rt := NewRouter(RouterConfig{})
		addSpec(t, rt, RouteSpec{Host: "*.staging.example.com", Label: "wild", Handler: ok200("w")})
		addSpec(t, rt, RouteSpec{Host: "pr-1.staging.example.com", Label: "pr1", Handler: ok200("p")})
		findings := rt.ShadowedRoutes()
		require.Len(t, findings, 1)
		assert.Contains(t, findings[0], `"pr1"`)
		assert.Contains(t, findings[0], `"wild"`)
	})
	t.Run("narrower wildcard shadowed by wider wildcard", func(t *testing.T) {
		t.Parallel()
		rt := NewRouter(RouterConfig{})
		addSpec(t, rt, RouteSpec{Host: "*.example.com", Label: "wide", Handler: ok200("w")})
		addSpec(t, rt, RouteSpec{Host: "*.pr.example.com", Label: "narrow", Handler: ok200("n")})
		require.Len(t, rt.ShadowedRoutes(), 1)
	})
	t.Run("prefix extension shadowed by shorter prefix", func(t *testing.T) {
		t.Parallel()
		rt := NewRouter(RouterConfig{})
		rt.AddRoute("", "/api/", "api", "", nil, ok200("a"), nil)
		rt.AddRoute("", "/api/v1/", "api-v1", "", nil, ok200("v"), nil)
		findings := rt.ShadowedRoutes()
		require.Len(t, findings, 1)
		assert.Contains(t, findings[0], `"api-v1"`)
	})
	t.Run("method subset shadowed by earlier superset", func(t *testing.T) {
		t.Parallel()
		rt := NewRouter(RouterConfig{})
		rt.AddRoute("", "/api/", "rw", "", []string{"GET", "HEAD"}, ok200("rw"), nil)
		rt.AddRoute("", "/api/", "ro", "", []string{"GET"}, ok200("ro"), nil)
		findings := rt.ShadowedRoutes()
		require.Len(t, findings, 1)
		assert.Contains(t, findings[0], `"ro"`)
	})
	t.Run("identical regex routes shadow", func(t *testing.T) {
		t.Parallel()
		rt := NewRouter(RouterConfig{})
		addSpec(t, rt, RouteSpec{Path: `^/x.*$`, Label: "first", Handler: ok200("f")})
		addSpec(t, rt, RouteSpec{Path: `^/x.*$`, Label: "second", Handler: ok200("s")})
		require.Len(t, rt.ShadowedRoutes(), 1)
	})
}

func TestRouter_ShadowedRoutes_NonShadowsStaySilent(t *testing.T) {
	t.Parallel()
	// Every pair here overlaps at most partially, or its relation is
	// undecidable (regex vs prefix): none may produce a finding.
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("api.example.com", "/v1/", "api", "", nil, ok200("a"), nil)
	rt.AddRoute("api.example.com", "/v2/", "api2", "", nil, ok200("b"), nil) // disjoint prefixes
	rt.AddRoute("", "/api/", "reads", "", []string{"GET", "HEAD"}, ok200("g"), nil)
	rt.AddRoute("", "/api/", "writes", "", []string{"POST"}, ok200("e"), nil) // methods escape the earlier method-set
	addSpec(t, rt, RouteSpec{Host: "*.example.com", Path: `^/api/.*$`, Label: "re", Handler: ok200("d")})
	addSpec(t, rt, RouteSpec{Path: `^/api/v[0-9]+/.*$`, Label: "re2", Handler: ok200("f")})
	assert.Empty(t, rt.ShadowedRoutes())
}

func TestRouter_ShadowedRoutes_NilWhenClean(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	assert.Nil(t, rt.ShadowedRoutes())
	rt.AddRoute("a.com", "/", "a", "", nil, ok200("a"), nil)
	assert.Nil(t, rt.ShadowedRoutes())
}

// TestRouter_ShadowedRoutes_StillMatchesFirst pins that shadow
// detection is reporting only: the router keeps serving with
// first-match precedence instead of rejecting the table.
func TestRouter_ShadowedRoutes_StillMatchesFirst(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "/", "first", "", nil, ok200("first"), nil)
	rt.AddRoute("", "/api/", "second", "", nil, ok200("second"), nil)
	require.NotEmpty(t, rt.ShadowedRoutes())

	ctx := serveRoute(t, rt, "GET", "example.com", "/api/x")
	require.Equal(t, "first", string(ctx.Response.Body()))
}
