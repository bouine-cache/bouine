package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRouteDefaults_DownstreamFreshness_MergeMatrix pins the
// route_defaults.cache.downstream_freshness precedence on the resolved
// routes: unset inherits, a route's own value replaces (passthrough is
// the explicit opt-out), and static routes inherit too — unlike
// bypass_on_user_agent the knob is consumed by every cache handler.
func TestRouteDefaults_DownstreamFreshness_MergeMatrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		route DownstreamFreshness
		want  DownstreamFreshness
	}{
		{
			name:  "unset inherits wholesale",
			route: "",
			want:  DownstreamFreshnessSync,
		},
		{
			name:  "explicit value replaces the default",
			route: DownstreamFreshnessPassthrough,
			want:  DownstreamFreshnessPassthrough,
		},
		{
			name:  "same value stays",
			route: DownstreamFreshnessSync,
			want:  DownstreamFreshnessSync,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := validBase()
			cfg.RouteDefaults.Cache.DownstreamFreshness = DownstreamFreshnessSync
			cfg.Routes[0].Cache.DownstreamFreshness = tt.route
			require.NoError(t, cfg.Validate())
			assert.Equal(t, tt.want, cfg.Routes[0].Cache.DownstreamFreshness)
		})
	}
}

// TestRouteDefaults_DownstreamFreshness_StaticRouteInherits pins the
// pool/static parity: the merged value is valid on a static route (the
// enum is validated by validateRouteCache on every route kind) and the
// cache handler consumes it there too.
func TestRouteDefaults_DownstreamFreshness_StaticRouteInherits(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.RouteDefaults.Cache.DownstreamFreshness = DownstreamFreshnessSync
	cfg.Routes[0].Pool = ""
	cfg.Routes[0].Static = StaticConfig{Root: "/tmp"}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, DownstreamFreshnessSync, cfg.Routes[0].Cache.DownstreamFreshness)
}

// TestRouteDefaults_DownstreamFreshness_EmptyDefaultIsNoOp pins that an
// unset default leaves routes untouched: there is nothing to inherit.
// A passthrough default, by contrast, merges — the resolved route then
// carries the value explicitly (see MergeMatrix above).
func TestRouteDefaults_DownstreamFreshness_EmptyDefaultIsNoOp(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	require.NoError(t, cfg.Validate())
	assert.Equal(t, DownstreamFreshness(""), cfg.Routes[0].Cache.DownstreamFreshness)
}

// TestRouteDefaults_DownstreamFreshness_InvalidStandsAlone pins that an
// invalid enum under route_defaults produces one error at its own path,
// and the broken default is not merged into any route.
func TestRouteDefaults_DownstreamFreshness_InvalidStandsAlone(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.RouteDefaults.Cache.DownstreamFreshness = "Synchronize"
	requireFieldError(t, cfg.Validate(), "route_defaults.cache.downstream_freshness", `must be "passthrough" or "sync"`)
	// The invalid default was not merged into the route.
	assert.Equal(t, DownstreamFreshness(""), cfg.Routes[0].Cache.DownstreamFreshness)
}

// TestRouteDefaults_DownstreamFreshness_Idempotent pins that a second
// Validate call does not re-merge or clobber the resolved routes.
func TestRouteDefaults_DownstreamFreshness_Idempotent(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.RouteDefaults.Cache.DownstreamFreshness = DownstreamFreshnessSync
	require.NoError(t, cfg.Validate())
	require.Equal(t, DownstreamFreshnessSync, cfg.Routes[0].Cache.DownstreamFreshness)
	require.NoError(t, cfg.Validate())
	assert.Equal(t, DownstreamFreshnessSync, cfg.Routes[0].Cache.DownstreamFreshness)
}

// TestRouteDefaults_DownstreamFreshness_YAMLEndToEnd pins the whole
// pipeline through Parse: the default decodes under route_defaults.cache,
// merges into unset routes, and a per-route value overrides it.
func TestRouteDefaults_DownstreamFreshness_YAMLEndToEnd(t *testing.T) {
	t.Parallel()
	yamlSrc := []byte(`
listen:
  admin: :9000
upstream_pools:
  - name: app
    targets: [a:1]
route_defaults:
  cache:
    downstream_freshness: sync
routes:
  - name: inherit
    pool: app
    cache: {ttl_default: 60s}
  - name: optout
    pool: app
    cache:
      ttl_default: 60s
      downstream_freshness: passthrough
`)
	cfg, err := Parse(yamlSrc)
	require.NoError(t, err)
	require.Len(t, cfg.Routes, 2)
	assert.Equal(t, DownstreamFreshnessSync, cfg.Routes[0].Cache.DownstreamFreshness,
		"unset route must inherit the default")
	assert.Equal(t, DownstreamFreshnessPassthrough, cfg.Routes[1].Cache.DownstreamFreshness,
		"route value must replace the default")
}

// TestRouteDefaults_DownstreamFreshness_OtherCacheFieldsRejected pins
// that route_defaults.cache still accepts only the designed merge
// surface: any other cache field fails strict decoding instead of
// silently not applying.
func TestRouteDefaults_DownstreamFreshness_OtherCacheFieldsRejected(t *testing.T) {
	t.Parallel()
	yamlSrc := []byte(`
listen:
  admin: :9000
upstream_pools:
  - name: app
    targets: [a:1]
route_defaults:
  cache:
    ttl_default: 60s
routes:
  - name: r
    pool: app
`)
	_, err := Parse(yamlSrc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ttl_default")
}
