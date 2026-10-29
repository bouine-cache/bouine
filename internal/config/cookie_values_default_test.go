package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRouteDefaults_CookieValues_MergeMatrix pins the
// route_defaults.cache.key.cookie_values precedence on the resolved
// route: unset inherits wholesale, an own list replaces (never a
// union), an explicit empty list opts out (issue #870).
func TestRouteDefaults_CookieValues_MergeMatrix(t *testing.T) {
	t.Parallel()
	def := []string{"locale", "currency"}
	tests := []struct {
		name string
		set  func(*Route)
		want []string
	}{
		{
			name: "unset route inherits wholesale",
			set:  func(r *Route) {},
			want: def,
		},
		{
			name: "own list replaces, never a union",
			set: func(r *Route) {
				r.Cache.Key.CookieValues = []string{"theme"}
			},
			want: []string{"theme"},
		},
		{
			name: "explicit empty list opts out",
			set: func(r *Route) {
				r.Cache.Key.CookieValues = []string{}
			},
			want: []string{},
		},
		{
			name: "static route inherits too (key policy is wired on every cache handler)",
			set: func(r *Route) {
				r.Pool = ""
				r.Static = StaticConfig{Root: "/tmp"}
			},
			want: def,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := validBase()
			cfg.RouteDefaults.Cache.Key.CookieValues = def
			tt.set(&cfg.Routes[0])
			require.NoError(t, cfg.Validate())
			got := cfg.Routes[0].Cache.Key.CookieValues
			if tt.want == nil {
				require.Nil(t, got)
			} else {
				require.Equal(t, tt.want, got)
			}
		})
	}
}

// TestRouteDefaults_CookieValues_Idempotent pins that a second
// Validate call (which re-merges) does not stack the default onto a
// route that already inherited it.
func TestRouteDefaults_CookieValues_Idempotent(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.RouteDefaults.Cache.Key.CookieValues = []string{"locale"}
	require.NoError(t, cfg.Validate())
	require.NoError(t, cfg.Validate())
	require.Equal(t, []string{"locale"}, cfg.Routes[0].Cache.Key.CookieValues)
}

// TestRouteDefaults_CookieValues_InvalidDefaultStandsAlone pins that an
// invalid default (over the cap) produces one error at route_defaults'
// own path and no inheritance on any route.
func TestRouteDefaults_CookieValues_InvalidDefaultStandsAlone(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.RouteDefaults.Cache.Key.CookieValues = []string{"a", "b", "c", "d", "e"}
	err := cfg.Validate()
	require.Error(t, err)
	requireFieldError(t, err, "route_defaults.cache.key.cookie_values", "capped at 4 entries")
	assert.Nil(t, cfg.Routes[0].Cache.Key.CookieValues, "invalid default must not be inherited")
}

// TestRouteDefaults_CookieValues_YAMLEndToEnd pins the whole pipeline
// through Parse: the default decodes under route_defaults.cache.key,
// merges into the unset route, and validates; the explicit empty list
// decodes non-nil and opts out.
func TestRouteDefaults_CookieValues_YAMLEndToEnd(t *testing.T) {
	t.Parallel()
	yamlSrc := []byte(`
listen:
  admin: :9000
upstream_pools:
  - name: app
    targets: [a:1]
route_defaults:
  cache:
    key:
      cookie_values: [locale, currency]
routes:
  - name: r
    match: { host: example.com }
    pool: app
  - name: opted-out
    match: { host: opt.example.com }
    pool: app
    cache:
      key:
        cookie_values: []
`)
	cfg, err := Parse(yamlSrc)
	require.NoError(t, err)
	require.Len(t, cfg.Routes, 2)
	require.Equal(t, []string{"locale", "currency"}, cfg.Routes[0].Cache.Key.CookieValues)
	require.NotNil(t, cfg.Routes[1].Cache.Key.CookieValues, "explicit [] must decode non-nil to opt out")
	require.Empty(t, cfg.Routes[1].Cache.Key.CookieValues)
}

// TestRouteDefaults_CookieValues_UnknownKeyFieldRejected pins that
// route_defaults.cache.key accepts only cookie_values: any other key
// field fails strict decoding instead of silently not applying.
func TestRouteDefaults_CookieValues_UnknownKeyFieldRejected(t *testing.T) {
	t.Parallel()
	yamlSrc := []byte(`
listen:
  admin: :9000
upstream_pools:
  - name: app
    targets: [a:1]
route_defaults:
  cache:
    key:
      include_host: false
routes:
  - name: r
    pool: app
`)
	_, err := Parse(yamlSrc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "include_host")
}
