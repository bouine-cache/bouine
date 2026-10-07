package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_BypassOnUserAgent_ValidYAML(t *testing.T) {
	t.Parallel()
	yamlSrc := `
upstream_pools:
  - name: app
    targets: [a:1]
routes:
  - match: { host: example.com }
    pool: app
    cache:
      bypass_on_user_agent:
        - ShoppingFeedBot
        - "*Googlebot*"
`
	cfg, err := Parse([]byte(yamlSrc))
	require.NoError(t, err, "unexpected error")
	require.Len(t, cfg.Routes, 1)
	assert.Equal(t, []string{"ShoppingFeedBot", "*Googlebot*"}, cfg.Routes[0].Cache.BypassOnUserAgent)
	require.NoError(t, cfg.Validate())
}

func TestParse_BypassOnUserAgent_DefaultEmpty(t *testing.T) {
	t.Parallel()
	yamlSrc := `
upstream_pools:
  - name: app
    targets: [a:1]
routes:
  - match: { host: example.com }
    pool: app
`
	cfg, err := Parse([]byte(yamlSrc))
	require.NoError(t, err, "unexpected error")
	require.Len(t, cfg.Routes, 1)
	assert.Empty(t, cfg.Routes[0].Cache.BypassOnUserAgent, "bypass_on_user_agent must default to empty (off)")
	require.NoError(t, cfg.Validate())
}

func TestValidate_BypassOnUserAgent_Rejected(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		patterns    []string
		pathSuffix  string
		msgContains string
	}{
		{
			name:        "empty entry",
			patterns:    []string{"Bot", "  "},
			pathSuffix:  "bypass_on_user_agent[1]",
			msgContains: "non-empty",
		},
		{
			name:        "lone star",
			patterns:    []string{"*"},
			pathSuffix:  "bypass_on_user_agent[0]",
			msgContains: "matches every request",
		},
		{
			name:        "adjacent stars",
			patterns:    []string{"Bot**1"},
			pathSuffix:  "bypass_on_user_agent[0]",
			msgContains: "empty wildcard",
		},
		{
			name:        "question mark metachar",
			patterns:    []string{"Bot?"},
			pathSuffix:  "bypass_on_user_agent[0]",
			msgContains: "only supported wildcard",
		},
		{
			name:        "bracket metachar",
			patterns:    []string{"[Bb]ot"},
			pathSuffix:  "bypass_on_user_agent[0]",
			msgContains: "only supported wildcard",
		},
		{
			name:        "backslash metachar",
			patterns:    []string{`\*Bot`},
			pathSuffix:  "bypass_on_user_agent[0]",
			msgContains: "only supported wildcard",
		},
		{
			name:        "non-ascii",
			patterns:    []string{"Böt"},
			pathSuffix:  "bypass_on_user_agent[0]",
			msgContains: "printable ASCII",
		},
		{
			name:        "oversized pattern",
			patterns:    []string{strings.Repeat("a", 257)},
			pathSuffix:  "bypass_on_user_agent[0]",
			msgContains: "capped at 256 bytes",
		},
		{
			name:        "exact duplicate",
			patterns:    []string{"*Bot*", "*Bot*"},
			pathSuffix:  "bypass_on_user_agent[1]",
			msgContains: "duplicate",
		},
		{
			name:        "case-insensitive duplicate",
			patterns:    []string{"*Bot*", "*bot*"},
			pathSuffix:  "bypass_on_user_agent[1]",
			msgContains: "case-insensitive",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := validBase()
			cfg.Routes[0].Cache.BypassOnUserAgent = tc.patterns
			err := cfg.Validate()
			requireFieldError(t, err, "routes[0].cache."+tc.pathSuffix, tc.msgContains)
		})
	}
}

func TestValidate_BypassOnUserAgent_TooManyEntries(t *testing.T) {
	t.Parallel()
	patterns := make([]string, 17)
	for i := range patterns {
		patterns[i] = "Bot" + string(rune('a'+i))
	}
	cfg := validBase()
	cfg.Routes[0].Cache.BypassOnUserAgent = patterns
	err := cfg.Validate()
	requireFieldError(t, err, "routes[0].cache.bypass_on_user_agent", "capped at 16 entries")
}

func TestValidate_BypassOnUserAgent_Accepted(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.Routes[0].Cache.BypassOnUserAgent = []string{
		"ShoppingFeedBot",       // exact
		"*Googlebot*",           // substring glob
		"  Mozilla/5.0*Feed*  ", // trimmed, multi-segment, '/' literal
		"*Bingbot",              // suffix glob
		"bot",                   // case-variant of Bot is distinct
	}
	require.NoError(t, cfg.Validate())
}

// --- route_defaults inheritance (mergeBypassOnUserAgentDefaults) ---

// TestRouteDefaults_BypassOnUserAgent_MergeMatrix pins the
// route_defaults.cache.bypass_on_user_agent precedence (see
// mergeBypassOnUserAgentDefaults): unset inherits wholesale, a route's
// own list replaces (never unions), an explicit empty list opts out,
// and static routes inherit nothing.
func TestRouteDefaults_BypassOnUserAgent_MergeMatrix(t *testing.T) {
	t.Parallel()
	def := []string{"*ShoppingFeedBot*", "*Googlebot*"}
	tests := []struct {
		name  string
		route []string
		want  []string
	}{
		{
			name:  "unset inherits wholesale",
			route: nil,
			want:  def,
		},
		{
			name:  "own list replaces, never unions",
			route: []string{"*Bingbot"},
			want:  []string{"*Bingbot"},
		},
		{
			name:  "explicit empty list opts out",
			route: []string{},
			want:  []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := validBase()
			cfg.RouteDefaults.Cache.BypassOnUserAgent = def
			cfg.Routes[0].Cache.BypassOnUserAgent = tt.route
			require.NoError(t, cfg.Validate())
			assert.Equal(t, tt.want, cfg.Routes[0].Cache.BypassOnUserAgent)
		})
	}
}

// Static routes inherit nothing: the knob is wired on pool routes only
// (the per-route field is equally inert there), and no error is raised.
func TestRouteDefaults_BypassOnUserAgent_StaticRouteSkipped(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.RouteDefaults.Cache.BypassOnUserAgent = []string{"*Bot*"}
	cfg.Routes[0].Pool = ""
	cfg.Routes[0].Static = StaticConfig{Root: "/tmp"}
	require.NoError(t, cfg.Validate())
	assert.Nil(t, cfg.Routes[0].Cache.BypassOnUserAgent)
}

// An invalid default stands alone: one error at route_defaults' own
// path, and nothing is merged into any route.
func TestRouteDefaults_BypassOnUserAgent_InvalidDefaultsStandsAlone(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.RouteDefaults.Cache.BypassOnUserAgent = []string{"*"}
	requireFieldError(t, cfg.Validate(), "route_defaults.cache.bypass_on_user_agent[0]", "matches every request")
	assert.Nil(t, cfg.Routes[0].Cache.BypassOnUserAgent)
}

// The merge is idempotent: a second Validate call neither re-merges
// nor clobbers the resolved routes.
func TestRouteDefaults_BypassOnUserAgent_Idempotent(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.RouteDefaults.Cache.BypassOnUserAgent = []string{"*Bot*"}
	require.NoError(t, cfg.Validate())
	first := cfg.Routes[0].Cache.BypassOnUserAgent
	require.NoError(t, cfg.Validate())
	assert.Equal(t, first, cfg.Routes[0].Cache.BypassOnUserAgent)
}

// An empty default is a no-op: there is nothing to inherit, and routes
// keep whatever they declared.
func TestRouteDefaults_BypassOnUserAgent_EmptyDefaultIsNoOp(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.RouteDefaults.Cache.BypassOnUserAgent = nil
	cfg.Routes[0].Cache.BypassOnUserAgent = []string{"*Bot*"}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, []string{"*Bot*"}, cfg.Routes[0].Cache.BypassOnUserAgent)
}

// TestRouteDefaults_BypassOnUserAgent_YAMLEndToEnd pins the whole
// pipeline through Parse: the default merges into unset routes, a
// route's own list replaces it, and an explicit empty list opts out —
// including that yaml decodes `[]` to a non-nil empty slice (the
// opt-out form's carrier).
func TestRouteDefaults_BypassOnUserAgent_YAMLEndToEnd(t *testing.T) {
	t.Parallel()
	b := []byte(`
listen:
  admin: :9000
upstream_pools:
  - name: app
    targets: ["a:1"]
route_defaults:
  cache:
    bypass_on_user_agent: ["*ShoppingFeedBot*"]
routes:
  - name: inherit
    pool: app
    cache: {ttl_default: 60s}
  - name: override
    pool: app
    cache:
      ttl_default: 60s
      bypass_on_user_agent: ["*Bingbot"]
  - name: optout
    pool: app
    cache:
      ttl_default: 60s
      bypass_on_user_agent: []
`)
	cfg, err := Parse(b)
	require.NoError(t, err)
	require.Len(t, cfg.Routes, 3)
	assert.Equal(t, []string{"*ShoppingFeedBot*"}, cfg.Routes[0].Cache.BypassOnUserAgent,
		"unset route must inherit the default wholesale")
	assert.Equal(t, []string{"*Bingbot"}, cfg.Routes[1].Cache.BypassOnUserAgent,
		"a route's own list must replace the default, never union with it")
	require.NotNil(t, cfg.Routes[2].Cache.BypassOnUserAgent,
		"an explicit empty list must decode non-nil (the opt-out carrier)")
	assert.Empty(t, cfg.Routes[2].Cache.BypassOnUserAgent)
}

// route_defaults.cache accepts only bypass_on_user_agent: any other
// cache field under it fails strict decoding instead of silently not
// applying.
func TestRouteDefaults_BypassOnUserAgent_UnknownFieldRejected(t *testing.T) {
	t.Parallel()
	b := []byte(`
listen:
  admin: :9000
upstream_pools:
  - name: app
    targets: ["a:1"]
route_defaults:
  cache:
    ttl_default: 60s
routes:
  - name: r
    pool: app
`)
	_, err := Parse(b)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ttl_default")
}
