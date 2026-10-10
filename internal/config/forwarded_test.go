package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestValidate_Forwarded_RejectsHeaderSetConflict pins the conflict rule
// (issue #769): header_set and forwarded both write the same headers, so
// targeting the same header from both is rejected — whichever applied
// second would silently mask the other. Case-insensitive on the
// header_set key.
func TestValidate_Forwarded_RejectsHeaderSetConflict(t *testing.T) {
	t.Parallel()
	for name, hsValue := range map[string]string{
		"X-Forwarded-For":   "1.2.3.4",
		"x-forwarded-for":   "1.2.3.4",
		"X-Forwarded-Proto": "https",
		"X-Forwarded-Host":  "h",
		"Via":               "1.1 x",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := validBase()
			cfg.Routes[0].Request.Forwarded = ForwardedConfig{ClientIP: true, Proto: true, Host: true, Via: true}
			cfg.Routes[0].Request.HeaderSet = map[string]string{name: hsValue}
			err := cfg.Validate()
			requireFieldError(t, err, "routes[0].request.forwarded", "mutually exclusive with header_set entry")
		})
	}
}

// TestValidate_Forwarded_HeaderSetWithoutForwardedIsFine pins that
// header_set may still target the forwarded header names when the
// forwarded block is unset — header_set remains the explicit escape
// hatch for static values.
func TestValidate_Forwarded_HeaderSetWithoutForwardedIsFine(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.Routes[0].Request.HeaderSet = map[string]string{"X-Forwarded-Proto": "https"}
	require.NoError(t, cfg.Validate())
}

// TestValidate_Forwarded_RequiresPool pins that forwarded headers only
// apply to origin-proxy routes: a static route forwards to no origin.
func TestValidate_Forwarded_RequiresPool(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.Routes[0].Pool = ""
	cfg.Routes[0].Static = StaticConfig{Root: "/tmp"}
	cfg.Routes[0].Request.Forwarded = ForwardedConfig{ClientIP: true}
	err := cfg.Validate()
	requireFieldError(t, err, "routes[0].request.forwarded", "requires a pool")
}

// TestValidate_Forwarded_MaxAppendBounds pins the range check: 1..64.
func TestValidate_Forwarded_MaxAppendBounds(t *testing.T) {
	t.Parallel()
	for _, max := range []int{-1, MaxForwardedAppend + 1} {
		cfg := validBase()
		cfg.Routes[0].Request.Forwarded = ForwardedConfig{ClientIP: true, MaxAppend: max}
		err := cfg.Validate()
		requireFieldError(t, err, "routes[0].request.forwarded.max_append", "must be between 1 and")
	}
}

// TestValidate_Forwarded_DefaultsMaxAppend pins the normalisation: an
// unset max_append becomes DefaultForwardedMaxAppend when any flag is
// on (mirroring route method upper-casing), and explicit values pass
// through unchanged.
func TestValidate_Forwarded_DefaultsMaxAppend(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.Routes[0].Request.Forwarded = ForwardedConfig{ClientIP: true}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, DefaultForwardedMaxAppend, cfg.Routes[0].Request.Forwarded.MaxAppend)

	cfg = validBase()
	cfg.Routes[0].Request.Forwarded = ForwardedConfig{Via: true, MaxAppend: 3}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 3, cfg.Routes[0].Request.Forwarded.MaxAppend)
}

// TestValidate_Forwarded_UnsetBlockIsNoOp pins the default: routes
// without the forwarded block validate unchanged and no default is
// injected into the struct.
func TestValidate_Forwarded_UnsetBlockIsNoOp(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	require.NoError(t, cfg.Validate())
	assert.Equal(t, ForwardedConfig{}, cfg.Routes[0].Request.Forwarded)
}

// TestForwardedUnmarshal_Shapes pins every accepted YAML shape of the
// forwarded block (issue #769): scalar presets (and their bool
// spellings), the token list, the full mapping, and null (unset). All
// normalize to the one ForwardedConfig, with the form recorded for the
// route_defaults merge.
func TestForwardedUnmarshal_Shapes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		yaml string
		want ForwardedConfig
	}{
		{
			"preset standard",
			"forwarded: standard",
			ForwardedConfig{ClientIP: true, Proto: true, Host: true, Via: true, form: forwardedFormExact},
		},
		{
			"preset standard upper-case",
			"forwarded: Standard",
			ForwardedConfig{ClientIP: true, Proto: true, Host: true, Via: true, form: forwardedFormExact},
		},
		{
			"bool true",
			"forwarded: true",
			ForwardedConfig{ClientIP: true, Proto: true, Host: true, Via: true, form: forwardedFormExact},
		},
		{
			"quoted true",
			`forwarded: "true"`,
			ForwardedConfig{ClientIP: true, Proto: true, Host: true, Via: true, form: forwardedFormExact},
		},
		{
			"preset none",
			"forwarded: none",
			ForwardedConfig{form: forwardedFormOff},
		},
		{
			"bool false",
			"forwarded: false",
			ForwardedConfig{form: forwardedFormOff},
		},
		{
			"quoted false",
			`forwarded: "false"`,
			ForwardedConfig{form: forwardedFormOff},
		},
		{
			"token list",
			"forwarded: [client_ip, proto]",
			ForwardedConfig{ClientIP: true, Proto: true, form: forwardedFormExact},
		},
		{
			"token list with padding",
			"forwarded: [ via , host ]",
			ForwardedConfig{Host: true, Via: true, form: forwardedFormExact},
		},
		{
			"full mapping",
			"forwarded: {client_ip: true, via: true, max_append: 9}",
			ForwardedConfig{ClientIP: true, Via: true, MaxAppend: 9, form: forwardedFormMerge},
		},
		{
			"mapping all-false is a merge form, not an off",
			"forwarded: {client_ip: false}",
			ForwardedConfig{form: forwardedFormMerge},
		},
		{
			"null is unset",
			"forwarded: null",
			ForwardedConfig{},
		},
		{
			"absent is unset",
			"other: 1",
			ForwardedConfig{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var dst struct {
				Forwarded ForwardedConfig `yaml:"forwarded"`
				Other     int             `yaml:"other"`
			}
			require.NoError(t, yamlUnmarshal(t, []byte(tt.yaml), &dst))
			assert.Equal(t, tt.want, dst.Forwarded)
		})
	}
}

// TestForwardedUnmarshal_Rejects pins the decode-time rejections:
// unknown presets, unknown tokens, empty lists, and non-scalar,
// non-list, non-mapping kinds.
func TestForwardedUnmarshal_Rejects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		yaml string
		msg  string
	}{
		{"unknown preset", "forwarded: full", `preset must be "standard" or "none"`},
		{"numeric scalar", "forwarded: 5", "must be a preset, bool, token list, or mapping"},
		{"unknown token", "forwarded: [client_ip, xff]", `unknown forwarded token "xff"`},
		{"empty list", "forwarded: []", "token list must not be empty"},
		{"nested list", "forwarded: [[client_ip]]", "must be a preset, bool, token list, or mapping"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var dst struct {
				Forwarded ForwardedConfig `yaml:"forwarded"`
			}
			err := yamlUnmarshal(t, []byte(tt.yaml), &dst)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.msg)
		})
	}
}

// TestRouteDefaults_MergeMatrix pins the route_defaults precedence
// rules (mergeRouteDefaults): unset inherits wholesale, none/false opts
// out, the token list replaces flags, the mapping ORs flags per field,
// and max_append falls back to the default when unset.
func TestRouteDefaults_MergeMatrix(t *testing.T) {
	t.Parallel()
	standard := ForwardedStandard()
	tests := []struct {
		name  string
		route ForwardedConfig
		want  ForwardedConfig
	}{
		{
			name:  "unset inherits wholesale",
			route: ForwardedConfig{},
			want:  ForwardedConfig{ClientIP: true, Proto: true, Host: true, Via: true, MaxAppend: 3, form: forwardedFormExact, inherited: true},
		},
		{
			name:  "none opts out",
			route: ForwardedConfig{form: forwardedFormOff},
			want:  ForwardedConfig{form: forwardedFormOff},
		},
		{
			name:  "token list replaces flags",
			route: ForwardedConfig{Proto: true, Host: true, form: forwardedFormExact},
			want:  ForwardedConfig{Proto: true, Host: true, MaxAppend: 3, form: forwardedFormExact},
		},
		{
			name:  "mapping ors flags per field",
			route: ForwardedConfig{Via: true, form: forwardedFormMerge},
			want:  ForwardedConfig{ClientIP: true, Proto: true, Host: true, Via: true, MaxAppend: 3, form: forwardedFormMerge},
		},
		{
			name:  "mapping with only max_append inherits flags",
			route: ForwardedConfig{MaxAppend: 7, form: forwardedFormMerge},
			want:  ForwardedConfig{ClientIP: true, Proto: true, Host: true, Via: true, MaxAppend: 7, form: forwardedFormMerge},
		},
		{
			name:  "token list inherits default max_append",
			route: ForwardedConfig{Via: true, form: forwardedFormExact},
			want:  ForwardedConfig{Via: true, MaxAppend: 3, form: forwardedFormExact},
		},
		{
			name:  "programmatic fields are exact, not clobbered",
			route: ForwardedConfig{ClientIP: true},
			want:  ForwardedConfig{ClientIP: true, MaxAppend: 3, form: forwardedFormExact},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := validBase()
			d := standard
			d.MaxAppend = 3
			cfg.RouteDefaults.Request.Forwarded = d
			cfg.Routes[0].Request.Forwarded = tt.route
			require.NoError(t, cfg.Validate())
			assert.Equal(t, tt.want, cfg.Routes[0].Request.Forwarded)
		})
	}
}

// TestRouteDefaults_StaticRouteSkipped pins that a default landing on a
// static route is vacuously ignored (no error, no injection), while an
// explicit block on a static route stays an error.
func TestRouteDefaults_StaticRouteSkipped(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.RouteDefaults.Request.Forwarded = ForwardedStandard()
	cfg.Routes[0].Pool = ""
	cfg.Routes[0].Static = StaticConfig{Root: "/tmp"}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, ForwardedConfig{}, cfg.Routes[0].Request.Forwarded)

	cfg = validBase()
	cfg.RouteDefaults.Request.Forwarded = ForwardedStandard()
	cfg.Routes[0].Pool = ""
	cfg.Routes[0].Static = StaticConfig{Root: "/tmp"}
	cfg.Routes[0].Request.Forwarded = ForwardedConfig{ClientIP: true}
	requireFieldError(t, cfg.Validate(), "routes[0].request.forwarded", "requires a pool")
}

// TestRouteDefaults_InheritedHeaderSetConflict pins that the conflict
// rule fires on the resolved value: a route_defaults forwarded block
// plus a route header_set entry targeting the same header is rejected.
func TestRouteDefaults_InheritedHeaderSetConflict(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.RouteDefaults.Request.Forwarded = ForwardedStandard()
	cfg.Routes[0].Request.HeaderSet = map[string]string{"X-Forwarded-For": "1.2.3.4"}
	requireFieldError(t, cfg.Validate(), "routes[0].request.forwarded", "mutually exclusive with header_set entry")
}

// TestRouteDefaults_InvalidDefaultsStandsAlone pins that an
// out-of-bounds route_defaults max_append produces one error at its own
// path, not a cascade of per-route errors.
func TestRouteDefaults_InvalidDefaultsStandsAlone(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	d := ForwardedStandard()
	d.MaxAppend = MaxForwardedAppend + 1
	cfg.RouteDefaults.Request.Forwarded = d
	requireFieldError(t, cfg.Validate(), "route_defaults.request.forwarded.max_append", "must be between 1 and")
	// The invalid default was not merged into the route.
	assert.Equal(t, ForwardedConfig{}, cfg.Routes[0].Request.Forwarded)
}

// TestRouteDefaults_Idempotent pins that a second Validate call (tests
// call Validate directly; Parse calls it once) does not re-merge or
// clobber the resolved routes.
func TestRouteDefaults_Idempotent(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.RouteDefaults.Request.Forwarded = ForwardedStandard()
	cfg.Routes[0].Request.Forwarded = ForwardedConfig{MaxAppend: 7, form: forwardedFormMerge}
	require.NoError(t, cfg.Validate())
	first := cfg.Routes[0].Request.Forwarded
	require.NoError(t, cfg.Validate())
	assert.Equal(t, first, cfg.Routes[0].Request.Forwarded)
}

// TestRouteDefaults_NoneDefaultIsNoOp pins that a default of none (or a
// bool false default) leaves routes untouched: there is nothing to
// inherit.
func TestRouteDefaults_NoneDefaultIsNoOp(t *testing.T) {
	t.Parallel()
	for name, d := range map[string]ForwardedConfig{
		"none":  {form: forwardedFormOff},
		"false": {form: forwardedFormOff},
		"empty": {},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := validBase()
			cfg.RouteDefaults.Request.Forwarded = d
			require.NoError(t, cfg.Validate())
			assert.Equal(t, ForwardedConfig{}, cfg.Routes[0].Request.Forwarded)
		})
	}
}

// TestRouteDefaults_YAMLEndToEnd pins the whole pipeline through Parse:
// the compact forms decode, the default merges, per-route forms
// override, and the resolved config validates.
func TestRouteDefaults_YAMLEndToEnd(t *testing.T) {
	t.Parallel()
	b := []byte(`
listen:
  admin: :9000
upstream_pools:
  - name: app
    targets: ["a:1"]
route_defaults:
  request:
    forwarded: standard
routes:
  - name: inherit
    pool: app
    cache: {ttl_default: 60s}
  - name: optout
    pool: app
    request:
      forwarded: none
    cache: {ttl_default: 60s}
  - name: subset
    pool: app
    request:
      forwarded: [proto, host]
    cache: {ttl_default: 60s}
  - name: tuned
    pool: app
    request:
      forwarded:
        max_append: 12
    cache: {ttl_default: 60s}
`)
	cfg, err := Parse(b)
	require.NoError(t, err)

	inherit := cfg.Routes[0].Request.Forwarded
	assert.True(t, inherit.ClientIP && inherit.Proto && inherit.Host && inherit.Via, "inherit route: %v", inherit)
	assert.Equal(t, DefaultForwardedMaxAppend, inherit.MaxAppend)

	assert.Equal(t, ForwardedConfig{form: forwardedFormOff}, cfg.Routes[1].Request.Forwarded)

	subset := cfg.Routes[2].Request.Forwarded
	assert.True(t, subset.Proto && subset.Host, "subset route: %v", subset)
	assert.False(t, subset.ClientIP || subset.Via, "subset route: %v", subset)

	tuned := cfg.Routes[3].Request.Forwarded
	assert.True(t, tuned.ClientIP && tuned.Proto && tuned.Host && tuned.Via, "tuned route: %v", tuned)
	assert.Equal(t, 12, tuned.MaxAppend)
}

// TestRouteDefaults_UnknownFieldRejected pins that route_defaults only
// accepts the designed merge surface: any other route field under it
// fails strict decoding instead of silently not applying.
func TestRouteDefaults_UnknownFieldRejected(t *testing.T) {
	t.Parallel()
	b := []byte(`
listen:
  admin: :9000
upstream_pools:
  - name: app
    targets: ["a:1"]
route_defaults:
  request:
    header_set: {"X-Foo": "bar"}
routes:
  - name: r
    pool: app
    cache: {ttl_default: 60s}
`)
	_, err := Parse(b)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "header_set")
}
