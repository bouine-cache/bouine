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
