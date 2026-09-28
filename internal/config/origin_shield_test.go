package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
)

// shieldConfig builds a minimal valid strong-mode config with the
// shield on and one pool-backed route.
func shieldConfig() Config {
	cfg := Defaults()
	cfg.Cluster.OriginShield = true
	cfg.UpstreamPools = []UpstreamPool{{Name: "origin", Targets: []string{"127.0.0.1:80"}}}
	cfg.Routes = []Route{{
		Match: RouteMatch{PathPrefix: "/"},
		Pool:  "origin",
	}}
	return cfg
}

// TestValidate_OriginShieldBackfillBounds pins the knob bounds
// (D10): [0.0, 1.0], nil (unset) valid and defaulting at wiring.
func TestValidate_OriginShieldBackfillBounds(t *testing.T) {
	t.Parallel()

	for _, p := range []float64{-0.1, 1.1, 2} {
		cfg := shieldConfig()
		cfg.Cluster.OriginShieldBackfillProbability = &p
		err := cfg.Validate()
		require.Error(t, err, "backfill probability %v must be rejected", p)
		assert.Contains(t, err.Error(), "origin_shield_backfill_probability")
	}
	for _, p := range []float64{0, 0.5, 1} {
		cfg := shieldConfig()
		cfg.Cluster.OriginShieldBackfillProbability = &p
		require.NoError(t, cfg.Validate(), "backfill probability %v must be accepted", p)
	}
}

// TestValidate_OriginShieldFetchTimeoutInvariant pins D4's config
// side: an explicit route fetch_timeout at or below the shield bound
// is rejected while the shield is on — a requester that gives up on
// the wait must still have budget for its own fetch.
func TestValidate_OriginShieldFetchTimeoutInvariant(t *testing.T) {
	t.Parallel()

	cfg := shieldConfig()
	cfg.Routes[0].Cache.FetchTimeout = api.ShieldForwardTimeout
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetch_timeout")

	cfg.Routes[0].Cache.FetchTimeout = api.ShieldForwardTimeout - time.Second
	require.Error(t, cfg.Validate(), "equal-to-bound values are rejected too")

	cfg.Routes[0].Cache.FetchTimeout = api.ShieldForwardTimeout + time.Second
	require.NoError(t, cfg.Validate())

	// Flag off: the invariant is irrelevant — identical config validates.
	cfg.Cluster.OriginShield = false
	cfg.Routes[0].Cache.FetchTimeout = time.Second
	require.NoError(t, cfg.Validate())
}

// TestValidate_OriginShieldRequiresStrongMode pins D9's scope: the
// shield forwards cold misses to the consistent-hash ring owner,
// which only exists in strong mode.
func TestValidate_OriginShieldRequiresStrongMode(t *testing.T) {
	t.Parallel()

	cfg := shieldConfig()
	cfg.Cluster.Mode = ClusterModeEventual
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "origin_shield")

	cfg.Cluster.Mode = ClusterModeStrong
	require.NoError(t, cfg.Validate())
}
