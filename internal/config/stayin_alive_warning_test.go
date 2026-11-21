package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStayinAlive_NoHealthSignalWarns pins issue #933: a stayin_alive
// route on a pool without passive consecutive_5xx and without an active
// probe produces a non-fatal warning (validation still passes), because
// ADR-0051 grace retention cannot engage without an ejection signal.
func TestStayinAlive_NoHealthSignalWarns(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.Routes[0].Cache.StayinAlive = true

	require.NoError(t, cfg.Validate())
	require.Len(t, cfg.Warnings, 1, "expected one warning, got %v", cfg.Warnings)
	assert.Equal(t, "routes[0].cache.stayin_alive", cfg.Warnings[0].Path)
	assert.Contains(t, cfg.Warnings[0].Message, `pool "app" has no health signal`)
	assert.Contains(t, cfg.Warnings[0].Message, "consecutive_5xx")
}

func TestStayinAlive_HealthSignalVariants(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		mutPool  func(*UpstreamPool)
		wantWarn bool
	}{
		{"passive consecutive_5xx", func(p *UpstreamPool) { p.Health.Passive.Consecutive5xx = 5 }, false},
		{"active probe", func(p *UpstreamPool) { p.Health.Active.Path = "/healthz" }, false},
		{"neither", func(*UpstreamPool) {}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := validBase()
			tc.mutPool(&cfg.UpstreamPools[0])
			cfg.Routes[0].Cache.StayinAlive = true
			require.NoError(t, cfg.Validate())
			if tc.wantWarn {
				require.Len(t, cfg.Warnings, 1)
			} else {
				assert.Empty(t, cfg.Warnings)
			}
		})
	}
}

// TestStayinAlive_WarningSkipped pins the cases where no warning is
// emitted: no stayin_alive, cache disabled, static route (no pool), or
// unknown pool (that path already errors).
func TestStayinAlive_WarningSkipped(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		mut  func(*Config)
	}{
		{"stayin_alive off", func(c *Config) {}},
		{"cache disabled", func(c *Config) {
			c.Routes[0].Cache = RouteCache{Enabled: boolPtr(false), StayinAlive: true}
		}},
		{"static route", func(c *Config) {
			c.Routes[0].Pool = ""
			c.Routes[0].Static.Root = "/tmp"
			c.Routes[0].Cache.StayinAlive = true
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := validBase()
			tc.mut(&cfg)
			require.NoError(t, cfg.Validate())
			for _, w := range cfg.Warnings {
				assert.NotEqual(t, "routes[0].cache.stayin_alive", w.Path,
					"stayin_alive warning must not fire here")
			}
		})
	}
}

// TestStayinAlive_WarningIdempotent ensures a second Validate pass (the
// mergeRouteDefaults contract) does not duplicate the warning.
func TestStayinAlive_WarningIdempotent(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.Routes[0].Cache.StayinAlive = true
	require.NoError(t, cfg.Validate())
	require.NoError(t, cfg.Validate())
	assert.Len(t, cfg.Warnings, 1)
}
