package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func boolPtr(b bool) *bool { return &b }

// A disabled route needs no ttl_default or any other cache knob.
func TestRouteCacheDisabled_MinimalBlockValid(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.Routes[0].Cache = RouteCache{Enabled: boolPtr(false)}
	assert.NoError(t, cfg.Validate())
}

// Dead-config rejection (ADR-0074): each storage-side knob errors at its own field path.
func TestRouteCacheDisabled_DeadKnobsRejected(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		mut  func(*RouteCache)
		want string
	}{
		{
			"bypass_on_cookie",
			func(rc *RouteCache) { rc.BypassOnCookie = boolPtr(true) },
			"routes[0].cache.bypass_on_cookie",
		},
		{
			"bypass_on_cookie_names",
			func(rc *RouteCache) { rc.BypassOnCookieNames = []string{"sid"} },
			"routes[0].cache.bypass_on_cookie_names",
		},
		{
			"bypass_on_user_agent",
			func(rc *RouteCache) { rc.BypassOnUserAgent = []string{"*Bot*"} },
			"routes[0].cache.bypass_on_user_agent",
		},
		{
			"key.strip_query_params",
			func(rc *RouteCache) { rc.Key.StripQueryParams = []string{"utm"} },
			"routes[0].cache.key",
		},
		{
			"key.include_host false",
			func(rc *RouteCache) { rc.Key.IncludeHost = boolPtr(false) },
			"routes[0].cache.key",
		},
		{
			"key.cookie_values",
			func(rc *RouteCache) { rc.Key.CookieValues = []string{"locale"} },
			"routes[0].cache.key",
		},
		{
			"key.verbatim_encoding",
			func(rc *RouteCache) { rc.Key.VerbatimEncoding = true },
			"routes[0].cache.key",
		},
		{
			"ttl_default",
			func(rc *RouteCache) { rc.TTLDefault = time.Minute },
			"routes[0].cache.ttl_default",
		},
		{
			"ttl_default negative",
			func(rc *RouteCache) { rc.TTLDefault = -time.Minute },
			"routes[0].cache.ttl_default",
		},
		{
			"ttl_override",
			func(rc *RouteCache) { rc.TTLOverride = time.Minute },
			"routes[0].cache.ttl_override",
		},
		{
			"stale_while_revalidate",
			func(rc *RouteCache) { rc.StaleWhileRevalidate = 30 * time.Second },
			"routes[0].cache.stale_while_revalidate",
		},
		{
			"swr_override",
			func(rc *RouteCache) { rc.SWROverride = 30 * time.Second },
			"routes[0].cache.swr_override",
		},
		{
			"stale_if_error",
			func(rc *RouteCache) { rc.StaleIfError = 5 * time.Minute },
			"routes[0].cache.stale_if_error",
		},
		{
			"negative_ttl scalar",
			func(rc *RouteCache) { rc.NegativeTTL = NegTTLScalar(30 * time.Second) },
			"routes[0].cache.negative_ttl",
		},
		{
			"max_variants",
			func(rc *RouteCache) { rc.MaxVariants = 64 },
			"routes[0].cache.max_variants",
		},
		{
			"refresh_before_expiry",
			func(rc *RouteCache) { rc.RefreshBeforeExpiry = true },
			"routes[0].cache.refresh_before_expiry",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := validBase()
			cfg.Routes[0].Cache = RouteCache{Enabled: boolPtr(false)}
			tc.mut(&cfg.Routes[0].Cache)
			requireFieldError(t, cfg.Validate(), tc.want, "is dead config on a route with cache.enabled: false")
		})
	}
}

// Fetch-side knobs keep their range checks on a disabled route (live on the BYPASS path).
func TestRouteCacheDisabled_FetchKnobsStayValidated(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		mut  func(*RouteCache)
		want string
	}{
		{
			"max_response_bytes negative",
			func(rc *RouteCache) { rc.MaxResponseBytes = -1 },
			"routes[0].cache.max_response_bytes",
		},
		{
			"max_fetch_concurrency negative",
			func(rc *RouteCache) { rc.MaxFetchConcurrency = -1 },
			"routes[0].cache.max_fetch_concurrency",
		},
		{
			"fetch_timeout negative",
			func(rc *RouteCache) { rc.FetchTimeout = -time.Second },
			"routes[0].cache.fetch_timeout",
		},
		{
			"fetch_timeout at safety net",
			func(rc *RouteCache) { rc.FetchTimeout = 10 * time.Minute },
			"routes[0].cache.fetch_timeout",
		},
		{
			"fetch_wait_timeout negative",
			func(rc *RouteCache) { rc.FetchWaitTimeout = -time.Second },
			"routes[0].cache.fetch_wait_timeout",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := validBase()
			cfg.Routes[0].Cache = RouteCache{Enabled: boolPtr(false)}
			tc.mut(&cfg.Routes[0].Cache)
			fes := fieldErrs(t, cfg.Validate())
			require.Len(t, fes, 1)
			assert.Equal(t, tc.want, fes[0].Path)
			assert.Contains(t, fes[0].Message, "must be")
		})
	}
}

// Accepted-but-inert knobs (ADR-0074) are form-checked, never rejected.
func TestRouteCacheDisabled_InertKnobsAccepted(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.Routes[0].Cache = RouteCache{
		Enabled:             boolPtr(false),
		JitterPercent:       10,
		AllowSetCookie:      boolPtr(true),
		StayinAlive:         true,
		DownstreamFreshness: DownstreamFreshnessSync,
		MaxObjectSize:       1 << 20,
		FetchTimeout:        5 * time.Second,
	}
	assert.NoError(t, cfg.Validate())
}

// enabled: true runs today's full validation — the disabled branch doesn't leak into it.
func TestRouteCacheDisabled_EnabledTrueUnaffected(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	cfg.Routes[0].Cache = RouteCache{
		Enabled:              boolPtr(true),
		TTLDefault:           time.Minute,
		StaleWhileRevalidate: 30 * time.Second,
		StaleIfError:         5 * time.Minute,
		MaxVariants:          64,
		RefreshBeforeExpiry:  true,
	}
	assert.NoError(t, cfg.Validate())

	// Today's requirement checks still fire on the enabled side.
	cfg.Routes[0].Cache.RefreshBeforeExpiry = true
	cfg.Routes[0].Cache.TTLDefault = 0
	cfg.Routes[0].Cache.TTLOverride = 0
	requireFieldError(t, cfg.Validate(), "routes[0].cache.refresh_before_expiry", "requires ttl_default or ttl_override > 0")
}

// Disabled(): nil and true keep the cache; only explicit false disables it.
func TestRouteCacheDisabled_DisabledMethod(t *testing.T) {
	t.Parallel()
	assert.False(t, RouteCache{}.Disabled(), "nil enabled keeps the cache (today's default)")
	assert.False(t, RouteCache{Enabled: boolPtr(true)}.Disabled())
	assert.True(t, RouteCache{Enabled: boolPtr(false)}.Disabled())
}

// No route_defaults.cache.enabled form: strict decode rejects it (ADR-0074).
func TestRouteCacheDisabled_NoDefaultsMergeForm(t *testing.T) {
	t.Parallel()
	b := []byte(`
listen:
  admin: :9000
upstream_pools:
  - name: app
    targets: ["a:1"]
route_defaults:
  cache:
    enabled: false
routes:
  - name: r
    pool: app
`)
	_, err := Parse(b)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "enabled")
}

// Merge-side skips (ADR-0074): the rejected list defaults never land on a
// disabled route; downstream_freshness still merges (accepted-but-inert).
func TestRouteDefaults_DisabledRoutesSkipDeadDefaults(t *testing.T) {
	t.Parallel()
	cfg := validBase()
	no := boolPtr(false)
	cfg.Routes[0].Cache = RouteCache{Enabled: no}
	cfg.Routes = append(cfg.Routes, Route{Name: "r1", Pool: "app", Cache: RouteCache{Enabled: no}})
	cfg.RouteDefaults.Cache.BypassOnUserAgent = []string{"*Bot*"}
	cfg.RouteDefaults.Cache.Key.CookieValues = []string{"locale"}
	cfg.RouteDefaults.Cache.DownstreamFreshness = DownstreamFreshnessSync
	require.NoError(t, cfg.Validate())
	for i := range cfg.Routes {
		assert.Nil(t, cfg.Routes[i].Cache.BypassOnUserAgent)
		assert.Nil(t, cfg.Routes[i].Cache.Key.CookieValues)
		assert.Equal(t, DownstreamFreshnessSync, cfg.Routes[i].Cache.DownstreamFreshness)
	}

	// The enabled sibling keeps inheriting everything.
	cfg = validBase()
	cfg.Routes = append(cfg.Routes, Route{Name: "r1", Pool: "app", Cache: RouteCache{Enabled: no}})
	cfg.RouteDefaults.Cache.BypassOnUserAgent = []string{"*Bot*"}
	cfg.RouteDefaults.Cache.Key.CookieValues = []string{"locale"}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, []string{"*Bot*"}, cfg.Routes[0].Cache.BypassOnUserAgent)
	assert.Equal(t, []string{"locale"}, cfg.Routes[0].Cache.Key.CookieValues)
	assert.Nil(t, cfg.Routes[1].Cache.BypassOnUserAgent)
	assert.Nil(t, cfg.Routes[1].Cache.Key.CookieValues)
}

// End-to-end through Parse: a disabled route beside an enabled sibling.
func TestRouteCacheDisabled_YAMLEndToEnd(t *testing.T) {
	t.Parallel()
	b := []byte(`
listen:
  admin: :9000
upstream_pools:
  - name: app
    targets: ["a:1"]
routes:
  - name: per-user-api
    pool: app
    cache:
      enabled: false
  - name: api
    pool: app
    cache:
      ttl_default: 60s
      stale_while_revalidate: 30s
      stale_if_error: 5m
`)
	cfg, err := Parse(b)
	require.NoError(t, err)
	require.Len(t, cfg.Routes, 2)
	assert.True(t, cfg.Routes[0].Cache.Disabled())
	assert.False(t, cfg.Routes[1].Cache.Disabled())
	assert.Equal(t, time.Minute, cfg.Routes[1].Cache.TTLDefault)
}

// The dead-knob rejection fires through Parse, at load.
func TestRouteCacheDisabled_DeadKnobInDisabledRouteFailsLoad(t *testing.T) {
	t.Parallel()
	b := []byte(`
listen:
  admin: :9000
upstream_pools:
  - name: app
    targets: ["a:1"]
routes:
  - name: r
    pool: app
    cache:
      enabled: false
      key:
        include_headers: [Accept-Language]
`)
	_, err := Parse(b)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "routes[0].cache.key")
	assert.Contains(t, err.Error(), "dead config")
}
