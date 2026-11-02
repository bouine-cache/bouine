package insights

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/observability"
)

// A cache-disabled route raises no cache-knob insight (the isCacheEnabled
// gate, ADR-0074) — but cache-disabled-high-traffic still fires.
func TestRules_CacheDisabledRouteYieldsNoCacheKnobInsights(t *testing.T) {
	t.Parallel()
	off := false
	data := InsightData{
		Config: &config.Config{
			Routes: []config.Route{{
				Name:  "per-user-api",
				Pool:  "app",
				Cache: config.RouteCache{Enabled: &off},
			}},
		},
		// Traffic high enough that every gated rule would fire without the gate.
		RouteStats: []observability.RouteStat{{
			Route:     "per-user-api",
			Requests:  500,
			Hits:      0,
			Misses:    500,
			Errors:    10,
			HitPct:    0,
			Sparkline: []int64{120, 120},
		}},
	}
	rules := map[string]func(InsightData) *Insight{
		"cache-no-ttl":                    ruleCacheNoTTL,
		"cache-no-swr":                    ruleCacheNoSWR,
		"cache-no-sie":                    ruleCacheNoSIE,
		"cache-no-negttl":                 ruleCacheNoNegTTL,
		"cache-stayin-alive-off":          ruleCacheStayinAliveOff,
		"config-jitter-zero":              ruleConfigJitterZero,
		"config-max-object-size-unset":    ruleConfigMaxObjectSizeUnset,
		"config-cookie-bypass-missing":    ruleConfigCookieBypassMissing,
		"cache-swr-configured-but-unused": ruleCacheSWRConfiguredButUnused,
	}
	for id, rule := range rules {
		assert.Nil(t, rule(data), "a cache-disabled route must not raise %s", id)
	}

	ins := ruleCacheDisabledHighTraffic(data)
	require.NotNil(t, ins, "cache-disabled-high-traffic must fire on a busy disabled route")
	assert.Equal(t, "cache-disabled-high-traffic", ins.ID)
}

// The gate is per-route: the enabled sibling still raises insights.
func TestRules_EnabledSiblingStillRaisesInsights(t *testing.T) {
	t.Parallel()
	on := true
	cfg := &config.Config{
		Routes: []config.Route{
			{Name: "off", Pool: "app", Cache: config.RouteCache{Enabled: &on}},
		},
		UpstreamPools: []config.UpstreamPool{{Name: "app"}},
	}
	data := InsightData{
		Config: cfg,
		RouteStats: []observability.RouteStat{{
			Route:     "off",
			Requests:  500,
			Hits:      0,
			Misses:    500,
			Sparkline: []int64{120, 120},
		}},
	}
	assert.NotNil(t, ruleCacheNoTTL(data))
	assert.NotNil(t, ruleCacheNoSIE(data))
}

// isCacheEnabled: nil and true keep the route cacheable; only false disables it.
func TestRules_IsCacheEnabled(t *testing.T) {
	t.Parallel()
	assert.True(t, isCacheEnabled(&config.Route{}))
	assert.True(t, isCacheEnabled(&config.Route{Cache: config.RouteCache{}}))
	off := false
	assert.False(t, isCacheEnabled(&config.Route{Cache: config.RouteCache{Enabled: &off}}))
}
