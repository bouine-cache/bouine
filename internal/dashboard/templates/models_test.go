package templates

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/observability"
)

func TestBuildRouteRowsTTLOverride(t *testing.T) {
	t.Parallel()
	cfg := []config.Route{
		{
			Name:  "api",
			Pool:  "api-pool",
			Match: config.RouteMatch{PathPrefix: "/api"},
			Cache: config.RouteCache{TTLOverride: 2 * time.Minute, NegativeTTL: 30 * time.Second},
		},
		{
			Name:  "inherit",
			Pool:  "web-pool",
			Match: config.RouteMatch{PathPrefix: "/web"},
			Cache: config.RouteCache{NegativeTTL: 10 * time.Second},
		},
	}
	rows := BuildRouteRows(cfg, []observability.RouteStat{})
	assert.Len(t, rows, 2)

	// The TTL column shows ttl_override, not negative_ttl.
	assert.Equal(t, "2m", rows[0].TTL)
	assert.Equal(t, "30s", rows[0].NegTTL)
	// Without ttl_override the column reads "—": the route inherits
	// the origin's Cache-Control TTL.
	assert.Equal(t, "—", rows[1].TTL)
	assert.Equal(t, "10s", rows[1].NegTTL)
}

func TestBuildRouteRowsJoinsLiveStats(t *testing.T) {
	t.Parallel()
	cfg := []config.Route{{Name: "api", Pool: "p", Match: config.RouteMatch{PathPrefix: "/a"}}}
	stats := []observability.RouteStat{
		{Route: "api", Requests: 42, Hits: 40, HitPct: 95.2, Sparkline: []int64{1, 2, 3}},
		{Route: "_catch-all", Requests: 7, Hits: 1},
	}
	rows := BuildRouteRows(cfg, stats)
	assert.Len(t, rows, 2)
	assert.Equal(t, "api", rows[0].Name)
	assert.EqualValues(t, 42, rows[0].Requests)
	assert.EqualValues(t, 40, rows[0].Hits)
	assert.InDelta(t, 95.2, rows[0].HitPct, 0.01)
	// Live routes absent from config (e.g. _catch-all) are appended.
	assert.Equal(t, "_catch-all", rows[1].Name)
	assert.Equal(t, "—", rows[1].Pool)
}
