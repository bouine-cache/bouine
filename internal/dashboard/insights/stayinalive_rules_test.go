package insights

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/config"
)

func stayinData(pool config.UpstreamPool, rc config.RouteCache, staticRoot string) InsightData {
	route := config.Route{Name: "cms", Pool: pool.Name, Cache: rc}
	route.Static.Root = staticRoot
	return InsightData{
		Config: &config.Config{
			UpstreamPools: []config.UpstreamPool{pool},
			Routes:        []config.Route{route},
		},
	}
}

// TestRule_StayinAliveNoHealthSignal pins issue #933's dashboard half:
// a stayin_alive route on a pool without any health signal raises the
// advisory; a pool with passive or active health does not.
func TestRule_StayinAliveNoHealthSignal(t *testing.T) {
	t.Parallel()
	rc := config.RouteCache{StayinAlive: true}

	noSignal := config.UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	ins := ruleCacheStayinAliveNoHealthSignal(stayinData(noSignal, rc, ""))
	require.NotNil(t, ins)
	assert.Equal(t, "cache-stayinalive-no-health-signal", ins.ID)
	assert.Equal(t, []string{"cms"}, ins.Routes)

	passive := config.UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	passive.Health.Passive.Consecutive5xx = 5
	assert.Nil(t, ruleCacheStayinAliveNoHealthSignal(stayinData(passive, rc, "")))

	active := config.UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	active.Health.Active.Path = "/healthz"
	assert.Nil(t, ruleCacheStayinAliveNoHealthSignal(stayinData(active, rc, "")))
}

// TestRule_StayinAliveNoHealthSignal_Skips pins the non-firing cases:
// stayin_alive off, cache disabled, static route, unknown pool.
func TestRule_StayinAliveNoHealthSignal_Skips(t *testing.T) {
	t.Parallel()
	pool := config.UpstreamPool{Name: "app", Targets: []string{"a:1"}}

	assert.Nil(t, ruleCacheStayinAliveNoHealthSignal(stayinData(pool, config.RouteCache{}, "")),
		"stayin_alive off must not fire")

	off := false
	assert.Nil(t, ruleCacheStayinAliveNoHealthSignal(stayinData(pool, config.RouteCache{Enabled: &off, StayinAlive: true}, "")),
		"cache disabled must not fire")

	assert.Nil(t, ruleCacheStayinAliveNoHealthSignal(stayinData(pool, config.RouteCache{StayinAlive: true}, "/tmp")),
		"static route must not fire")

	route := config.Route{Name: "orphan", Pool: "ghost", Cache: config.RouteCache{StayinAlive: true}}
	data := InsightData{Config: &config.Config{
		UpstreamPools: []config.UpstreamPool{pool},
		Routes:        []config.Route{route},
	}}
	ins := ruleCacheStayinAliveNoHealthSignal(data)
	require.NotNil(t, ins, "unknown pool has no resolvable health signal and should fire")
	assert.Equal(t, []string{"orphan"}, ins.Routes)
}
