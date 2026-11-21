package insights

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/origin"
	"github.com/bouine-cache/bouine/pkg/api"
)

func graceHoldData(holds, prevHolds int64, poolHealthy bool) InsightData {
	return InsightData{
		Config: &config.Config{
			Routes: []config.Route{{
				Name:  "cms",
				Pool:  "origin",
				Cache: config.RouteCache{StayinAlive: true},
			}},
		},
		PoolHealth: map[string][]origin.TargetStatus{
			"origin": {{Addr: "a:1", Healthy: poolHealthy}},
		},
		StoreStats:     api.Stats{ReaperGraceHolds: holds},
		PrevStoreStats: api.Stats{ReaperGraceHolds: prevHolds},
	}
}

// TestRule_GraceHoldsStaleEjection pins issue #937: grace holds while
// every stayin_alive pool target is healthy => stale ejection insight.
func TestRule_GraceHoldsStaleEjection(t *testing.T) {
	t.Parallel()
	ins := ruleCacheGraceHoldsStaleEjection(graceHoldData(5, 0, true))
	require.NotNil(t, ins)
	assert.Equal(t, "cache-grace-holds-stale-ejection", ins.ID)
	assert.Equal(t, SeverityHigh, ins.Severity)

	// No holds delta -> silent.
	assert.Nil(t, ruleCacheGraceHoldsStaleEjection(graceHoldData(5, 5, true)))
	// Any ejected target -> not a stale ejection.
	assert.Nil(t, ruleCacheGraceHoldsStaleEjection(graceHoldData(5, 0, false)))
	// No stayin_alive routes -> silent.
	d := graceHoldData(5, 0, true)
	d.Config.Routes[0].Cache.StayinAlive = false
	assert.Nil(t, ruleCacheGraceHoldsStaleEjection(d))
}

// TestRule_GraceHoldsNoEjection pins the companion rule: holds with no
// ejected target at all.
func TestRule_GraceHoldsNoEjection(t *testing.T) {
	t.Parallel()
	ins := ruleCacheGraceHoldsNoEjection(graceHoldData(3, 0, true))
	require.NotNil(t, ins)
	assert.Equal(t, "cache-grace-holds-no-ejection", ins.ID)
	assert.Equal(t, SeverityMed, ins.Severity)

	assert.Nil(t, ruleCacheGraceHoldsNoEjection(graceHoldData(3, 3, true)))
	assert.Nil(t, ruleCacheGraceHoldsNoEjection(graceHoldData(3, 0, false)),
		"any ejected target means the other rule owns the signal")
}
