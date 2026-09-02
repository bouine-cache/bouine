package observability

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataPlaneMetrics_IncrementSmugglingRejected(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.IncrementSmugglingRejected()
}

func TestDataPlaneMetrics_RecordHit_NoRouteTable(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.RecordHit("GET", "/api", "HIT", "hot", 200, 1024, 5*time.Millisecond)
}

func TestDataPlaneMetrics_VaryCapHitsCount(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	assert.Equal(t, int64(0), m.VaryCapHitsCount())
}

func TestDataPlaneMetrics_VaryCapHitsCount_Nil(t *testing.T) {
	t.Parallel()
	var m *DataPlaneMetrics
	assert.Equal(t, int64(0), m.VaryCapHitsCount())
}

func TestDataPlaneMetrics_CFPurgeSkippedCount(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	assert.Equal(t, int64(0), m.CFPurgeSkippedCount())
}

func TestDataPlaneMetrics_CFPurgeSkippedCount_Nil(t *testing.T) {
	t.Parallel()
	var m *DataPlaneMetrics
	assert.Equal(t, int64(0), m.CFPurgeSkippedCount())
}

func TestRefreshMetrics_NilSafe(t *testing.T) {
	t.Parallel()
	var m *RefreshMetrics
	fr := m.ForRoute("test")
	fr.IncTotal("ok")
	fr.IncErrors("timeout")
	fr.IncSkips("disabled")
	fr.IncInFlight()
	fr.DecInFlight()
	fr.SetScheduled(5)
	fr.SetRegistrySize(10)
}

func TestRefreshMetrics_ForRoute_WithMetrics(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	fr := m.RefreshMetricsVec().ForRoute("api")
	fr.IncTotal("ok")
	fr.IncErrors("timeout")
	fr.IncSkips("disabled")
	fr.IncInFlight()
	fr.DecInFlight()
	fr.SetScheduled(5)
	fr.SetRegistrySize(10)
}

func TestRefreshMetricsVec(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.RefreshMetricsVec()
}

func TestDataPlaneMetrics_LookupPoolMetrics_NilTable(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	pm, ok := m.lookupPoolMetrics("test")
	assert.False(t, ok)
	assert.Nil(t, pm)
}

func TestDataPlaneMetrics_AccessLogMessage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		cacheResult string
		status      int
		want        string
	}{
		{"HIT", 200, "served cache hit"},
		{"MISS", 200, "served cache miss"},
		{"BYPASS", 200, "bypassed cache"},
		{"STALE", 200, "served stale response"},
		{"REVALIDATED", 200, "served revalidated response"},
		{"", 200, "served uncached response"},
		{"UNKNOWN", 200, "served response (unknown cache status)"},
		{"HIT", 500, "request completed with error"},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.want, accessLogMessage(tc.cacheResult, tc.status))
	}
}

func TestDataPlaneMetrics_LookupPoolMetrics_NotInTable(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.poolIDs = map[string]int{"test": 0}
	pm, ok := m.lookupPoolMetrics("test")
	assert.False(t, ok)
	assert.Nil(t, pm)
}

func TestDataPlaneMetrics_LookupPoolMetrics_Found(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.poolIDs = map[string]int{"test": 0}
	m.poolTable = []*poolMetrics{nil}
	pm, ok := m.lookupPoolMetrics("test")
	assert.True(t, ok)
	assert.Nil(t, pm)
}

func TestDataPlaneMetrics_TierMaxBytesGauges(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.HotStoreMaxBytes.Set(float64(1 << 30))  // 1 GiB
	m.WarmStoreMaxBytes.Set(float64(2 << 30)) // 2 GiB

	mfs, err := reg.Gather()
	assert.NoError(t, err)
	names := make(map[string]float64)
	for _, mf := range mfs {
		switch mf.GetName() {
		case "bouine_hot_store_max_bytes":
			names["hot"] = mf.GetMetric()[0].GetGauge().GetValue()
		case "bouine_warm_store_max_bytes":
			names["warm"] = mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	assert.Equal(t, float64(1<<30), names["hot"])
	assert.Equal(t, float64(2<<30), names["warm"])
}

func TestDataPlaneMetrics_MetricsResetTotal(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.MetricsResetTotal.Inc()

	mfs, err := reg.Gather()
	assert.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == "bouine_metrics_reset_total" {
			assert.Equal(t, float64(1), mf.GetMetric()[0].GetCounter().GetValue())
			return
		}
	}
	assert.Fail(t, "metrics_reset_total not found")
}

func TestDataPlaneMetrics_RequestQueueDepth(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.RequestQueueDepth.Inc()
	m.RequestQueueDepth.Inc()
	m.RequestQueueDepth.Dec()

	mfs, err := reg.Gather()
	assert.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == "bouine_request_queue_depth" {
			assert.Equal(t, float64(1), mf.GetMetric()[0].GetGauge().GetValue())
			return
		}
	}
	assert.Fail(t, "request_queue_depth not found")
}

// TestFastPath_RecordHit_EmptyPoolMapsToDefault verifies that
// fast-path hits arriving with pool="" (the engine-level fast path
// carries no pool attribution) are recorded on the pre-resolved
// _default pool metrics instead of falling through to WithLabelValues,
// and that the upstream_pool label is normalized to _default.
func TestFastPath_RecordHit_EmptyPoolMapsToDefault(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.PreResolveRoutes(nil)

	m.RecordHit("GET", "", "HIT", "hot", 200, 1024, 5*time.Millisecond)

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, fam := range families {
		if fam.GetName() != "bouine_requests_total" {
			continue
		}
		for _, met := range fam.GetMetric() {
			labels := met.GetLabel()
			pool := ""
			for _, l := range labels {
				if l.GetName() == "upstream_pool" {
					pool = l.GetValue()
				}
			}
			if pool == "" {
				t.Errorf("fast-path hit with empty pool must be labelled _default, got upstream_pool=\"\"")
			}
		}
	}
}
