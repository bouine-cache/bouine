package observability

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/observability/responsewriter"
)

func TestDataPlaneMetrics_IncrementSmugglingRejected(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	m.IncrementSmugglingRejected()
	// Should not panic.
}

func TestDataPlaneMetrics_RecordHit_NoRouteTable(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	// Without PreResolveRoutes, routeTable is nil → fallback path.
	m.RecordHit("GET", "/api", "HIT", "hot", 200, 1024, 5*time.Millisecond)
	// Should not panic.
}

func TestDataPlaneMetrics_VaryCapHitsCount(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	// Initially zero.
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
	// Should not panic.
}

func TestDataPlaneMetrics_LookupRouteMetrics_NilTable(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	rm, ok := m.lookupRouteMetrics("test")
	assert.False(t, ok)
	assert.Nil(t, rm)
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

func TestDataPlaneMetrics_BuildAccessLogAttrs(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/foo", nil)
	rec := httptest.NewRecorder()
	rw := responsewriter.Acquire(rec)
	defer responsewriter.Release(rw)
	rw.Status = 200
	rw.Bytes = 42

	attrs := m.buildAccessLogAttrs(req, rw, "HIT", 5*time.Millisecond)
	require.NotEmpty(t, attrs)
}

func TestDataPlaneMetrics_LookupRouteMetrics_NotInTable(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	// Manually set up a routeIDs map but with no routeTable entries.
	m.routeIDs = map[string]int{"test": 0}
	// routeTable is still nil — id 0 >= len(nil) → false.
	rm, ok := m.lookupRouteMetrics("test")
	assert.False(t, ok)
	assert.Nil(t, rm)
}

func TestDataPlaneMetrics_LookupRouteMetrics_Found(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	// Set up a minimal route table.
	m.routeIDs = map[string]int{"test": 0}
	m.routeTable = []*routeMetrics{nil}
	rm, ok := m.lookupRouteMetrics("test")
	assert.True(t, ok)
	assert.Nil(t, rm) // nil entry, but found
}
