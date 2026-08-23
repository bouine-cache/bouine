package observability

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

func TestNewMetrics_Defaults(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	require.NotNil(t, m.Registry)
	got, err := m.Registry.Gather()
	require.NoError(t, err, "gather")
	require.NotEmpty(t, got)
}

func TestMetrics_Handler_ExposesRegisteredCollector(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "bouine_test_counter",
		Help: "test",
	})
	m.Registry.MustRegister(counter)
	counter.Inc()

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/metrics")
	ctx.Request.Header.SetMethod("GET")
	m.Handler()(ctx)

	require.Equal(t, 200, ctx.Response.StatusCode())
	body := string(ctx.Response.Body())
	require.Contains(t, body, "bouine_test_counter 1")
}

func TestNewStartupMetrics_NilRegistry(t *testing.T) {
	t.Parallel()
	m := NewStartupMetrics(nil)
	require.NotNil(t, m)
	require.NotNil(t, m.StartupPhase)
	require.NotNil(t, m.StartupConditionReady)
	require.NotNil(t, m.StartupDurationSeconds)
}

func TestNewStartupMetrics_WithRegistry(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewStartupMetrics(reg)
	require.NotNil(t, m)
}

func TestSetPhase(t *testing.T) {
	t.Parallel()
	m := NewStartupMetrics(nil)
	tests := []struct {
		phase string
		val   float64
	}{
		{"init", 0},
		{"loading_warm", 1},
		{"loading_wal", 2},
		{"recompute_stats", 3},
		{"cluster_join", 4},
		{"ready", 5},
	}
	for _, tt := range tests {
		m.SetPhase(tt.phase)
	}
	// Unknown phase should be a no-op.
	m.SetPhase("unknown")
}

func TestSetPhase_NilMetrics(t *testing.T) {
	t.Parallel()
	var m *StartupMetrics
	m.SetPhase("ready") // must not panic
}

func TestSetCondition(t *testing.T) {
	t.Parallel()
	m := NewStartupMetrics(nil)
	m.SetCondition("warm_loaded", true)
	m.SetCondition("wal_loaded", false)
}

func TestSetCondition_NilMetrics(t *testing.T) {
	t.Parallel()
	var m *StartupMetrics
	m.SetCondition("test", true) // must not panic
}

func TestObserveStartupDuration(t *testing.T) {
	t.Parallel()
	m := NewStartupMetrics(nil)
	m.ObserveStartupDuration(1.5)
}

func TestObserveStartupDuration_NilMetrics(t *testing.T) {
	t.Parallel()
	var m *StartupMetrics
	m.ObserveStartupDuration(1.5) // must not panic
}

func TestSetNowFunc_Default(t *testing.T) {
	t.Parallel()
	m := &DataPlaneMetrics{}
	m.SetNowFunc(nil)
	// Should default to time.Now — just verify it doesn't panic.
	_ = m.nowFunc
}

func TestSetNowFunc_Custom(t *testing.T) {
	t.Parallel()
	m := &DataPlaneMetrics{}
	called := false
	m.SetNowFunc(func() (t time.Time) {
		called = true
		return
	})
	_ = m.nowFunc()
	assert.True(t, called)
}

func TestSetAccessLog(t *testing.T) {
	t.Parallel()
	m := &DataPlaneMetrics{}
	m.SetAccessLog(NoopLogger{}, 100)
	// Zero key with rate=100 uses counter: 1%100 != 0 → false.
	assert.False(t, m.shouldLogAccess(api.Key{}))
	// Zero rate always logs.
	m2 := &DataPlaneMetrics{}
	m2.SetAccessLog(NoopLogger{}, 0)
	assert.True(t, m2.shouldLogAccess(api.Key{}))
}

func TestRefreshMetrics_ForRoute_Nil(t *testing.T) {
	t.Parallel()
	var m *RefreshMetrics
	rm := m.ForRoute("test")
	rm.IncTotal("304")
	rm.IncErrors("timeout")
	rm.IncSkips("not_found")
	rm.IncInFlight()
	rm.DecInFlight()
	rm.SetScheduled(5)
	rm.SetRegistrySize(10)
}

func TestRefreshMetricsVec_Nil(t *testing.T) {
	t.Parallel()
	var m *DataPlaneMetrics
	assert.Nil(t, m.RefreshMetricsVec())
}

func TestVaryCapHitsCount_Nil(t *testing.T) {
	t.Parallel()
	var m *DataPlaneMetrics
	assert.Equal(t, int64(0), m.VaryCapHitsCount())
}

func TestCFPurgeSkippedCount_Nil(t *testing.T) {
	t.Parallel()
	var m *DataPlaneMetrics
	assert.Equal(t, int64(0), m.CFPurgeSkippedCount())
}
