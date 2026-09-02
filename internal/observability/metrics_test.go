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

// TestDataPlaneMetrics_ReactorTelemetry verifies the api.ReactorMetrics
// capability: the five H1 reactor counters are registered on the
// registry, increments flow through, and the handoff reason label
// carries the closed api.ReactorHandoff* set.
func TestDataPlaneMetrics_ReactorTelemetry(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)
	var rm api.ReactorMetrics = m

	rm.IncrementReactorConnRegistered()
	rm.IncrementReactorHit()
	for _, reason := range []string{
		api.ReactorHandoffMiss, api.ReactorHandoffDisqualified,
		api.ReactorHandoffMalformed, api.ReactorHandoffOversize,
		api.ReactorHandoffOverflow, api.ReactorHandoffCap,
	} {
		rm.IncrementReactorHandoff(reason)
	}
	rm.IncrementReactorReturn()
	rm.IncrementReactorDrop()

	got, err := reg.Gather()
	require.NoError(t, err, "gather")

	sums := map[string]float64{}
	handoffs := map[string]float64{}
	for _, mf := range got {
		var sum float64
		for _, metric := range mf.GetMetric() {
			sum += metric.GetCounter().GetValue()
			if mf.GetName() == "bouine_h1_reactor_handoffs_total" {
				for _, lp := range metric.GetLabel() {
					if lp.GetName() == "reason" {
						handoffs[lp.GetValue()] = metric.GetCounter().GetValue()
					}
				}
			}
		}
		sums[mf.GetName()] = sum
	}

	for name, want := range map[string]float64{
		"bouine_h1_reactor_conns_registered_total": 1,
		"bouine_h1_reactor_hits_total":             1,
		"bouine_h1_reactor_returns_total":          1,
		"bouine_h1_reactor_conns_dropped_total":    1,
		"bouine_h1_reactor_handoffs_total":         6,
	} {
		require.Equal(t, want, sums[name], "counter %s", name)
	}
	require.Equal(t, map[string]float64{
		"miss":         1,
		"disqualified": 1,
		"malformed":    1,
		"oversize":     1,
		"overflow":     1,
		"cap":          1,
	}, handoffs, "the closed handoff reason set must each appear once")
}
