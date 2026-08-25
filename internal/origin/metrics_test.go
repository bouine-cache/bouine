package origin

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestRegisterMetrics_AllRegistered(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	require.NotNil(t, m.passiveErrors)
	require.NotNil(t, m.ActiveConnections)
	require.NotNil(t, m.RequestDuration)
	require.NotNil(t, m.ConnectionErrors)

	// Initialize vec metrics so they appear in gather output.
	m.passiveErrors.WithLabelValues("p", "t", "200")
	m.ActiveConnections.WithLabelValues("p", "t")
	m.RequestDuration.WithLabelValues("p", "t", "200")
	m.ConnectionErrors.WithLabelValues("p", "t", "timeout")

	families, err := reg.Gather()
	require.NoError(t, err)
	names := make(map[string]bool)
	for _, f := range families {
		names[f.GetName()] = true
	}
	require.True(t, names["bouine_origin_passive_errors_total"])
	require.True(t, names["bouine_origin_active_connections"])
	require.True(t, names["bouine_origin_request_duration_seconds"])
	require.True(t, names["bouine_origin_connection_errors_total"])
}

func TestRegisterMetrics_NilRegistryIsSafe(t *testing.T) {
	t.Parallel()
	m := RegisterMetrics(nil)
	m.incPassiveError("pool", "target", "502")
	m.incActiveConnection("pool", "target")
	m.decActiveConnection("pool", "target")
	m.observeRequestDuration("pool", "target", "200", 0.1)
	m.incConnectionError("pool", "target", "timeout")
}

func TestMetrics_PassiveErrorWithStatus(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	m.incPassiveError("pool1", "target1", "502")
	m.incPassiveError("pool1", "target1", "503")
	m.incPassiveError("pool1", "target1", "502")

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "bouine_origin_passive_errors_total" {
			require.Len(t, f.Metric, 2) // two label sets: 502 and 503
			var total502, total503 float64
			for _, met := range f.Metric {
				status := ""
				for _, l := range met.Label {
					if l.GetName() == "status" {
						status = l.GetValue()
					}
				}
				switch status {
				case "502":
					total502 = met.GetCounter().GetValue()
				case "503":
					total503 = met.GetCounter().GetValue()
				}
			}
			require.Equal(t, float64(2), total502)
			require.Equal(t, float64(1), total503)
			return
		}
	}
	require.Fail(t, "origin_passive_errors_total not found")
}

func TestMetrics_ActiveConnectionIncDec(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	m.incActiveConnection("p", "t")
	m.incActiveConnection("p", "t")
	m.decActiveConnection("p", "t")

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "bouine_origin_active_connections" {
			require.NotEmpty(t, f.Metric)
			require.Equal(t, float64(1), f.Metric[0].GetGauge().GetValue())
			return
		}
	}
	require.Fail(t, "origin_active_connections not found")
}

func TestMetrics_RequestDurationObserved(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	m.observeRequestDuration("p", "t", "200", 0.05)

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "bouine_origin_request_duration_seconds" {
			require.NotEmpty(t, f.Metric)
			require.Equal(t, uint64(1), f.Metric[0].GetHistogram().GetSampleCount())
			return
		}
	}
	require.Fail(t, "origin_request_duration_seconds not found")
}

func TestMetrics_ConnectionErrorIncremented(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	m.incConnectionError("p", "t", "timeout")
	m.incConnectionError("p", "t", "timeout")
	m.incConnectionError("p", "t", "refused")

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "bouine_origin_connection_errors_total" {
			require.Len(t, f.Metric, 2) // timeout and refused
			return
		}
	}
	require.Fail(t, "origin_connection_errors_total not found")
}

// TestMetrics_ObserveDurationNonNil ensures observeRequestDuration
// doesn't panic with a zero duration.
func TestMetrics_ObserveDurationZero(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	m.observeRequestDuration("p", "t", "200", 0)
	_ = reg // reg is used to ensure metrics are initialized
}
