package wal

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestRegisterMetrics_AllRegistered(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	require.NotNil(t, m.WriteDuration)
	require.NotNil(t, m.WriteQueueDepth)
	require.NotNil(t, m.WriteTotal)

	families, err := reg.Gather()
	require.NoError(t, err)
	names := make(map[string]bool)
	for _, f := range families {
		names[f.GetName()] = true
	}
	require.True(t, names["bouine_wal_write_duration_seconds"])
	require.True(t, names["bouine_wal_write_queue_depth"])
	require.True(t, names["bouine_wal_write_total"])
}

func TestRegisterMetrics_NilRegistryIsSafe(t *testing.T) {
	m := RegisterMetrics(nil)
	m.ObserveWriteDuration(time.Millisecond)
	m.SetQueueDepth(42)
	m.IncWriteTotal(1)
}

func TestMetrics_WriteDurationObserved(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	m.ObserveWriteDuration(5 * time.Millisecond)

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "bouine_wal_write_duration_seconds" {
			require.NotEmpty(t, f.Metric)
			require.Greater(t, f.Metric[0].GetHistogram().GetSampleCount(), uint64(0))
			return
		}
	}
	require.Fail(t, "wal_write_duration_seconds not found")
}

func TestMetrics_QueueDepthSet(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	m.SetQueueDepth(100)

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "bouine_wal_write_queue_depth" {
			require.NotEmpty(t, f.Metric)
			require.Equal(t, float64(100), f.Metric[0].GetGauge().GetValue())
			return
		}
	}
	require.Fail(t, "wal_write_queue_depth not found")
}

func TestMetrics_WriteTotalIncremented(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	m.IncWriteTotal(5)
	m.IncWriteTotal(3)

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "bouine_wal_write_total" {
			require.NotEmpty(t, f.Metric)
			require.Equal(t, float64(8), f.Metric[0].GetCounter().GetValue())
			return
		}
	}
	require.Fail(t, "wal_write_total not found")
}

func TestLog_QueueDepth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	l, err := OpenAsync(path, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	require.Equal(t, 0, l.QueueDepth())

	for range 10 {
		require.NoError(t, l.Enqueue(Entry{Op: opPut}))
	}
	// With a 1-hour sync interval the drain loop cannot have ticked,
	// so all 10 entries must still be buffered in the channel.
	depth := l.QueueDepth()
	require.Equal(t, 10, depth, "queue depth should reflect all buffered entries")
}

func TestLog_SetMetrics_NilSafe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	l, err := OpenAsync(path, 50*time.Millisecond)
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	// Setting nil metrics should not panic.
	l.SetMetrics(nil)
	require.NoError(t, l.Enqueue(Entry{Op: opPut}))
}

func TestLog_WriteTotalIncrementedOnEnqueue(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	path := filepath.Join(t.TempDir(), "test.wal")
	l, err := OpenAsync(path, 50*time.Millisecond)
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	l.SetMetrics(m)

	for range 5 {
		require.NoError(t, l.Enqueue(Entry{Op: opPut}))
	}

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "bouine_wal_write_total" {
			require.NotEmpty(t, f.Metric)
			require.GreaterOrEqual(t, f.Metric[0].GetCounter().GetValue(), float64(5))
			return
		}
	}
	require.Fail(t, "wal_write_total not found")
}

func TestLog_WriteDurationObservedOnSync(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)

	path := filepath.Join(t.TempDir(), "test.wal")
	l, err := OpenAsync(path, 20*time.Millisecond)
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	l.SetMetrics(m)

	require.NoError(t, l.Enqueue(Entry{Op: opPut}))
	time.Sleep(100 * time.Millisecond) // wait for sync loop to drain

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == "bouine_wal_write_duration_seconds" {
			require.NotEmpty(t, f.Metric)
			var hist *dto.Histogram
			if h := f.Metric[0].GetHistogram(); h != nil {
				hist = h
			}
			require.NotNil(t, hist, "histogram should have observations")
			require.Greater(t, hist.GetSampleCount(), uint64(0))
			return
		}
	}
	require.Fail(t, "wal_write_duration_seconds not found")
}
