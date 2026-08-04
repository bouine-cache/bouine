package observability

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
)

func TestSampledByKeyDeterminism(t *testing.T) {
	t.Parallel()
	l := NewSampledLogger(nil, 100)
	for i := range 1000 {
		key := api.Key(i * 7)
		first := l.sampledByKey(key)
		second := l.sampledByKey(key)
		require.Equal(t, second, first)
	}
}

func TestSampledByKeyRate(t *testing.T) {
	t.Parallel()
	l := NewSampledLogger(nil, 100)
	sampled := 0
	for i := range 10000 {
		if l.sampledByKey(api.Key(i)) {
			sampled++
		}
	}
	require.Equal(t, 100, sampled)
}

func TestSampledByKeyZeroRateAlwaysLogs(t *testing.T) {
	t.Parallel()
	l := NewSampledLogger(nil, 0)
	for i := range 100 {
		require.True(t, l.sampledByKey(api.Key(i)))
	}
}

func TestSampledByKeyDistribution(t *testing.T) {
	t.Parallel()
	l := NewSampledLogger(nil, 100)
	buckets := make(map[int]int)
	for i := range 100000 {
		if l.sampledByKey(api.Key(i)) {
			buckets[i/1000%10]++
		}
	}
	require.Len(t, buckets, 10)
}

func TestSampledByCounterRate(t *testing.T) {
	t.Parallel()
	l := NewSampledLogger(nil, 100)
	sampled := 0
	for range 10000 {
		if l.sampledByCounter() {
			sampled++
		}
	}
	require.Equal(t, 100, sampled)
}

func TestSampledByCounterZeroRateAlwaysLogs(t *testing.T) {
	t.Parallel()
	l := NewSampledLogger(nil, 0)
	for range 100 {
		require.True(t, l.sampledByCounter())
	}
}

// sampledByKey is a test helper that exposes the key-based sampling decision.
func (l *SampledLogger) sampledByKey(key api.Key) bool {
	return l.sampleRate == 0 || uint64(key)%l.sampleRate == 0
}

// sampledByCounter is a test helper that exposes the counter-based sampling decision.
func (l *SampledLogger) sampledByCounter() bool {
	return l.sampleRate == 0 || l.counter.Add(1)%l.sampleRate == 0
}

func TestInfoZeroRateAlwaysLogs(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	l := NewSampledLogger(slog.New(slog.NewJSONHandler(&buf, nil)), 0)
	l.Info("test message", "key", api.Key(42))
	require.NotEqual(t, 0, buf.Len())
	var rec map[string]any
	err := json.Unmarshal(buf.Bytes(), &rec)
	require.NoErrorf(t, err, "unmarshal: %v", err)
	assert.Equal(t, "test message", rec["msg"])
}

func TestInfoKeyFilteredBySampling(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	l := NewSampledLogger(slog.New(slog.NewJSONHandler(&buf, nil)), 100)
	l.Info("filtered", "key", api.Key(1))
	require.Equal(t, 0, buf.Len())
	l.Info("passes", "key", api.Key(100))
	require.NotEqual(t, 0, buf.Len())
}

func TestInfoNoKeyUsesCounterSampling(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	l := NewSampledLogger(slog.New(slog.NewJSONHandler(&buf, nil)), 100)
	for range 99 {
		l.Info("no key here")
	}
	require.Equal(t, 0, buf.Len())
	l.Info("no key here")
	require.NotEqual(t, 0, buf.Len())
}
