package observability

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

func TestSampledByKeyDeterminism(t *testing.T) {
	t.Parallel()
	l := NewSampledLogger(nil, 100)
	for i := range 1000 {
		key := testkey.Key(uint64(i * 7))
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
		if l.sampledByKey(testkey.Key(uint64(i))) {
			sampled++
		}
	}
	require.Equal(t, 100, sampled)
}

func TestSampledByKeyZeroRateAlwaysLogs(t *testing.T) {
	t.Parallel()
	l := NewSampledLogger(nil, 0)
	for i := range 100 {
		require.True(t, l.sampledByKey(testkey.Key(uint64(i))))
	}
}

func TestSampledByKeyDistribution(t *testing.T) {
	t.Parallel()
	l := NewSampledLogger(nil, 100)
	buckets := make(map[int]int)
	for i := range 100000 {
		if l.sampledByKey(testkey.Key(uint64(i))) {
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
	return l.sampleRate == 0 || key.Hash64()%l.sampleRate == 0
}

// sampledByCounter is a test helper that exposes the counter-based sampling decision.
func (l *SampledLogger) sampledByCounter() bool {
	return l.sampleRate == 0 || l.counter.Add(1)%l.sampleRate == 0
}

func TestInfoZeroRateAlwaysLogs(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	l := NewSampledLogger(slog.New(slog.NewJSONHandler(&buf, nil)), 0)
	l.Info("test message", "key", testkey.Key(42))
	require.NotEqual(t, 0, buf.Len())
	var rec map[string]any
	err := json.Unmarshal(buf.Bytes(), &rec)
	require.NoError(t, err, "unmarshal")
	assert.Equal(t, "test message", rec["msg"])
}

func TestInfoKeyFilteredBySampling(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	l := NewSampledLogger(slog.New(slog.NewJSONHandler(&buf, nil)), 100)
	l.Info("filtered", "key", testkey.Key(1))
	require.Equal(t, 0, buf.Len())
	l.Info("passes", "key", testkey.Key(100))
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

func TestResolveLogger_Nil(t *testing.T) {
	t.Parallel()
	l := ResolveLogger(nil)
	assert.NotNil(t, l)
	assert.IsType(t, NoopLogger{}, l)
}

func TestResolveLogger_NonNil(t *testing.T) {
	t.Parallel()
	nl := NoopLogger{}
	l := ResolveLogger(nl)
	assert.Equal(t, nl, l)
}

func TestNoopLogger_AllMethods(t *testing.T) {
	t.Parallel()
	l := NoopLogger{}
	l.Info("test")
	l.Warn("test")
	l.Error("test")
	l.Debug("test")
}

// TestSampledLogger_WarnNeverSampled verifies Warn is never sampled.
func TestSampledLogger_WarnNeverSampled(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	base := slog.New(slog.NewTextHandler(&buf, nil))
	l := NewSampledLogger(base, 2) // 1-in-2 sampling
	l.Warn("warn-msg", "key", "val")
	assert.Contains(t, buf.String(), "warn-msg")
}

// TestSampledLogger_ErrorNeverSampled verifies Error is never sampled.
func TestSampledLogger_ErrorNeverSampled(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	base := slog.New(slog.NewTextHandler(&buf, nil))
	l := NewSampledLogger(base, 2)
	l.Error("error-msg", "key", "val")
	assert.Contains(t, buf.String(), "error-msg")
}

// TestSampledLogger_DebugNeverSampled verifies Debug is never sampled.
func TestSampledLogger_DebugNeverSampled(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	base := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	l := NewSampledLogger(base, 2)
	l.Debug("debug-msg", "key", "val")
	assert.Contains(t, buf.String(), "debug-msg")
}

// TestSampledLogger_InfoZeroRateAlwaysLogs verifies that sampleRate=0
// disables sampling — every Info call is logged.
func TestSampledLogger_InfoZeroRateAlwaysLogs(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	base := slog.New(slog.NewTextHandler(&buf, nil))
	l := NewSampledLogger(base, 0)
	for i := range 10 {
		l.Info("msg", "key", testkey.Key(uint64(i)))
	}
	// All 10 should be logged — count newlines.
	lines := bytes.Count(buf.Bytes(), []byte{'\n'})
	assert.Equal(t, 10, lines)
}
