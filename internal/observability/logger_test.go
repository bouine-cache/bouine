package observability

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/bouine-cache/bouine/pkg/api"
)

func TestSampledLogger_AllLevelsNotSampled(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	base := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	l := NewSampledLogger(base, 0) // 0 = no sampling

	l.Info("info msg")
	l.Warn("warn msg")
	l.Error("error msg")
	l.Debug("debug msg")

	output := buf.String()
	assert.Contains(t, output, "info msg")
	assert.Contains(t, output, "warn msg")
	assert.Contains(t, output, "error msg")
	assert.Contains(t, output, "debug msg")
}

func TestSampledLogger_SampledByKey(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	base := slog.New(slog.NewTextHandler(&buf, nil))
	l := NewSampledLogger(base, 2) // 1-in-2 sampling

	l.Info("msg", "key", api.Key{0, 0, 0, 0, 0, 0, 0, 1})
	l.Info("dropped", "key", api.Key{0, 0, 0, 0, 0, 0, 0, 2})

	output := buf.String()
	assert.Contains(t, output, "msg")
}

func TestSampledLogger_SampledByCounter(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	base := slog.New(slog.NewTextHandler(&buf, nil))
	l := NewSampledLogger(base, 3) // 1-in-3 sampling

	for range 6 {
		l.Info("counter msg")
	}
	output := buf.String()
	assert.Contains(t, output, "counter msg")
	count := bytes.Count([]byte(output), []byte("counter msg"))
	assert.LessOrEqual(t, count, 3)
}

func TestSampledLogger_WarnErrorDebugNeverSampled(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	base := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	l := NewSampledLogger(base, 1000)

	l.Warn("warn msg")
	l.Error("error msg")
	l.Debug("debug msg")

	output := buf.String()
	assert.Contains(t, output, "warn msg")
	assert.Contains(t, output, "error msg")
	assert.Contains(t, output, "debug msg")
}
