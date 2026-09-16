package observability

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureLogger records every record emitted through the Logger
// interface, preserving the level so tests can assert on it.
type captureLogger struct {
	buf    bytes.Buffer
	logger *slog.Logger
}

func newCaptureLogger() *captureLogger {
	c := &captureLogger{}
	c.logger = slog.New(slog.NewTextHandler(&c.buf, nil))
	return c
}

func (c *captureLogger) Info(msg string, args ...any)  { c.logger.Info(msg, args...) }
func (c *captureLogger) Warn(msg string, args ...any)  { c.logger.Warn(msg, args...) }
func (c *captureLogger) Error(msg string, args ...any) { c.logger.Error(msg, args...) }
func (c *captureLogger) Debug(msg string, args ...any) { c.logger.Debug(msg, args...) }

func TestFastHTTPLogger_WarnByDefault(t *testing.T) {
	t.Parallel()
	c := newCaptureLogger()
	l := NewFastHTTPLogger(c, "http")

	l.Printf("error when serving connection %q<->%q: %v", "local", "remote", "boom")

	out := c.buf.String()
	assert.Contains(t, out, "level=WARN")
	assert.Contains(t, out, "error when serving connection")
	assert.Contains(t, out, "component=http")
	assert.Contains(t, out, "boom")
}

func TestFastHTTPLogger_PermanentAcceptErrorAtError(t *testing.T) {
	t.Parallel()
	c := newCaptureLogger()
	l := NewFastHTTPLogger(c, "https")

	l.Printf("Permanent error when accepting new connections: %v", "fd storm")

	out := c.buf.String()
	assert.Contains(t, out, "level=ERROR")
	assert.Contains(t, out, "Permanent error when accepting new connections")
	assert.Contains(t, out, "component=https")
}

func TestFastHTTPLogger_TimeoutAcceptErrorAtWarn(t *testing.T) {
	t.Parallel()
	c := newCaptureLogger()
	l := NewFastHTTPLogger(c, "admin")

	l.Printf("Timeout error when accepting new connections: %v", "slow")

	out := c.buf.String()
	assert.Contains(t, out, "level=WARN")
	assert.Contains(t, out, "Timeout error when accepting new connections")
}

func TestFastHTTPLogger_NilLoggerResolvedToNoop(t *testing.T) {
	t.Parallel()
	l := NewFastHTTPLogger(nil, "http")
	require.NotPanics(t, func() {
		l.Printf("error when serving connection: %v", "boom")
	})
}
