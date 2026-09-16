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
	// Debug enabled: the client adapter classifies routine teardown
	// noise at Debug, and assertions must see those records.
	c.logger = slog.New(slog.NewTextHandler(&c.buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
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

func TestFastHTTPClientLogger_Classification(t *testing.T) {
	t.Parallel()
	// The four shapes fasthttp's pipeline worker actually emits in
	// production (prod-eu log export 2026-09-16), plus the mid-response
	// variant. Expected levels follow AGENTS.md §9: routine teardown and
	// retirement noise carry no operator action (Debug); a degraded peer
	// (refused dial, timeout) does (Warn).
	tests := []struct {
		name  string
		err   string
		level string
	}{
		{"shutdown drain of a retired address", "peer address retired", "DEBUG"},
		{"peer closed the connection", "EOF", "DEBUG"},
		{"peer closed mid-response", "unexpected EOF", "DEBUG"},
		{"write half of a closed socket", "write tcp 10.88.32.74:53124->10.88.24.67:9000: write: broken pipe", "DEBUG"},
		{"peer down, refusing dials", "dial tcp 10.88.139.76:9000: connect: connection refused", "WARN"},
		{"peer RPC timeout", "fasthttp: timeout", "WARN"},
		{"unknown transport error stays visible", "tls: handshake failure", "WARN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := newCaptureLogger()
			l := NewFastHTTPClientLogger(c, "cluster")

			l.Printf(`error in PipelineClient(%q): %v`, "10.88.24.67:9000", tt.err)

			out := c.buf.String()
			assert.Contains(t, out, "level="+tt.level)
			assert.Contains(t, out, "error in PipelineClient")
			assert.Contains(t, out, "component=cluster")
			assert.Contains(t, out, tt.err)
		})
	}
}

func TestFastHTTPClientLogger_ServerPrefixNeverDebug(t *testing.T) {
	t.Parallel()
	// A client-side adapter must not misclassify a server-shaped message:
	// if one ever flows through (adapter miswired), it stays at Warn.
	c := newCaptureLogger()
	l := NewFastHTTPClientLogger(c, "cluster")

	l.Printf("error when serving connection %q<->%q: %v", "local", "remote", "boom")

	assert.Contains(t, c.buf.String(), "level=WARN")
}

func TestFastHTTPClientLogger_NilLoggerResolvedToNoop(t *testing.T) {
	t.Parallel()
	l := NewFastHTTPClientLogger(nil, "cluster")
	require.NotPanics(t, func() {
		l.Printf(`error in PipelineClient(%q): %v`, "10.88.24.67:9000", "EOF")
	})
}
