package cluster

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/observability"
)

// pipelineWorkerFmt is the exact Printf format fasthttp uses when a
// PipelineClient's background worker dies and restarts. Anchored to
// fasthttp@v1.74.0 client.go:3038:
//
//	c.logger().Printf("error in PipelineClient(%q): %v", c.Addr, err)
//
// If a fasthttp upgrade changes this wording, level classification
// silently degrades to the default. Update this constant and the test
// corpus when bumping fasthttp.
const pipelineWorkerFmt = "error in PipelineClient(%q): %v"

// closedConnMsg is the substring logged when a socket write or read
// fails because the local side already closed the connection.
const fasthttpClosedConnMsg = "use of closed network connection"

// fasthttpLogger bridges fasthttp's Printf-only Logger interface into
// slog. fasthttp uses it to report background pipeline-worker errors —
// dial failures and read/write timeouts on peer connections. Without
// it these lines fall back to fasthttp's default stdlib logger and
// surface as unstructured stderr, which log collectors label as info,
// hiding degraded peers from alerting.
//
// fasthttp provides no level token, so the adapter classifies by
// substring: timeout, connection-refused, and unknown-host errors are
// degraded-but-expected (WARN — the worker redials and RPCs fall back
// to origin), while anything else (TLS misconfiguration, protocol
// bugs) is ERROR. During shutdown, socket-closed races inside
// fasthttp's worker goroutines are expected and downgraded to DEBUG.
//
// Unstable.
type fasthttpLogger struct {
	logger observability.Logger
	// closing is set by PeerFetcher.Close. Once true, "use of closed
	// network connection" errors from fasthttp's in-flight worker
	// goroutines are downgraded to DEBUG — they are an expected race
	// during teardown, not actionable diagnostics.
	closing atomic.Bool
}

// newFasthttpLogger returns a fasthttp.Logger that forwards worker
// errors to logger as structured slog records tagged with
// component=fasthttp.
func newFasthttpLogger(logger observability.Logger) *fasthttpLogger {
	return &fasthttpLogger{logger: observability.ResolveLogger(logger)}
}

// markClosing signals that the owning PeerFetcher is shutting down.
// Subsequent "use of closed network connection" worker errors are
// downgraded to DEBUG.
func (l *fasthttpLogger) markClosing() {
	l.closing.Store(true)
}

// Printf implements fasthttp.Logger.
func (l *fasthttpLogger) Printf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if l.closing.Load() && strings.Contains(msg, fasthttpClosedConnMsg) {
		l.logger.Debug(msg, "component", "fasthttp")
		return
	}
	switch {
	case strings.Contains(msg, "timeout"),
		strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "no such host"):
		l.logger.Warn(msg, "component", "fasthttp")
	default:
		l.logger.Error(msg, "component", "fasthttp")
	}
}

// interface compliance: usable directly as PipelineClient.Logger.
var _ fasthttp.Logger = (*fasthttpLogger)(nil)
