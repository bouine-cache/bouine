package observability

import (
	"fmt"
	"strings"
)

// fasthttpPermanentAcceptErr is the prefix fasthttp logs when the accept
// loop hit a permanent error and Serve is about to return — the listener
// is dying, not degraded. Anchored to fasthttp@v1.74.0 server.go:2137:
//
//	s.logger().Printf("Permanent error when accepting new connections: %v", err)
const fasthttpPermanentAcceptErr = "Permanent error when accepting new connections"

// FastHTTPLogger implements fasthttp.Logger by re-emitting fasthttp's
// printf-style internal diagnostics as structured records tagged with
// component. fasthttp has no log levels; without this adapter its
// diagnostics go to a raw log.Logger on stderr, bypassing slog entirely
// and showing up as unstructured lines in log pipelines.
//
// Every message that reaches this adapter is an error-path diagnostic
// (accept failures, per-connection serve errors, resource limits), so
// records are classified from known message shapes: listener-fatal
// accept errors are forwarded at Error, everything else at Warn. Benign
// per-connection errors (broken pipe, reset by peer, i/o timeout,
// unexpected EOF, small read buffer, bad trailer) never get here —
// fasthttp filters them before calling Printf unless Server.LogAllErrors
// is set, which bouine never sets. Messages are forwarded verbatim: the
// prefixes ("error when serving connection", "Timeout error when
// accepting new connections", ...) carry the failure site.
//
// This only covers connections served by fasthttp's own worker pool.
// The H1 fast path (h1parser) bypasses it and discards per-connection
// errors by design (see server.fp_conn.reportFastPathError).
type FastHTTPLogger struct {
	logger    Logger
	component string
}

// NewFastHTTPLogger returns a fasthttp.Logger that forwards diagnostics
// to logger, tagging each record with component (e.g. "http", "https",
// "admin") so operators can tell which serving surface produced them.
// A nil logger is resolved to NoopLogger.
func NewFastHTTPLogger(logger Logger, component string) FastHTTPLogger {
	return FastHTTPLogger{logger: ResolveLogger(logger), component: component}
}

// Printf implements fasthttp.Logger with log.Printf semantics. It is
// only called on fasthttp error paths, never on the hot path.
func (l FastHTTPLogger) Printf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if strings.HasPrefix(msg, fasthttpPermanentAcceptErr) {
		l.logger.Error(msg, "component", l.component)
		return
	}
	l.logger.Warn(msg, "component", l.component)
}
