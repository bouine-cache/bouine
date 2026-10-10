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
//
// Matched with strings.HasPrefix (fasthttp appends the wrapped error).
const fasthttpPermanentAcceptErr = "Permanent error when accepting new connections"

// fasthttpPipelineErr is the prefix of the only message the
// fasthttp.PipelineClient worker logs (client.go:3038):
//
//	c.logger().Printf("error in PipelineClient(%q): %v", c.Addr, err)
//
// What follows is the %q-quoted peer address, then `"): "`, then the
// formatted transport error.
const fasthttpPipelineErr = "error in PipelineClient("

// fasthttpClientDebugErrs are client-side transport errors that are
// routine connection teardown or bouine's own retirement mechanics:
// they carry no operator action and are forwarded at Debug so they
// stay out of log pipelines at the production Info floor. EOF (and
// its mid-response "unexpected EOF" variant) is the peer closing the
// connection; broken pipe is the local write half hitting that closed
// socket. Both are the steady drip of rolling restarts and idle
// reaping. "peer address retired" is the PeerFetcher's parked dial
// returning after Close released it: the pipeline worker's bounded
// 1 Hz restart loop then spins until the process exits by design.
// Sustained peer degradation is not tracked through logs; the
// per-address failure breaker owns it (bouine_peer_blacklisted).
var fasthttpClientDebugErrs = []string{
	"peer address retired",
	"EOF",
	"write: broken pipe",
}

// isFastHTTPClientDebugErr reports whether a client-side pipeline
// error is routine teardown noise (Debug) rather than a degraded
// peer (Warn).
func isFastHTTPClientDebugErr(errText string) bool {
	for _, s := range fasthttpClientDebugErrs {
		if strings.Contains(errText, s) {
			return true
		}
	}
	return false
}

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
	// clientSide marks adapters attached to fasthttp clients
	// (PipelineClient workers) instead of servers. Client messages
	// carry an inner transport error that classifies the record:
	// routine teardown noise logs at Debug, degraded-peer failures at
	// Warn. Server messages have no such inner error and stay at
	// Warn unless the accept loop is dying (Error).
	clientSide bool
}

// NewFastHTTPLogger returns a fasthttp.Logger that forwards diagnostics
// to logger, tagging each record with component (e.g. "http", "https",
// "admin") so operators can tell which serving surface produced them.
// A nil logger is resolved to NoopLogger.
func NewFastHTTPLogger(logger Logger, component string) FastHTTPLogger {
	return FastHTTPLogger{logger: ResolveLogger(logger), component: component}
}

// NewFastHTTPClientLogger returns a fasthttp.Logger for fasthttp client
// structs (PipelineClient). It forwards the pipeline worker's error
// diagnostics to logger, tagging each record with component
// (e.g. "cluster"). Unlike server diagnostics, every one of which
// means this process is failing to serve, client errors range from
// routine peer-connection teardown to a dead peer: known-benign
// transport errors log at Debug and the rest at Warn. A nil logger
// is resolved to NoopLogger.
func NewFastHTTPClientLogger(logger Logger, component string) FastHTTPLogger {
	return FastHTTPLogger{logger: ResolveLogger(logger), component: component, clientSide: true}
}

// Printf implements fasthttp.Logger with log.Printf semantics. It is
// only called on fasthttp error paths, never on the hot path.
func (l FastHTTPLogger) Printf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if strings.HasPrefix(msg, fasthttpPermanentAcceptErr) {
		l.logger.Error(msg, "component", l.component)
		return
	}
	if l.clientSide {
		// The PipelineClient worker logs one shape (fasthttpPipelineErr):
		// the peer address is %q-quoted, so the first `"): "` after it
		// separates the address from the formatted transport error.
		// Unknown transport errors (dial refused, timeouts, TLS failures)
		// mean a degraded peer and stay at Warn; never silence a client
		// error shape we don't recognize.
		if rest, ok := strings.CutPrefix(msg, fasthttpPipelineErr); ok {
			if _, errText, ok := strings.Cut(rest, "\"): "); ok {
				if isFastHTTPClientDebugErr(errText) {
					l.logger.Debug(msg, "component", l.component)
					return
				}
			}
		}
	}
	l.logger.Warn(msg, "component", l.component)
}
