package server

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"

	"github.com/bouine-cache/bouine/internal/platform"
	"github.com/bouine-cache/bouine/internal/server/h1parser"
)

// serveFastPath accepts connections and routes them to the h1parser
// for zero-alloc cache-hit serving. On miss, the h1parser calls the
// fallback handler (fasthttp.RequestHandler) directly — no byte
// reconstruction or net/http handoff needed.
func (s *Listener) serveFastPath(ctx context.Context, ln net.Listener) error {
	scheme := s.scheme
	if scheme == "" {
		scheme = "http"
	}

	parser := h1parser.New(
		s.fastPath,
		s.inner.Handler,
		h1parser.WithScheme(scheme),
		// CoarseNow: ~2-4ns vs ~25-40ns for time.Now on Linux. The 1ms
		// clock resolution is sufficient — deadlines are second-scale.
		h1parser.WithNowFunc(platform.CoarseNow),
		h1parser.WithIdleReadTimeout(s.idleTimeout),
		h1parser.WithWriteTimeout(safetyNetWriteTimeout),
		h1parser.WithMetricsHook(s.fastMetrics.RecordHit),
		h1parser.WithSmugglingHook(s.fastMetrics.IncrementSmugglingRejected),
	)

	var wg sync.WaitGroup

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			continue
		}

		wg.Add(1)
		go func(c net.Conn) { //nolint:contextcheck // parser manages its own deadlines
			defer wg.Done()
			s.handleFastPathConn(c, parser)
		}(conn)
	}

	wg.Wait()
	return nil
}

// handleFastPathConn routes a single accepted connection to the h1parser.
// For TLS connections, the handshake is performed first. All connections
// go to the h1parser — HTTP/2 is not supported.
func (s *Listener) handleFastPathConn(conn net.Conn, parser *h1parser.Parser) {
	defer func() { _ = conn.Close() }()

	if parser == nil {
		return
	}

	if tlsConn, ok := conn.(*tls.Conn); ok {
		if err := tlsConn.HandshakeContext(context.Background()); err != nil {
			return
		}
	}

	// Errors from the parser are per-connection: EOF, closed, malformed
	// request, timeout, smuggling detection, write failure. None are
	// listener-level failures.
	_ = parser.Serve(conn) //nolint:contextcheck // parser manages its own deadlines
}

// serveMultiFastPath runs the fast-path accept loop across multiple
// SO_REUSEPORT listeners. Called from serveMulti when the fast path is enabled.
func (s *Listener) serveMultiFastPath(ctx context.Context, listeners []net.Listener) error {
	var wg sync.WaitGroup
	for _, ln := range listeners {
		wg.Add(1)
		go func(l net.Listener) {
			defer wg.Done()
			_ = s.serveFastPath(ctx, l)
		}(ln)
	}

	wg.Wait()
	return nil
}
