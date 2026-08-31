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
//
// When the reactor is enabled and available (Linux, epoll), the
// accept loop hands the listener to a single-goroutine event loop
// that batch-serves cache hits (see reactor.go); every non-hit
// connection is handed off to the blocking path, so behavior is
// unchanged except for how hits are scheduled.
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

	if s.h1Reactor {
		if loop, ok := h1parser.NewReactorLoop(parser, ln); ok {
			s.logger.Info("H1 reactor enabled (epoll batch hit serving)",
				"name", s.name, "addr", ln.Addr().String())
			loop.Run()
			return nil
		}
		s.logger.Warn("h1_reactor requested but unavailable on this platform; using blocking path",
			"name", s.name)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 4)

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
			s.handleFastPathConn(c, parser, errCh)
		}(conn)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil && !errors.Is(err, net.ErrClosed) {
			return err
		}
	}
	return nil
}

// handleFastPathConn routes a single accepted connection to the h1parser.
// For TLS connections, the handshake is performed first. All connections
// go to the h1parser — HTTP/2 is not supported.
func (s *Listener) handleFastPathConn(conn net.Conn, parser *h1parser.Parser, errCh chan<- error) {
	defer func() { _ = conn.Close() }()

	if parser == nil {
		return
	}

	if tlsConn, ok := conn.(*tls.Conn); ok {
		if err := tlsConn.HandshakeContext(context.Background()); err != nil {
			return
		}
	}

	if err := parser.Serve(conn); err != nil { //nolint:contextcheck // parser manages its own deadlines
		reportFastPathError(err, errCh)
	}
}

// reportFastPathError handles errors from parser.Serve. All errors from
// the parser are per-connection: EOF, closed, malformed request, timeout,
// smuggling detection, write failure. None are listener-level failures.
func reportFastPathError(_ error, _ chan<- error) {}

// serveMultiFastPath runs the fast-path accept loop across multiple
// SO_REUSEPORT listeners. Called from serveMulti when the fast path is enabled.
func (s *Listener) serveMultiFastPath(ctx context.Context, listeners []net.Listener) error {
	multiCtx, multiCancel := context.WithCancel(ctx)
	defer multiCancel()

	errCh := make(chan error, len(listeners))
	var wg sync.WaitGroup
	for _, ln := range listeners {
		wg.Add(1)
		go func(l net.Listener) {
			defer wg.Done()
			if err := s.serveFastPath(multiCtx, l); err != nil && !errors.Is(err, net.ErrClosed) {
				errCh <- err
			}
		}(ln)
	}

	var firstErr error
	select {
	case <-ctx.Done():
	case firstErr = <-errCh:
		multiCancel()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
