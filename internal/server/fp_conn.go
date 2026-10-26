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
// Shutdown mirrors fasthttp.Server.Shutdown (the listener's
// CloseOnShutdown path): closing the listener only unblocks Accept, so
// the tracker distinguishes idle keep-alive connections (closed
// immediately) from in-flight ones (left to finish their response, then
// closed by the conn-state hook before they park on the next read).
// wg.Wait therefore returns once in-flight responses complete instead
// of hanging until every idle deadline expires.
func (s *Listener) serveFastPath(ctx context.Context, ln net.Listener) error {
	scheme := s.scheme
	if scheme == "" {
		scheme = "http"
	}

	tracker := newConnTracker()
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
		h1parser.WithConnStateHook(tracker.stateChanged),
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

		tracker.add(conn)
		wg.Add(1)
		go func(c net.Conn) { //nolint:contextcheck // parser manages its own deadlines
			defer wg.Done()
			defer tracker.remove(c)
			s.handleFastPathConn(c, parser)
		}(conn)
	}

	// The accept loop only exits on listener close (context cancelled
	// above, or serveMultiFastPath tearing down after a sibling
	// failure), so remaining connections are idle or in-flight — never
	// future ones.
	tracker.drain()
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

	// All errors from parser.Serve are per-connection: EOF, closed,
	// malformed request, timeout, smuggling detection, write failure.
	// None are listener-level failures, so they are dropped here.
	_ = parser.Serve(conn) //nolint:contextcheck // parser manages its own deadlines
}

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

// connTracker holds the fast-path listener's live connections with their
// idle/in-flight state, so shutdown can close idle connections while
// letting in-flight responses finish (see serveFastPath). The state
// hook costs one locked map update per request transition — O(1), no
// allocation, off the hit path's zero-alloc budget.
type connTracker struct {
	conns    map[net.Conn]*connEntry
	draining bool
	mu       sync.Mutex
}

type connEntry struct {
	active bool
}

func newConnTracker() *connTracker {
	return &connTracker{conns: make(map[net.Conn]*connEntry)}
}

func (t *connTracker) add(c net.Conn) {
	t.mu.Lock()
	t.conns[c] = &connEntry{}
	t.mu.Unlock()
}

func (t *connTracker) remove(c net.Conn) {
	t.mu.Lock()
	delete(t.conns, c)
	t.mu.Unlock()
}

// stateChanged is the h1parser conn-state hook: active=true while a
// request is being served, active=false once its response is written.
// During drain, a connection going idle is closed right away so it
// never parks on the next keep-alive read.
func (t *connTracker) stateChanged(c net.Conn, active bool) {
	t.mu.Lock()
	if entry, ok := t.conns[c]; ok {
		entry.active = active
	}
	closeIdle := !active && t.draining
	t.mu.Unlock()
	if closeIdle {
		_ = c.Close()
	}
}

// drain marks the tracker as draining and closes every idle connection.
// In-flight connections are left alone: their goroutines finish the
// response, the hook fires with active=false, and the draining flag
// closes them there. Double closes are harmless — a connection that
// exits concurrently removes itself under the same lock.
func (t *connTracker) drain() {
	t.mu.Lock()
	t.draining = true
	for c, entry := range t.conns {
		if !entry.active {
			_ = c.Close()
		}
	}
	t.mu.Unlock()
}
