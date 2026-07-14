package server

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/bouine-cache/bouine/internal/server/h1parser"
)

// h2cPreface is the HTTP/2 cleartext connection preface (RFC 9113 §3.4).
const h2cPreface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

// serveFastPath accepts connections and routes them to the h1parser or
// net/http based on the protocol. HTTP/2 connections (ALPN h2 or h2c
// upgrade preface) go to net/http. HTTP/1.1 connections go to the
// custom h1parser for zero-alloc cache-hit serving.
func (s *Listener) serveFastPath(ctx context.Context, ln net.Listener) error {
	scheme := s.scheme
	if scheme == "" {
		scheme = "http"
	}

	parser := h1parser.New(
		s.fastPath,
		s.inner.Handler,
		h1parser.WithScheme(scheme),
		h1parser.WithNowFunc(time.Now),
		h1parser.WithIdleReadTimeout(10*time.Second),
		h1parser.WithWriteTimeout(safetyNetWriteTimeout),
		h1parser.WithMetricsHook(s.fastMetrics.RecordHit),
		h1parser.WithFallbackServer(s.inner),
	)

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

// handleFastPathConn routes a single accepted connection to the h1parser
// or net/http. For TLS connections, ALPN determines the protocol. For
// cleartext, the first bytes are peeked to detect the h2c preface.
func (s *Listener) handleFastPathConn(conn net.Conn, parser *h1parser.Parser, errCh chan<- error) {
	defer func() { _ = conn.Close() }()

	if tlsConn, ok := conn.(*tls.Conn); ok {
		s.handleTLSFastPath(tlsConn, parser, errCh)
		return
	}

	s.handleCleartextFastPath(conn, parser, errCh)
}

// handleTLSFastPath routes a TLS connection to the h1parser or net/http
// based on the ALPN negotiation result.
func (s *Listener) handleTLSFastPath(conn *tls.Conn, parser *h1parser.Parser, errCh chan<- error) {
	if err := conn.HandshakeContext(context.Background()); err != nil {
		return
	}
	if state := conn.ConnectionState(); state.NegotiatedProtocol == "h2" {
		s.serveConnWithHTTP(conn, errCh)
		return
	}
	if err := parser.Serve(conn); err != nil { //nolint:contextcheck // parser manages its own deadlines
		reportFastPathError(err, errCh)
	}
}

// handleCleartextFastPath routes a cleartext connection to the h1parser
// or net/http by peeking for the h2c preface.
func (s *Listener) handleCleartextFastPath(conn net.Conn, parser *h1parser.Parser, errCh chan<- error) {
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	peeker := newPeekConn(conn)
	preface, err := peeker.Peek(len(h2cPreface))
	if err != nil && len(preface) == 0 {
		return
	}
	if len(preface) >= len(h2cPreface) && string(preface[:len(h2cPreface)]) == h2cPreface {
		s.serveConnWithHTTP(peeker, errCh)
		return
	}
	if err := parser.Serve(peeker); err != nil { //nolint:contextcheck // parser manages its own deadlines
		reportFastPathError(err, errCh)
	}
}

// reportFastPathError sends a non-fatal parser error to errCh using a
// non-blocking send. Expected errors (EOF, closed, fall-through) are
// silently discarded.
func reportFastPathError(err error, errCh chan<- error) {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, h1parser.ErrFallThrough) {
		return
	}
	select {
	case errCh <- err:
	default:
	}
}

// serveConnWithHTTP hands a single connection to net/http via a
// one-shot listener. The closeNotifyConn ensures Serve does not return
// until the handler goroutine finishes, preventing the caller's defer
// from closing the connection mid-response.
func (s *Listener) serveConnWithHTTP(conn net.Conn, errCh chan<- error) {
	notifyConn := newCloseNotifyConn(conn)
	cl := &singleConnListener{conn: notifyConn, ready: notifyConn.done}
	if err := s.inner.Serve(cl); err != nil &&
		!errors.Is(err, http.ErrServerClosed) &&
		!errors.Is(err, net.ErrClosed) {
		errCh <- err
	}
}

// serveMultiFastPath runs the fast-path accept loop across multiple
// SO_REUSEPORT listeners. Called from serveMulti when the fast path is enabled.
func (s *Listener) serveMultiFastPath(ctx context.Context, listeners []net.Listener) error {
	errCh := make(chan error, len(listeners))
	var wg sync.WaitGroup
	for _, ln := range listeners {
		wg.Add(1)
		go func(l net.Listener) {
			defer wg.Done()
			if err := s.serveFastPath(ctx, l); err != nil && !errors.Is(err, net.ErrClosed) {
				errCh <- err
			}
		}(ln)
	}

	select {
	case <-ctx.Done():
		for _, l := range listeners {
			_ = l.Close()
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			if err != nil {
				return err
			}
		}
		return nil
	case err := <-errCh:
		for _, l := range listeners {
			_ = l.Close()
		}
		wg.Wait()
		return err
	}
}

// closeNotifyConn wraps a net.Conn and closes a channel when Close is
// called, so the singleConnListener can block until the handler goroutine
// is done with the connection. Duplicated from h1parser to avoid a
// circular dependency; keep both copies in sync.
type closeNotifyConn struct {
	net.Conn
	done chan struct{}
	once sync.Once
}

func newCloseNotifyConn(c net.Conn) *closeNotifyConn {
	return &closeNotifyConn{Conn: c, done: make(chan struct{})}
}

func (c *closeNotifyConn) Close() error {
	c.once.Do(func() { close(c.done) })
	return c.Conn.Close()
}

// singleConnListener returns one pre-accepted connection, then blocks
// until that connection is closed before returning ErrClosed on the
// next Accept. This ensures http.Server.Serve does not return until the
// handler goroutine has finished, preventing the caller from closing the
// connection mid-response. Duplicated from h1parser to avoid a circular
// dependency; keep both copies in sync.
type singleConnListener struct {
	conn  net.Conn
	ready <-chan struct{}
	done  bool
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	if l.done {
		<-l.ready
		return nil, net.ErrClosed
	}
	l.done = true
	return l.conn, nil
}

func (l *singleConnListener) Close() error   { return nil }
func (l *singleConnListener) Addr() net.Addr { return l.conn.LocalAddr() }

// peekConn wraps a net.Conn and allows peeking bytes before reading.
// Peeked bytes are buffered and returned on subsequent Read calls.
type peekConn struct {
	net.Conn
	peekBuf []byte
}

func newPeekConn(c net.Conn) *peekConn {
	return &peekConn{Conn: c}
}

// Peek reads up to n bytes from the connection without consuming them.
// The peeked bytes are buffered and returned by subsequent Read calls.
func (p *peekConn) Peek(n int) ([]byte, error) {
	if len(p.peekBuf) >= n {
		return p.peekBuf[:n], nil
	}
	needed := n - len(p.peekBuf)
	buf := make([]byte, needed)
	nr, err := p.Conn.Read(buf)
	if nr > 0 {
		p.peekBuf = append(p.peekBuf, buf[:nr]...)
	}
	if err != nil && len(p.peekBuf) == 0 {
		return nil, err
	}
	if len(p.peekBuf) > n {
		return p.peekBuf[:n], err
	}
	return p.peekBuf, err
}

// Read returns buffered peek bytes first, then reads from the connection.
func (p *peekConn) Read(b []byte) (int, error) {
	if len(p.peekBuf) > 0 {
		n := copy(b, p.peekBuf)
		p.peekBuf = p.peekBuf[n:]
		return n, nil
	}
	return p.Conn.Read(b)
}
