// Package server is the data-plane front door. It owns HTTP/1.1 +
// HTTP/2 listeners (TLS and cleartext) and the route-matching router
// that dispatches requests to cache handlers.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/platform"
	"github.com/bouine-cache/bouine/pkg/api"
)

// safetyNetWriteTimeout is a generous write deadline that acts as a
// last-resort guard against slowloris-style clients that read 1 byte/s.
// It is NOT the primary origin-fetch timeout — that is bounded per-fetch
// by fetch_timeout and per-transport by response_header_timeout. This
// safety net only fires for truly stuck connections that would otherwise
// hold a goroutine and its buffers indefinitely.
//
// fetch_timeout (config) must be strictly less than this value.
// Config validation (config.maxFetchTimeout) enforces this at load
// time. The two constants are duplicated across packages because the
// layering rules (L1 and L2 cannot depend on each other) prevent a
// shared import. If you change one, change the other.
const safetyNetWriteTimeout = 5 * time.Minute

// setSocketOptions applies Linux-specific TCP optimizations to the
// listener socket: TCP_FASTOPEN (data in SYN, -1 RTT) and
// TCP_DEFER_ACCEPT (defer accept until data arrives). Both are no-ops
// on non-Linux platforms via the platform package.
func setSocketOptions(log observability.Logger, fastOpen, deferAccept, reusePort bool) func(string, string, syscall.RawConn) error {
	return func(network, address string, c syscall.RawConn) error {
		var fatalErr error
		if err := c.Control(func(fd uintptr) {
			if fastOpen {
				if err := platform.SetTCPFastOpen(int(fd), 16); err != nil {
					log.Warn("listener socket option failed", "option", "tcp_fast_open", "error", err)
				} else {
					log.Info("listener socket option enabled", "option", "tcp_fast_open")
				}
			}
			if deferAccept {
				if err := platform.SetTCPDeferAccept(int(fd), 1); err != nil {
					log.Warn("listener socket option failed", "option", "tcp_defer_accept", "error", err)
				} else {
					log.Info("listener socket option enabled", "option", "tcp_defer_accept")
				}
			}
			if reusePort {
				if err := platform.SetReusePort(int(fd)); err != nil {
					log.Warn("listener socket option failed", "option", "so_reuseport", "error", err)
					fatalErr = err
				} else {
					log.Info("listener socket option enabled", "option", "so_reuseport")
				}
			}
		}); err != nil {
			fatalErr = err
		}
		return fatalErr
	}
}

// ListenerConfig controls a single listener.
//
// MaxConnections, when > 0, caps the number of simultaneously open
// data-plane connections. Excess connections are accepted then
// immediately closed with a 503 response, preventing FD exhaustion
// under slowloris or connection-flood attacks.
type ListenerConfig struct {
	Addr           string
	Handler        http.Handler
	Logger         observability.Logger
	TLSConfig      *tls.Config
	MaxConnections int
	TCPFastOpen    bool
	TCPDeferAccept bool
	ReusePort      bool
	FastPath       api.FastPathHandler
	FastMetrics    api.FastPathMetrics
	Scheme         string
}

// Listener wraps a net/http Server with lifecycle methods matching the
// supervised-group contract. Each protocol has its own instance.
//
// Stable.
type Listener struct {
	inner          *http.Server
	name           string
	logger         observability.Logger
	resolved       atomic.Value // stores string
	maxConns       int
	tcpFastOpen    bool
	tcpDeferAccept bool
	reusePort      bool
	fastPath       api.FastPathHandler
	fastMetrics    api.FastPathMetrics
	scheme         string
}

// NewHTTP creates a plaintext HTTP/1.1 + HTTP/2 cleartext (h2c) listener.
//
// Stable.
func NewHTTP(cfg ListenerConfig) *Listener {
	cfg.Logger = observability.ResolveLogger(cfg.Logger)

	var protos http.Protocols
	protos.SetHTTP1(true)
	protos.SetUnencryptedHTTP2(true)

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           cfg.Handler,
		Protocols:         &protos,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      safetyNetWriteTimeout, // safety net — primary timeouts are fetch_timeout and response_header_timeout
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
	return &Listener{inner: srv, name: "http", logger: cfg.Logger, maxConns: cfg.MaxConnections,
		tcpFastOpen: cfg.TCPFastOpen, tcpDeferAccept: cfg.TCPDeferAccept, reusePort: cfg.ReusePort,
		fastPath: cfg.FastPath, fastMetrics: cfg.FastMetrics, scheme: cfg.Scheme}
}

// NewHTTPS creates an HTTP/1.1 + HTTP/2 TLS listener.
//
// Stable.
func NewHTTPS(cfg ListenerConfig) *Listener {
	cfg.Logger = observability.ResolveLogger(cfg.Logger)
	if cfg.TLSConfig == nil {
		cfg.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	cfg.TLSConfig.NextProtos = append(cfg.TLSConfig.NextProtos, "h2", "http/1.1")

	var protos http.Protocols
	protos.SetHTTP1(true)
	protos.SetHTTP2(true)

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           cfg.Handler,
		TLSConfig:         cfg.TLSConfig,
		Protocols:         &protos,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      safetyNetWriteTimeout, // safety net — primary timeouts are fetch_timeout and response_header_timeout
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
	return &Listener{inner: srv, name: "https", logger: cfg.Logger, maxConns: cfg.MaxConnections,
		tcpFastOpen: cfg.TCPFastOpen, tcpDeferAccept: cfg.TCPDeferAccept, reusePort: cfg.ReusePort,
		fastPath: cfg.FastPath, fastMetrics: cfg.FastMetrics, scheme: cfg.Scheme}
}

// Serve starts the listener and blocks until ctx is cancelled. When
// SO_REUSEPORT is enabled and supported, N parallel accept loops (one
// per GOMAXPROCS) share the same port, distributing connections via
// kernel-level hashing. Otherwise a single accept loop is used.
//
// Stable.
func (s *Listener) Serve(ctx context.Context) error {
	if s.reusePort && platform.ReusePortSupported {
		return s.serveMulti(ctx)
	}
	return s.serveSingle(ctx)
}

// fastPathEnabled reports whether the fast-path H1 parser is active.
func (s *Listener) fastPathEnabled() bool {
	return s.fastPath != nil
}

// logReusePortStart logs the listener startup with reuse-port info.
func (s *Listener) logReusePortStart(addr string, n int) {
	if s.fastPathEnabled() {
		s.logger.Info("listener started with H1 fast path + SO_REUSEPORT",
			"name", s.name, "addr", addr, "listeners", n)
	} else {
		s.logger.Info("listener started with SO_REUSEPORT",
			"name", s.name, "addr", addr, "listeners", n)
	}
}

// serveSingle runs a single accept loop — the traditional listener model.
func (s *Listener) serveSingle(ctx context.Context) error {
	lc := net.ListenConfig{
		Control: setSocketOptions(s.logger, s.tcpFastOpen, s.tcpDeferAccept, false),
	}
	ln, err := lc.Listen(ctx, "tcp", s.inner.Addr)
	if err != nil {
		return err
	}
	s.resolved.Store(ln.Addr().String())

	if s.maxConns > 0 {
		ln = newConnLimitListener(ln, s.maxConns, s.logger)
	}

	s.logger.Info("listener started",
		"name", s.name,
		"addr", s.resolved.Load().(string))

	if s.inner.TLSConfig != nil {
		ln = tls.NewListener(ln, s.inner.TLSConfig)
	}

	if s.fastPathEnabled() {
		s.logger.Info("listener started with H1 fast path",
			"name", s.name,
			"addr", s.resolved.Load().(string))
		return s.serveFastPath(ctx, ln)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- s.inner.Serve(ln) }()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		return s.inner.Shutdown(shutCtx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// serveMulti creates N=runtime.GOMAXPROCS(0) listeners with SO_REUSEPORT
// so the kernel distributes incoming connections across them. All N
// listeners share a single connection-limit semaphore. If the first
// listener creation fails (e.g. SO_REUSEPORT unsupported at runtime),
// it falls back to serveSingle.
func (s *Listener) serveMulti(ctx context.Context) error {
	n := runtime.GOMAXPROCS(0)

	var sem chan struct{}
	if s.maxConns > 0 {
		sem = make(chan struct{}, s.maxConns)
	}

	control := setSocketOptions(s.logger, s.tcpFastOpen, s.tcpDeferAccept, true)
	lc := net.ListenConfig{Control: control}

	listeners := make([]net.Listener, 0, n)
	var firstAddr string

	for range n {
		ln, err := lc.Listen(ctx, "tcp", s.inner.Addr)
		if err != nil {
			for _, l := range listeners {
				_ = l.Close()
			}
			if len(listeners) == 0 {
				s.logger.Warn("reuse_port: listener creation failed, falling back to single listener",
					"error", err)
				return s.serveSingle(ctx)
			}
			s.logger.Warn("reuse_port: partial creation, using fewer listeners",
				"created", len(listeners), "requested", n, "error", err)
			break
		}
		if firstAddr == "" {
			firstAddr = ln.Addr().String()
		}
		if s.maxConns > 0 {
			ln = newConnLimitListenerWithSem(ln, sem, s.logger)
		}
		if s.inner.TLSConfig != nil {
			ln = tls.NewListener(ln, s.inner.TLSConfig)
		}
		listeners = append(listeners, ln)
	}

	s.resolved.Store(firstAddr)
	s.logReusePortStart(firstAddr, len(listeners))

	if s.fastPathEnabled() {
		return s.serveMultiFastPath(ctx, listeners)
	}

	errCh := make(chan error, len(listeners))
	for _, ln := range listeners {
		go func(l net.Listener) {
			if err := s.inner.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		}(ln)
	}

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		return s.inner.Shutdown(shutCtx)
	case err := <-errCh:
		shutCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = s.inner.Shutdown(shutCtx)
		return err
	}
}

// Shutdown gracefully stops the listener, waiting for in-flight
// requests to complete or ctx to expire. Safe to call concurrently
// with Serve; the inner http.Server.Shutdown is idempotent.
func (s *Listener) Shutdown(ctx context.Context) error {
	return s.inner.Shutdown(ctx)
}

// Name returns the protocol label ("http", "https").
func (s *Listener) Name() string { return s.name }

// Addr returns the resolved listen address after Serve has been called.
func (s *Listener) Addr() string {
	if v := s.resolved.Load(); v != nil {
		return v.(string)
	}
	return s.inner.Addr
}

// connLimitListener wraps a net.Listener with a semaphore that caps
// the number of concurrently accepted connections. When the limit is
// reached, new connections are closed immediately to prevent FD
// exhaustion under connection-flood attacks (e.g. slowloris).
type connLimitListener struct {
	net.Listener
	sem  chan struct{}
	log  observability.Logger
	open int32 // atomic, for observability only
}

func newConnLimitListener(inner net.Listener, max int, log observability.Logger) net.Listener {
	return newConnLimitListenerWithSem(inner, make(chan struct{}, max), log)
}

func newConnLimitListenerWithSem(inner net.Listener, sem chan struct{}, log observability.Logger) net.Listener {
	return &connLimitListener{
		Listener: inner,
		sem:      sem,
		log:      log,
	}
}

func (l *connLimitListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	select {
	case l.sem <- struct{}{}:
		atomic.AddInt32(&l.open, 1)
		return &connLimitConn{Conn: conn, sem: l.sem, open: &l.open}, nil
	default:
		// Limit reached — send 503 then close.
		_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		_, _ = conn.Write([]byte("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"))
		_ = conn.Close()
		l.log.Warn("connection rejected: max_connections reached")
		return nil, errors.New("max_connections reached")
	}
}

// connLimitConn releases the semaphore slot when the connection is
// closed, ensuring accurate accounting even if the server doesn't
// explicitly close connections.
type connLimitConn struct {
	net.Conn
	sem  chan struct{}
	open *int32
	once sync.Once
}

func (c *connLimitConn) Close() error {
	c.once.Do(func() {
		<-c.sem
		atomic.AddInt32(c.open, -1)
	})
	return c.Conn.Close()
}
