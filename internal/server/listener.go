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
	"sync"
	"sync/atomic"
	"time"

	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/internal/observability/tracing"
)

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
}

// Listener wraps a net/http Server with lifecycle methods matching the
// supervised-group contract. Each protocol has its own instance.
//
// Stable.
type Listener struct {
	inner    *http.Server
	name     string
	logger   observability.Logger
	resolved atomic.Value // stores string
	maxConns int
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
		Handler:           tracing.HTTPMiddleware("bouine.listener.http", cfg.Handler),
		Protocols:         &protos,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
	return &Listener{inner: srv, name: "http", logger: cfg.Logger, maxConns: cfg.MaxConnections}
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
		Handler:           tracing.HTTPMiddleware("bouine.listener.https", cfg.Handler),
		TLSConfig:         cfg.TLSConfig,
		Protocols:         &protos,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
	return &Listener{inner: srv, name: "https", logger: cfg.Logger, maxConns: cfg.MaxConnections}
}

// Serve starts the listener and blocks until ctx is cancelled.
//
// Stable.
func (s *Listener) Serve(ctx context.Context) error {
	lc := net.ListenConfig{}
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
	return &connLimitListener{
		Listener: inner,
		sem:      make(chan struct{}, max),
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
		// Limit reached — reject immediately.
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
