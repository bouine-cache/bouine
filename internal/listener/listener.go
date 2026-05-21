// Package listener is the L1 layer. It owns the data-plane network
// sockets: HTTP/1.1, HTTP/2 (via net/http TLS + ALPN), HTTP/3 (via
// quic-go), and optional PROXY-protocol parsing.
//
// Each protocol gets its own *http.Server (or http3.Server) instance
// (ADR-0004). All listeners share the same http.Handler — the L2
// pipeline router.
package listener

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// Config controls a single listener.
type Config struct {
	Addr      string
	Handler   http.Handler
	Logger    *slog.Logger
	TLSConfig *tls.Config
}

// Server wraps a net/http Server with lifecycle methods matching the
// supervised-group contract. Each protocol has its own instance.
//
// Stable.
type Server struct {
	inner    *http.Server
	name     string
	logger   *slog.Logger
	resolved atomic.Value // stores string
}

// NewHTTP creates a plaintext HTTP/1.1 listener. It also supports h2c
// (cleartext HTTP/2 upgrade) for in-mesh traffic.
//
// Stable.
func NewHTTP(cfg Config) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	h2s := &http2.Server{}
	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           h2c.NewHandler(cfg.Handler, h2s),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
	return &Server{inner: srv, name: "http", logger: cfg.Logger}
}

// NewHTTPS creates an HTTP/1.1 + HTTP/2 TLS listener. ALPN negotiation
// ("h2", "http/1.1") is driven by the provided tls.Config.
//
// Stable.
func NewHTTPS(cfg Config) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.TLSConfig == nil {
		cfg.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	cfg.TLSConfig.NextProtos = append(cfg.TLSConfig.NextProtos, "h2", "http/1.1")

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           cfg.Handler,
		TLSConfig:         cfg.TLSConfig,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
	if err := http2.ConfigureServer(srv, &http2.Server{}); err != nil {
		cfg.Logger.Error("h2 configure failed", "error", err)
	}
	return &Server{inner: srv, name: "https", logger: cfg.Logger}
}

// Serve starts the listener. For plaintext it calls ListenAndServe;
// for TLS it calls ListenAndServeTLS with empty cert/key paths because
// the tls.Config already carries the certificates.
//
// Stable.
func (s *Server) Serve(ctx context.Context) error {
	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", s.inner.Addr)
	if err != nil {
		return err
	}
	s.resolved.Store(ln.Addr().String())

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

// Name returns the protocol label ("http", "https").
func (s *Server) Name() string { return s.name }

// Addr returns the resolved listen address after Serve has been
// called. Before Serve, returns the configured address.
func (s *Server) Addr() string {
	if v := s.resolved.Load(); v != nil {
		return v.(string)
	}
	return s.inner.Addr
}
