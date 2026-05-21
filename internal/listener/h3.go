package listener

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"sync/atomic"

	"github.com/quic-go/quic-go/http3"
)

// H3Server wraps a quic-go http3.Server with the same lifecycle
// contract as Server (context-driven, named, supervised).
//
// Stable.
type H3Server struct {
	inner    *http3.Server
	addr     string
	name     string
	logger   *slog.Logger
	resolved atomic.Value // stores string
}

// NewHTTP3 creates an HTTP/3 listener. The provided tls.Config must
// have at least one certificate; ALPN is forced to "h3".
//
// Stable.
func NewHTTP3(cfg Config) *H3Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.TLSConfig == nil {
		cfg.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS13}
	}
	// HTTP/3 mandates TLS 1.3.
	cfg.TLSConfig.MinVersion = tls.VersionTLS13
	cfg.TLSConfig.NextProtos = []string{"h3"}

	srv := &http3.Server{
		Addr:      cfg.Addr,
		TLSConfig: cfg.TLSConfig,
		Handler:   cfg.Handler,
	}
	return &H3Server{
		inner:  srv,
		addr:   cfg.Addr,
		name:   "http3",
		logger: cfg.Logger,
	}
}

// Serve opens a UDP socket and serves HTTP/3 until ctx is cancelled.
//
// Stable.
func (s *H3Server) Serve(ctx context.Context) error {
	udpAddr, err := net.ResolveUDPAddr("udp", s.addr)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return err
	}
	s.resolved.Store(conn.LocalAddr().String())

	s.logger.Info("listener started",
		"name", s.name,
		"addr", s.resolved.Load().(string))

	errCh := make(chan error, 1)
	go func() { errCh <- s.inner.Serve(conn) }()

	select {
	case <-ctx.Done():
		return s.inner.Close()
	case err := <-errCh:
		if err != nil && err.Error() == "quic: server closed" {
			return nil
		}
		return err
	}
}

// Name returns "http3".
func (s *H3Server) Name() string { return s.name }

// Addr returns the resolved listen address.
func (s *H3Server) Addr() string {
	if v := s.resolved.Load(); v != nil {
		return v.(string)
	}
	return s.addr
}
