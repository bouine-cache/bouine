// Package fasthttptest provides helpers to start fasthttp.Server instances
// on ephemeral ports for use in tests, replacing httptest.NewServer.
package fasthttptest

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/valyala/fasthttp"
)

// Server is a minimal fasthttp test server with the same lifecycle
// semantics as httptest.Server: call Close when done.
type Server struct {
	ln     net.Listener
	server *fasthttp.Server
	Addr   string
}

// NewServer starts a fasthttp.Server on 127.0.0.1:0 with the given handler
// and returns it. The caller must call Close when finished.
func NewServer(t testing.TB, handler fasthttp.RequestHandler) *Server {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &fasthttp.Server{
		Handler:     handler,
		IdleTimeout: 50 * time.Millisecond,
	}
	go func() { _ = srv.Serve(ln) }()
	return &Server{Addr: ln.Addr().String(), server: srv, ln: ln}
}

// NewTLSServer starts a fasthttp.Server with TLS on 127.0.0.1:0.
// The caller must call Close when finished.
func NewTLSServer(t testing.TB, handler fasthttp.RequestHandler, tlsCfg *tls.Config) *Server {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	tlsLn := tls.NewListener(ln, tlsCfg)
	srv := &fasthttp.Server{Handler: handler, TLSConfig: tlsCfg, IdleTimeout: 50 * time.Millisecond}
	go func() { _ = srv.Serve(tlsLn) }()
	return &Server{Addr: ln.Addr().String(), server: srv, ln: ln}
}

// Close shuts down the server gracefully and closes the listener.
func (s *Server) Close() {
	_ = s.server.Shutdown()
	_ = s.ln.Close()
}
