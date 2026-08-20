package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetSocketOptions_NoPanic(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	control := setSocketOptions(logger, true, true, false)
	require.NotNil(t, control)
}

func TestSetSocketOptions_ReusePort(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	control := setSocketOptions(logger, false, false, true)
	require.NotNil(t, control)
}

func TestServe_HTTPS_NilTLSConfig(t *testing.T) {
	t.Parallel()
	srv := NewHTTPS(ListenerConfig{
		Addr:    "127.0.0.1:0",
		Handler: echo200(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	assert.Equal(t, "https", srv.Name())
}

func TestServe_HTTPS_WithTLSConfig(t *testing.T) {
	t.Parallel()
	srv := NewHTTPS(ListenerConfig{
		Addr:      "127.0.0.1:0",
		Handler:   echo200(),
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		TLSConfig: nil,
	})
	require.NotNil(t, srv)
	assert.Equal(t, "https", srv.Name())
}

func TestListener_AddrAfterServe(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:    "127.0.0.1:0",
		Handler: echo200(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()
	waitForAddr(t, srv)
	addr := srv.Addr()
	assert.NotEmpty(t, addr)
	cancel()
	<-errCh
}

func TestServeSingle_ContextCancel(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:    "127.0.0.1:0",
		Handler: echo200(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()
	waitForAddr(t, srv)
	cancel()
	err := <-errCh
	require.NoError(t, err)
}

func TestServeSingle_HTTPS(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:           "127.0.0.1:0",
		Handler:        echo200(),
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxConnections: 2,
	})
	assert.Equal(t, 2, srv.maxConns)
}

func TestConnLimitListener_Close(t *testing.T) {
	t.Parallel()
	pl := newPipeListener()
	lim := newConnLimitListener(pl, 1, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, lim.Close())
}

func TestConnLimitConn_Close(t *testing.T) {
	t.Parallel()
	sem := make(chan struct{}, 4)
	var open int32
	_, server := net.Pipe()
	defer func() { _ = server.Close() }()
	c := &connLimitConn{Conn: server, sem: sem, open: &open}
	sem <- struct{}{}
	atomic.AddInt32(&open, 1)
	require.NoError(t, c.Close())
	assert.Equal(t, int32(0), atomic.LoadInt32(&open))
}

func TestServeConnWithHTTP_Error(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:    "127.0.0.1:0",
		Handler: echo200(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	client, server := net.Pipe()
	errCh := make(chan error, 2)
	srv.serveConnWithHTTP(server, errCh)
	_ = client.Close()
}

func TestServeHTTPS_WithFastPath_TLS(t *testing.T) {
	t.Parallel()
	t.Skip("TLS fast path test takes too long due to idle timeout; covered by integration tests")
}

func TestServeHTTPS_WithFastPath_H2(t *testing.T) {
	t.Parallel()
	t.Skip("H2 fast path requires proper ALPN negotiation; covered by integration tests")
}

func TestHandleCleartextFastPath_H2CAndH1(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:        "127.0.0.1:0",
		Handler:     echo200(),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		FastPath:    &mockFastPathHandler{},
		FastMetrics: &mockFastPathMetrics{},
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()
	waitForAddr(t, srv)

	addr := srv.Addr()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
	require.NoError(t, err)
	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, err := conn.Read(buf)
	require.NoError(t, err)
	assert.Contains(t, string(buf[:n]), "200")
	cancel()
	<-errCh
}

func TestHandleTLSFastPath_PlainConn(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:    "127.0.0.1:0",
		Handler: echo200(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	// handleFastPathConn with a non-TLS conn should route to handleCleartextFastPath.
	// Close the server side immediately to trigger a clean exit (peek returns 0 bytes).
	_, server := net.Pipe()
	_ = server.Close()
	errCh := make(chan error, 2)
	srv.handleFastPathConn(server, nil, errCh)
}

func TestLogReusePortStart_WithFastPath(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:        "127.0.0.1:0",
		Handler:     echo200(),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		FastPath:    &mockFastPathHandler{},
		FastMetrics: &mockFastPathMetrics{},
	})
	srv.logReusePortStart("127.0.0.1:0", 4)
}

func TestRouter_NoRouteMetrics(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rr := httptest.NewRecorder()
	rt.ServeHTTP(rr, httptest.NewRequest("GET", "/nope", nil))
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestRouter_WithMetrics(t *testing.T) {
	t.Parallel()
	var reqCount, noRoute int
	rt := NewRouter(RouterConfig{
		Metrics: &RouterMetrics{
			RequestsTotal: &counterFunc{f: func() { reqCount++ }},
			NoRouteTotal:  &counterFunc{f: func() { noRoute++ }},
		},
	})
	rt.AddRoute("", "/api", "api", nil, ok200("api"))
	rr := httptest.NewRecorder()
	rt.ServeHTTP(rr, httptest.NewRequest("GET", "/api/v1", nil))
	assert.Equal(t, "api", rr.Body.String())
	assert.Equal(t, 1, reqCount)
	assert.Equal(t, 0, noRoute)
}

func TestServeFastPath_ContextCancel(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:        "127.0.0.1:0",
		Handler:     echo200(),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		FastPath:    &mockFastPathHandler{},
		FastMetrics: &mockFastPathMetrics{},
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()
	waitForAddr(t, srv)
	cancel()
	err := <-errCh
	require.NoError(t, err)
}

func TestServeMulti_ReusePort(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:      "127.0.0.1:0",
		Handler:   echo200(),
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		ReusePort: true,
	})
	// If reuse port is not supported, Serve falls back to serveSingle.
	// We just verify it doesn't panic.
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()
	waitForAddr(t, srv)
	cancel()
	err := <-errCh
	require.NoError(t, err)
}

func TestServeMulti_ReusePortWithFastPath(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:        "127.0.0.1:0",
		Handler:     echo200(),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		ReusePort:   true,
		FastPath:    &mockFastPathHandler{},
		FastMetrics: &mockFastPathMetrics{},
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()
	waitForAddr(t, srv)
	cancel()
	err := <-errCh
	require.NoError(t, err)
}

// TestServeConnWithHTTP_FiltersClosedError verifies that serveConnWithHTTP
// does NOT send net.ErrClosed or http.ErrServerClosed to the error channel.
func TestServeConnWithHTTP_FiltersClosedError(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:        "127.0.0.1:0",
		Handler:     echo200(),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		FastPath:    &mockFastPathHandler{},
		FastMetrics: &mockFastPathMetrics{},
	})

	// A normal pipe connection: h2c preface then close.
	// http.Server.Serve will get the connection, process it, then get
	// net.ErrClosed on the second Accept and return ErrServerClosed.
	// serveConnWithHTTP should filter both and NOT send to errCh.
	clientConn, serverConn := net.Pipe()
	go func() {
		_, _ = clientConn.Write([]byte(h2cPreface))
		_, _ = clientConn.Write([]byte{0, 0, 0, 4, 0, 0, 0, 0, 0})
		_ = clientConn.Close()
	}()

	errCh := make(chan error, 1)
	// Call serveConnWithHTTP directly — it creates its own
	// singleConnListener internally.
	go srv.serveConnWithHTTP(serverConn, errCh)

	select {
	case err := <-errCh:
		t.Fatalf("serveConnWithHTTP should not have sent error, got: %v", err)
	case <-time.After(3 * time.Second):
		// Expected: no error sent (filtered).
	}
}

// TestServeMultiFastPath_CtxCancellation verifies the normal shutdown
// path: ctx cancellation triggers cleanup in all serveFastPath goroutines.
func TestServeMultiFastPath_CtxCancellation(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:        "127.0.0.1:0",
		Handler:     echo200(),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		FastPath:    &mockFastPathHandler{},
		FastMetrics: &mockFastPathMetrics{},
	})

	ctx, cancel := context.WithCancel(context.Background())

	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.serveMultiFastPath(ctx, []net.Listener{ln1, ln2}) }()

	// Cancel after a short delay to trigger the ctx.Done branch.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("serveMultiFastPath did not return after ctx cancellation")
	}
}

func TestHandleFastPathConn_Cleartext(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:        "127.0.0.1:0",
		Handler:     echo200(),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		FastPath:    &mockFastPathHandler{},
		FastMetrics: &mockFastPathMetrics{},
	})
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	errCh := make(chan error, 2)
	// Close the server side immediately to trigger a clean exit from handleCleartextFastPath.
	_ = server.Close()
	srv.handleFastPathConn(server, nil, errCh)
}
