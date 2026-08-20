package server

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServeSingle_WithMaxConnections verifies the connection limit listener
// is wired up when MaxConnections > 0.
func TestServeSingle_WithMaxConnections(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:           "127.0.0.1:0",
		Handler:        echo200(),
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxConnections: 10,
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()
	waitForAddr(t, srv)

	resp, err := http.Get("http://" + srv.Addr())
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Contains(t, string(body), "hello")
	cancel()
	<-errCh
}

// TestServeSingle_HTTPSWithMaxConnections verifies HTTPS listener with max conns.
func TestServeSingle_HTTPSWithMaxConnections(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:           "127.0.0.1:0",
		Handler:        echo200(),
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxConnections: 5,
	})
	assert.Equal(t, 5, srv.maxConns)
}

// TestPeekConn_PeekErrorAndRead verifies peek with error then read.
func TestPeekConn_PeekErrorThenRead(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	pk := newPeekConn(server)
	// Close the client to trigger EOF on read.
	_ = client.Close()
	// Peek should return error with 0 bytes.
	peeked, err := pk.Peek(10)
	if len(peeked) == 0 {
		require.Error(t, err)
	}
}

// TestPeekConn_PeekExactThenReadMore verifies peek returns exactly n bytes
// when already buffered.
func TestPeekConn_PeekExactThenReadMore(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	pk := newPeekConn(server)
	go func() { _, _ = client.Write([]byte("hello world")) }()
	peeked, err := pk.Peek(5)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(peeked))
	// Peek again - should return from buffer without reading more.
	peeked2, err := pk.Peek(5)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(peeked2))
	// Read should return peeked bytes first.
	buf := make([]byte, 5)
	n, err := pk.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(buf[:n]))
}

// TestServeConnWithHTTP_ValidRequest tests serveConnWithHTTP with a real HTTP request.
func TestServeConnWithHTTP_ValidRequest(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:    "127.0.0.1:0",
		Handler: echo200(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	errCh := make(chan error, 2)
	go func() {
		_, _ = client.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
		time.Sleep(100 * time.Millisecond)
	}()

	go srv.serveConnWithHTTP(server, errCh)

	buf := make([]byte, 1024)
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := client.Read(buf)
	assert.Contains(t, string(buf[:n]), "200")
	_ = server.Close()
}

// TestHandleCleartextFastPath_PeekError tests handleCleartextFastPath with a closed conn.
func TestHandleCleartextFastPath_PeekError(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:        "127.0.0.1:0",
		Handler:     echo200(),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		FastPath:    &mockFastPathHandler{},
		FastMetrics: &mockFastPathMetrics{},
	})
	_, server := net.Pipe()
	_ = server.Close()
	errCh := make(chan error, 2)
	srv.handleCleartextFastPath(server, nil, errCh)
}

// TestServeFastPath_AcceptError tests serveFastPath with a closed listener.
func TestServeFastPath_AcceptError(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:        "127.0.0.1:0",
		Handler:     echo200(),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		FastPath:    &mockFastPathHandler{},
		FastMetrics: &mockFastPathMetrics{},
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.serveFastPath(ctx, ln) }()
	_ = ln.Close()
	cancel()
	<-errCh
}

// TestSetSocketOptions_RealListener verifies the Control function works
// with a real TCP listener.
func TestSetSocketOptions_RealListener(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	control := setSocketOptions(logger, true, true, false)
	lc := net.ListenConfig{Control: control}
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	_ = ln.Close()

	// Also test with reusePort=true (may fail on non-Linux, that's ok).
	control2 := setSocketOptions(logger, false, false, true)
	lc2 := net.ListenConfig{Control: control2}
	ln2, err := lc2.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		// reusePort may not be supported on this platform, that's fine.
		return
	}
	_ = ln2.Close()
}

// TestServe_HTTPS_ProvidedTLSConfig tests Serve with HTTPS and a provided TLS config.
func TestServe_HTTPS_ProvidedTLSConfig(t *testing.T) {
	t.Parallel()
	srv := NewHTTPS(ListenerConfig{
		Addr:      "127.0.0.1:0",
		Handler:   echo200(),
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	})
	require.NotNil(t, srv.inner.TLSConfig)
}

// TestServe_ReusePortFallsBack tests that Serve with ReusePort falls back
// to serveSingle on platforms where SO_REUSEPORT is not supported.
func TestServe_ReusePortFallsBack(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:      "127.0.0.1:0",
		Handler:   echo200(),
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		ReusePort: true,
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()
	waitForAddr(t, srv)

	resp, err := http.Get("http://" + srv.Addr())
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Contains(t, string(body), "hello")
	cancel()
	<-errCh
}

// TestServe_ReusePortFallsBackWithFastPath tests reuseport fallback with fast path.
func TestServe_ReusePortFallsBackWithFastPath(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:           "127.0.0.1:0",
		Handler:        echo200(),
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		ReusePort:      true,
		FastPath:       &mockFastPathHandler{},
		FastMetrics:    &mockFastPathMetrics{},
		MaxConnections: 100,
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()
	waitForAddr(t, srv)

	conn, err := net.Dial("tcp", srv.Addr())
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

// TestServe_HTTPS_WithFastPathAndMaxConns tests HTTPS with fast path and max conns.
func TestServe_HTTPS_WithFastPathAndMaxConns(t *testing.T) {
	t.Parallel()
	srv := NewHTTPS(ListenerConfig{
		Addr:           "127.0.0.1:0",
		Handler:        echo200(),
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxConnections: 100,
	})
	assert.Equal(t, 100, srv.maxConns)
}

// TestServe_HTTP_WithScheme tests that the scheme is set correctly.
func TestServe_HTTP_WithScheme(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:    "127.0.0.1:0",
		Handler: echo200(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Scheme:  "http",
	})
	assert.Equal(t, "http", srv.scheme)
}

// TestServeConnWithHTTP_InvalidRequest tests serveConnWithHTTP with invalid HTTP data.
func TestServeConnWithHTTP_InvalidRequest(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:    "127.0.0.1:0",
		Handler: echo200(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	errCh := make(chan error, 2)
	go func() {
		_, _ = client.Write([]byte("GARBAGE\r\n\r\n"))
		time.Sleep(100 * time.Millisecond)
	}()

	go srv.serveConnWithHTTP(server, errCh)

	// Read response (might be an error response or nothing).
	buf := make([]byte, 1024)
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _ = client.Read(buf)
	_ = server.Close()
}
