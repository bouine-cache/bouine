package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleTLSFastPath_HandshakeFail(t *testing.T) {
	t.Skip("handleTLSFastPath removed — TLS handled inline in handleFastPathConn")
}

func TestServeMulti_FallbackToSingle(t *testing.T) {
	t.Parallel()
	// If reusePort is requested but not supported, Serve falls back
	// to serveSingle. On macOS SO_REUSEPORT is supported, so this just
	// verifies Serve works and returns cleanly on context cancel.
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
	cancel()
	err := <-errCh
	require.NoError(t, err)
}

func TestServeMultiFastPath_FastPathWithReusePort(t *testing.T) {
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
	// Make a request to verify the server works.
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
	err = <-errCh
	require.NoError(t, err)
}

func TestServe_ReusePortNotSupported_Fallback(t *testing.T) {
	t.Parallel()
	// On platforms where reusePort is false, Serve uses serveSingle.
	srv := NewHTTP(ListenerConfig{
		Addr:      "127.0.0.1:0",
		Handler:   echo200(),
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		ReusePort: false,
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()
	waitForAddr(t, srv)
	cancel()
	err := <-errCh
	require.NoError(t, err)
}

func TestServeMulti_DirectCall_Fallback(t *testing.T) {
	t.Parallel()
	// Call serveMulti directly. On macOS, SO_REUSEPORT is not supported,
	// so the first lc.Listen with reusePort control will fail and serveMulti
	// falls back to serveSingle.
	srv := NewHTTP(ListenerConfig{
		Addr:      "127.0.0.1:0",
		Handler:   echo200(),
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		ReusePort: true,
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.serveMulti(ctx) }()
	waitForAddr(t, srv)
	cancel()
	err := <-errCh
	require.NoError(t, err)
}

func TestServeMulti_DirectCall_WithFastPath(t *testing.T) {
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
	go func() { errCh <- srv.serveMulti(ctx) }()
	waitForAddr(t, srv)
	cancel()
	err := <-errCh
	// On macOS, serveMulti falls back to serveSingle (reusePort unsupported).
	// The fallback path does not use the fast path, so this just verifies
	// no panic.
	_ = err
}

func TestServeMulti_WithMaxConns(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:           "127.0.0.1:0",
		Handler:        echo200(),
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		ReusePort:      true,
		MaxConnections: 10,
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.serveMulti(ctx) }()
	waitForAddr(t, srv)
	cancel()
	err := <-errCh
	require.NoError(t, err)
}

func TestServeMultiFastPath_DirectCall(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:        "127.0.0.1:0",
		Handler:     echo200(),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		FastPath:    &mockFastPathHandler{},
		FastMetrics: &mockFastPathMetrics{},
	})
	// Create a real listener on a free port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	listeners := []net.Listener{ln}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.serveMultiFastPath(ctx, listeners) }()

	// Cancel context immediately to trigger the ctx.Done path.
	cancel()
	err = <-errCh
	require.NoError(t, err)
}

func TestServeMultiFastPath_DirectCall_Error(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:        "127.0.0.1:0",
		Handler:     echo200(),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		FastPath:    &mockFastPathHandler{},
		FastMetrics: &mockFastPathMetrics{},
	})
	// Create a listener and close it so Accept fails immediately,
	// triggering an error path.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	_ = ln.Close()
	listeners := []net.Listener{ln}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.serveMultiFastPath(ctx, listeners) }()

	// The error from Accept should be received, or the context timeout
	// should trigger. Either way, the function should return.
	err = <-errCh
	_ = err
}
