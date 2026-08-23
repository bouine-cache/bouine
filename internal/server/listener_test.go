package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/poll"
	"github.com/bouine-cache/bouine/internal/testutil/tlsutil"

	"github.com/valyala/fasthttp"
)

func waitForAddr(t *testing.T, l *Listener) {
	t.Helper()
	poll.Eventually(t, 3*time.Second, 10*time.Millisecond, func() bool {
		addr := l.Addr()
		if addr == "" {
			return false
		}
		conn, err := net.DialTimeout("tcp", addr, 20*time.Millisecond)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	})
}

func echo200() fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("X-Proto", "HTTP/1.1")
		ctx.SetBodyString("hello")
	}
}

func TestHTTP_ListenAndServe(t *testing.T) {
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

	client := &fasthttp.Client{}
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI("http://" + srv.Addr())
	if err := client.Do(req, resp); err != nil {
		cancel()
		t.Fatalf("GET: %v", err)
	}

	body := string(resp.Body())
	if resp.StatusCode() != 200 || body != "hello" {
		t.Fatalf("status=%d body=%q", resp.StatusCode(), body)
	}

	cancel()
	serveErr := <-errCh
	require.NoError(t, serveErr, "serve")
}

func TestHTTPS_ListenAndServe(t *testing.T) {
	t.Parallel()
	tlsCfg := tlsutil.ServerConfig(t)

	srv := NewHTTPS(ListenerConfig{
		Addr:      "127.0.0.1:0",
		Handler:   echo200(),
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		TLSConfig: tlsCfg,
	})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()

	waitForAddr(t, srv)

	clientTLS := &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // test
	}
	client := &fasthttp.Client{
		TLSConfig: clientTLS,
	}

	url := fmt.Sprintf("https://%s/", srv.Addr())
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI(url)
	if err := client.Do(req, resp); err != nil {
		cancel()
		t.Fatalf("GET: %v", err)
	}

	body := string(resp.Body())
	if resp.StatusCode() != 200 || body != "hello" {
		t.Fatalf("status=%d body=%q", resp.StatusCode(), body)
	}

	proto := string(resp.Header.Peek("X-Proto"))
	require.Equal(t, "HTTP/1.1", proto)

	cancel()
	serveErr := <-errCh
	require.NoError(t, serveErr, "serve")
}
