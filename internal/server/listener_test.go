package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/poll"
	"github.com/bouine-cache/bouine/internal/testutil/tlsutil"
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

func echo200() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Proto", r.Proto)
		_, _ = io.WriteString(w, "hello")
	})
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

	resp, err := http.Get("http://" + srv.Addr())
	if err != nil {
		cancel()
		t.Fatalf("GET: %v", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			t.Errorf("close: %v", cerr)
		}
	}()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(body) != "hello" {
		t.Fatalf("status=%d body=%q", resp.StatusCode, body)
	}

	cancel()
	err = <-errCh
	require.NoError(t, err, "serve")
}

func TestHTTPS_ListenAndServe_H2(t *testing.T) {
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
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   clientTLS,
			ForceAttemptHTTP2: true,
		},
	}

	url := fmt.Sprintf("https://%s/", srv.Addr())
	resp, err := client.Get(url)
	if err != nil {
		cancel()
		t.Fatalf("GET: %v", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			t.Errorf("close: %v", cerr)
		}
	}()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(body) != "hello" {
		t.Fatalf("status=%d body=%q", resp.StatusCode, body)
	}

	proto := resp.Header.Get("X-Proto")
	require.Equal(t, "HTTP/2.0", proto)

	cancel()
	err = <-errCh
	require.NoError(t, err, "serve")
}
