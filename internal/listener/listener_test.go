package listener

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/thylong/bouine/internal/testutil/tlsutil"
)

func echo200() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Proto", r.Proto)
		_, _ = io.WriteString(w, "hello")
	})
}

func TestHTTP_ListenAndServe(t *testing.T) {
	srv := NewHTTP(Config{
		Addr:    "127.0.0.1:0",
		Handler: echo200(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()

	time.Sleep(200 * time.Millisecond)

	resp, err := http.Get("http://" + srv.Addr())
	if err != nil {
		// Addr may still be :0 before serve resolves; get from listener.
		// Fall back to a short retry.
		time.Sleep(300 * time.Millisecond)
		resp, err = http.Get("http://" + srv.Addr())
	}
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
	if err := <-errCh; err != nil {
		t.Fatalf("serve: %v", err)
	}
}

func TestHTTPS_ListenAndServe_H2(t *testing.T) {
	tlsCfg := tlsutil.ServerConfig(t)

	srv := NewHTTPS(Config{
		Addr:      "127.0.0.1:0",
		Handler:   echo200(),
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		TLSConfig: tlsCfg,
	})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()

	time.Sleep(200 * time.Millisecond)

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
		time.Sleep(300 * time.Millisecond)
		resp, err = client.Get(url)
	}
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
	if proto != "HTTP/2.0" {
		t.Fatalf("expected HTTP/2.0, got %q", proto)
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("serve: %v", err)
	}
}
