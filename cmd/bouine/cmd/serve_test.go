package cmd

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/poll"
	"github.com/bouine-cache/bouine/internal/testutil/tlsutil"
)

// waitForPort polls until addr accepts TCP connections or the 3s timeout expires.
func waitForPort(t *testing.T, addr string) {
	t.Helper()
	poll.Eventually(t, 3*time.Second, 10*time.Millisecond, func() bool {
		conn, err := net.DialTimeout("tcp", addr, 20*time.Millisecond)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	})
}

func TestProxyEndToEnd(t *testing.T) {
	// Full in-process e2e: origin + pipeline + listener.
	originSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Origin", "yes")
		_, _ = io.WriteString(w, "proxied!")
	}))
	defer originSrv.Close()

	dir := t.TempDir()
	certPath, keyPath := tlsutil.WriteCertFiles(t, dir)
	cfg := fmt.Sprintf(`
listen:
  http:  "127.0.0.1:18090"
  https: "127.0.0.1:18091"
  admin: "127.0.0.1:18092"
tls:
  certs:
    - cert_file: %q
      key_file: %q
upstream_pools:
  - name: echo
    targets: [%q]
routes:
  - match: {}
    pool: echo
`, certPath, keyPath, originSrv.Listener.Addr().String())
	cfgPath := filepath.Join(dir, "bouine.yaml")
	err := os.WriteFile(cfgPath, []byte(cfg), 0o600)
	require.NoError(t, err)

	root := Root()
	root.SetArgs([]string{"serve", "--config", cfgPath, "--log-format", "text"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- root.ExecuteContext(ctx) }()

	waitForPort(t, "127.0.0.1:18090")
	waitForPort(t, "127.0.0.1:18091")
	waitForPort(t, "127.0.0.1:18092")

	// HTTP/1.1 proxy test.
	resp, err := http.Get("http://127.0.0.1:18090/hello")
	if err != nil {
		cancel()
		t.Fatalf("HTTP GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	cerr := resp.Body.Close()
	assert.Nil(t, cerr)
	if resp.StatusCode != 200 || string(body) != "proxied!" {
		t.Fatalf("HTTP: status=%d body=%q", resp.StatusCode, body)
	}
	require.Equal(t, "yes", resp.Header.Get("X-Origin"))

	// HTTPS / HTTP/2 proxy test.
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test
		},
	}
	resp2, err := client.Get("https://127.0.0.1:18091/hello")
	if err != nil {
		cancel()
		t.Fatalf("HTTPS GET: %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	cerr = resp2.Body.Close()
	assert.Nil(t, cerr)
	if resp2.StatusCode != 200 || string(body2) != "proxied!" {
		t.Fatalf("HTTPS: status=%d body=%q", resp2.StatusCode, body2)
	}
	require.Equal(t, "HTTP/1.1", resp2.Proto)

	// POST with body.
	resp3, err := http.Post("http://127.0.0.1:18090/echo", "text/plain",
		bytes.NewBufferString("ping"))
	if err != nil {
		cancel()
		t.Fatalf("POST: %v", err)
	}
	_, _ = io.ReadAll(resp3.Body)
	cerr = resp3.Body.Close()
	assert.Nil(t, cerr)
	require.Equal(t, 200, resp3.StatusCode)

	cancel()
	select {
	case err := <-errCh:
		require.NoError(t, err, "serve")
	case <-time.After(3 * time.Second):
		t.Fatal("daemon did not shut down")
	}
}
