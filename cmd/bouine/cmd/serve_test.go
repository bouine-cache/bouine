package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/internal/testutil/poll"
	"github.com/bouine-cache/bouine/internal/testutil/tlsutil"

	"github.com/valyala/fasthttp"
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

func fastGet(t *testing.T, url string) (*fasthttp.Response, error) {
	t.Helper()
	client := &fasthttp.Client{}
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	req.SetRequestURI(url)
	req.Header.SetMethod("GET")
	if err := client.Do(req, resp); err != nil {
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
		return nil, err
	}
	fasthttp.ReleaseRequest(req)
	return resp, nil
}

func fastGetTLS(t *testing.T, url string) (*fasthttp.Response, error) {
	t.Helper()
	client := &fasthttp.Client{
		TLSConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test
	}
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	req.SetRequestURI(url)
	req.Header.SetMethod("GET")
	if err := client.Do(req, resp); err != nil {
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
		return nil, err
	}
	fasthttp.ReleaseRequest(req)
	return resp, nil
}

func fastPost(t *testing.T, url, contentType, body string) (*fasthttp.Response, error) {
	t.Helper()
	client := &fasthttp.Client{}
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	req.SetRequestURI(url)
	req.Header.SetMethod("POST")
	req.Header.SetContentType(contentType)
	req.SetBody([]byte(body))
	if err := client.Do(req, resp); err != nil {
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
		return nil, err
	}
	fasthttp.ReleaseRequest(req)
	return resp, nil
}

func TestProxyEndToEnd(t *testing.T) {
	originSrv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("X-Origin", "yes")
		_, _ = ctx.WriteString("proxied!")
	})
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
`, certPath, keyPath, originSrv.Addr)
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
	resp, err := fastGet(t, "http://127.0.0.1:18090/hello")
	if err != nil {
		cancel()
		t.Fatalf("HTTP GET: %v", err)
	}
	body := string(resp.Body())
	if resp.StatusCode() != 200 || body != "proxied!" {
		t.Fatalf("HTTP: status=%d body=%q", resp.StatusCode(), body)
	}
	require.Equal(t, "yes", string(resp.Header.Peek("X-Origin")))
	fasthttp.ReleaseResponse(resp)

	// HTTPS proxy test.
	resp2, err := fastGetTLS(t, "https://127.0.0.1:18091/hello")
	if err != nil {
		cancel()
		t.Fatalf("HTTPS GET: %v", err)
	}
	body2 := string(resp2.Body())
	if resp2.StatusCode() != 200 || body2 != "proxied!" {
		t.Fatalf("HTTPS: status=%d body=%q", resp2.StatusCode(), body2)
	}
	fasthttp.ReleaseResponse(resp2)

	// POST with body.
	resp3, err := fastPost(t, "http://127.0.0.1:18090/echo", "text/plain", "ping")
	if err != nil {
		cancel()
		t.Fatalf("POST: %v", err)
	}
	require.Equal(t, 200, resp3.StatusCode())
	fasthttp.ReleaseResponse(resp3)

	cancel()
	select {
	case err := <-errCh:
		require.NoError(t, err, "serve")
	case <-time.After(3 * time.Second):
		t.Fatal("daemon did not shut down")
	}
}
