//go:build integration

package integration

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"

	"github.com/bouine-cache/bouine/internal/testutil/poll"
	"github.com/bouine-cache/bouine/internal/testutil/tlsutil"
	"github.com/bouine-cache/bouine/test/integration/driver"
)

func TestH2_Multiplexing(t *testing.T) {
	stack := bootTLSCluster(t)

	// Use a custom dialer that counts TCP connections. With true HTTP/2
	// multiplexing, all concurrent streams share a single TCP connection.
	var connCount atomic.Int64
	transport := &http.Transport{
		ForceAttemptHTTP2: true,
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			connCount.Add(1)
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
	}
	client := &http.Client{Transport: transport}

	const numStreams = 10
	url := stack.Nodes[0].HTTPSAddr + "/hit"

	// Prime the cache so all concurrent requests are hits.
	primeResp, err := client.Get(url)
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, primeResp.Body)
	_ = primeResp.Body.Close()

	// Do NOT reset the counter — the prime request opened the single
	// TCP connection that all 10 concurrent streams will multiplex over.
	// If multiplexing works, connCount stays at 1. Without multiplexing,
	// the client would open additional connections, making connCount > 1.

	var wg sync.WaitGroup
	wg.Add(numStreams)
	results := make([]error, numStreams)
	start := make(chan struct{})

	for i := range numStreams {
		go func(idx int) {
			defer wg.Done()
			<-start
			resp, err := client.Get(url)
			if err != nil {
				results[idx] = err
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != 200 {
				results[idx] = fmt.Errorf("status %d", resp.StatusCode)
				return
			}
			if resp.Proto != "HTTP/2.0" {
				results[idx] = fmt.Errorf("proto %s, want HTTP/2.0", resp.Proto)
				return
			}
		}(i)
	}

	close(start)
	wg.Wait()

	for i, err := range results {
		assert.NoError(t, err, "stream %d failed", i)
	}

	// All 10 concurrent requests must have been multiplexed over a
	// single TCP connection.
	assert.Equal(t, int64(1), connCount.Load(),
		"all H2 streams should multiplex over a single TCP connection")
}

func TestH2_GracefulShutdownStopsNewRequests(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := tlsutil.WriteCertFiles(t, dir, "localhost")

	// Boot a single-node cluster without auto-cleanup so we control
	// shutdown explicitly.
	stack := driver.BootCluster(t, driver.ClusterOptions{
		Mode:          "strong",
		NoAutoCleanup: true,
		TLS: driver.TLSOptions{
			Enabled:  true,
			CertFile: certPath,
			KeyFile:  keyPath,
		},
	})
	defer stack.Down()

	client := &http.Client{
		Transport: &http.Transport{
			ForceAttemptHTTP2: true,
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test
		},
	}

	url := stack.Nodes[0].HTTPSAddr + "/hit"
	resp, err := client.Get(url)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	// Initiate graceful shutdown by killing node 0. The in-process
	// model cancels the context, which triggers http.Server.Shutdown.
	stack.KillNode(t, 0)

	// Poll until the server stops accepting new connections.
	poll.Eventually(t, 5*time.Second, 50*time.Millisecond, func() bool {
		r, err := client.Get(url)
		if err != nil {
			return true
		}
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		return false
	})

	// Final assertion: the server must reject new requests.
	resp, err = client.Get(url)
	if resp != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	assert.Error(t, err, "server should reject new requests after shutdown")
}

func TestH2C_PlaintextH2(t *testing.T) {
	stack := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong"})

	// Create an h2c client — plaintext HTTP/2 without TLS. The Go
	// standard library's http.Transport does not negotiate h2c
	// automatically, so we use http2.Transport with AllowHTTP and a
	// custom dial that skips TLS.
	transport := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
	}
	client := &http.Client{Transport: transport}

	// The HTTP listener (plaintext) should accept h2c connections.
	url := stack.Nodes[0].HTTPAddr + "/hit"
	resp, err := client.Get(url)
	require.NoError(t, err, "h2c request should succeed")
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()

	assert.Equal(t, 200, resp.StatusCode, "h2c response should be 200")
	assert.Equal(t, "HTTP/2.0", resp.Proto,
		"h2c response should use HTTP/2.0 protocol")
}
