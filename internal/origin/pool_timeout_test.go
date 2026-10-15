package origin

import (
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/valyala/fasthttp"
)

// timeoutPool builds a pool over targets with the given response-header
// timeout, so a fetch against a hanging target aborts as a timeout.
func timeoutPool(t *testing.T, responseHeaderTimeout time.Duration, targets ...string) *Pool {
	t.Helper()
	p, err := NewPool(PoolConfig{
		Name:                  "test",
		Targets:               targets,
		Logger:                slog.New(slog.NewTextHandler(io.Discard, nil)),
		ResponseHeaderTimeout: responseHeaderTimeout,
	})
	require.NoError(t, err, "NewPool")
	return p
}

// TestPool_FastHandler_Timeout_504 pins the uncached proxy path: a fetch
// that times out waiting for the upstream's response headers surfaces as
// 504 Gateway Timeout (RFC 9110 §15.6.5) — the connErrReason classification
// ("timeout") that also labels bouine_origin_connection_errors_total —
// while every other upstream failure keeps 502 Bad Gateway.
func TestPool_FastHandler_Timeout_504(t *testing.T) {
	t.Parallel()
	// A listener that accepts and never answers: the response-header
	// bound fires, not a dial error.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")
	defer ln.Close()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	go func() {
		for {
			conn, acceptErr := ln.Accept()
			if acceptErr != nil {
				return
			}
			go func() {
				<-release
				_ = conn.Close()
			}()
		}
	}()

	p := timeoutPool(t, 100*time.Millisecond, ln.Addr().String())
	h := p.FastHandler(0)

	code, _, _ := serveHandler(t, h, "GET", "/", "")
	require.Equal(t, fasthttp.StatusGatewayTimeout, code)
}

// TestPool_FastHandler_Refused_502 pins the other half of the mapping: a
// connection refused upstream is a 502 Bad Gateway, not a 504.
func TestPool_FastHandler_Refused_502(t *testing.T) {
	t.Parallel()
	// Bind, read the address, close: dialing it is guaranteed refused.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")
	addr := ln.Addr().String()
	_ = ln.Close()

	p := timeoutPool(t, 100*time.Millisecond, addr)
	h := p.FastHandler(0)

	code, _, _ := serveHandler(t, h, "GET", "/", "")
	require.Equal(t, fasthttp.StatusBadGateway, code)
}
