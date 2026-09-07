package origin

import (
	"bufio"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/valyala/fasthttp"
)

// newSlowOrigin serves a listener that accepts a connection, reads the
// request headers, and then sleeps before writing the response, so the
// caller can assert on how the fetch deadline interacts with the wait.
func newSlowOrigin(t *testing.T, delay time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				// Drain the request head so the write side completes.
				br := bufio.NewReader(c)
				for {
					line, err := br.ReadString('\n')
					if err != nil {
						return
					}
					if line == "\r\n" || line == "\n" {
						break
					}
				}
				time.Sleep(delay)
				_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
			}(conn)
		}
	}()
	return ln.Addr().String()
}

// TestPoolFastClient_PerRequestDeadlineBeatsPoolHeaderTimeout pins the
// per-route origin-timeout capability at the transport layer: a
// PoolFastClient fetch must honour a per-request DoDeadline LONGER than
// the pool's response_header_timeout. The historical wiring copied
// response_header_timeout into the client's ReadTimeout, and fasthttp
// composes the effective read deadline as min(per-request deadline,
// client.ReadTimeout) — silently capping every route's fetch timeout at
// the pool-wide knob, so a slow AI/report endpoint could never be given
// more time than the pool default (30s) without raising it for every
// other route. The client now carries no cap; the per-request deadline
// is the sole origin-wait bound.
func TestPoolFastClient_PerRequestDeadlineBeatsPoolHeaderTimeout(t *testing.T) {
	t.Parallel()

	const originDelay = 700 * time.Millisecond
	addr := newSlowOrigin(t, originDelay)

	// Pool configured with a SHORT header timeout — the deadline passed
	// per request must still be allowed to exceed it.
	p, err := NewPool(PoolConfig{
		Name:                  "per-route",
		Targets:               []string{"http://" + addr},
		Logger:                newDiscardLogger(),
		ResponseHeaderTimeout: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close(t.Context()) })
	client := p.FastClient()

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	req.Header.SetMethod("GET")
	req.SetRequestURI("/report")
	req.SetHost(addr)

	start := time.Now()
	err = client.DoDeadline(req, resp, time.Now().Add(2*originDelay))
	require.NoError(t, err, "a per-request deadline above the pool header timeout must be honoured")
	require.GreaterOrEqual(t, time.Since(start), originDelay, "the origin wait must not have been cut short")
	require.Equal(t, 200, resp.StatusCode())
	require.Equal(t, "ok", string(resp.Body()))

	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
}

// TestPoolFastClient_ShortPerRequestDeadlineStillEnforced pins the other
// direction: a per-request deadline shorter than response_header_timeout
// cuts the fetch, so removing the client-level cap did not make fetches
// unbounded below the pool default.
func TestPoolFastClient_ShortPerRequestDeadlineStillEnforced(t *testing.T) {
	t.Parallel()

	const originDelay = 700 * time.Millisecond
	addr := newSlowOrigin(t, originDelay)

	p, err := NewPool(PoolConfig{
		Name:                  "short",
		Targets:               []string{"http://" + addr},
		Logger:                newDiscardLogger(),
		ResponseHeaderTimeout: 10 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close(t.Context()) })
	client := p.FastClient()

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	req.Header.SetMethod("GET")
	req.SetRequestURI("/report")
	req.SetHost(addr)

	err = client.DoDeadline(req, resp, time.Now().Add(150*time.Millisecond))
	require.Error(t, err, "a deadline shorter than the origin delay must abort the fetch")

	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
}
