package origin

import (
	"context"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// TestPool_DialControlInvokedOnBothClients pins the DialControl wiring:
// the shared pool client and the SSE stream client must both install
// PoolConfig.DialControl as net.Dialer.Control, so the engine's outbound
// socket options (connect.tcp_fast_open → TCP_FASTOPEN_CONNECT) reach
// every origin-bound dial whichever client serves the request.
func TestPool_DialControlInvokedOnBothClients(t *testing.T) {
	t.Parallel()
	s := fasthttptest.NewServer(t, newEchoHandler())
	defer s.Close()

	var dials atomic.Int64
	p, err := NewPool(PoolConfig{
		Name:    "dial-control",
		Targets: []string{"http://" + s.Addr},
		DialControl: func(network, addr string, c syscall.RawConn) error {
			dials.Add(1)
			return nil
		},
	})
	require.NoError(t, err, "NewPool")
	client := p.FastClient()

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	// Plain request → the shared pool client dials.
	req.Header.SetMethod("GET")
	req.SetRequestURI("/")
	req.SetHost(s.Addr)
	require.NoError(t, client.Do(context.Background(), req, resp), "shared-client fetch")
	require.Equal(t, fasthttp.StatusOK, resp.StatusCode())
	require.Positive(t, dials.Load(), "the shared pool client's dial must pass through DialControl")

	// SSE-hinted request → the stream client dials (its connection pool
	// is separate from the shared client's, so this is a fresh dial).
	dials.Store(0)
	req.Reset()
	resp.Reset()
	req.Header.SetMethod("GET")
	req.SetRequestURI("/feed")
	req.SetHost(s.Addr)
	req.Header.Set(header.Accept, "text/event-stream")
	require.NoError(t, client.Do(context.Background(), req, resp), "stream-client fetch")
	require.Equal(t, fasthttp.StatusOK, resp.StatusCode())
	require.Positive(t, dials.Load(), "the SSE stream client's dial must pass through DialControl")
}

// TestPool_DialControlNilKeepsPlainDials pins the zero value: a nil
// DialControl must leave origin dials untouched (historical behaviour,
// and the non-Linux platform surface, where the engine wires nil).
func TestPool_DialControlNilKeepsPlainDials(t *testing.T) {
	t.Parallel()
	s := fasthttptest.NewServer(t, newEchoHandler())
	defer s.Close()

	p, err := NewPool(PoolConfig{Name: "nil-control", Targets: []string{s.Addr}})
	require.NoError(t, err, "NewPool")

	h := p.FastHandler(0)
	code, _, _ := serveHandler(t, h, "GET", "/", "")
	require.Equal(t, fasthttp.StatusOK, code)
}
