package h1parser

import (
	"bufio"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

// TestServe_ReturnsToReactorAfterMiss drives the return-to-reactor
// contract on the blocking parser: after serving a miss (fallback
// handler), Serve must call the injected return hook with the
// unwrapped conn and exit with errReactorReturned instead of parking
// on the next keep-alive read. This is what re-engages the reactor
// under mixed hit/miss traffic.
func TestServe_ReturnsToReactorAfterMiss(t *testing.T) {
	t.Parallel()
	handler := func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("miss")
	}
	p := New(&mockFastPathHandler{}, handler)

	var returned net.Conn
	p.reactorReturn = func(c net.Conn) bool {
		returned = c
		return true
	}

	client, server := dialTCPPair(t)
	defer client.Close()

	go func() {
		_, _ = client.Write([]byte("GET /miss HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	}()

	done := make(chan error, 1)
	go func() { done <- p.Serve(server) }()

	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(client)
	resp := &fasthttp.Response{}
	require.NoError(t, resp.Read(reader))
	assert.Equal(t, "miss", string(resp.Body()))

	select {
	case err := <-done:
		require.ErrorIs(t, err, errReactorReturned)
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return the conn to the reactor")
	}
	require.NotNil(t, returned, "the hook must receive the conn")
	assert.Same(t, server, returned, "the hook must receive the unwrapped conn")
}

// TestServe_ReturnsToReactorAfterHitUnwrapsPrefix asserts the same
// contract after a fast-path hit, and that prefixConn layers (how the
// reactor hands the conn to the blocking parser) are stripped before
// the hook sees the conn.
func TestServe_ReturnsToReactorAfterHitUnwrapsPrefix(t *testing.T) {
	t.Parallel()
	p := New(&mockSelectiveFastPath{}, noopHandler)

	var returned net.Conn
	p.reactorReturn = func(c net.Conn) bool {
		returned = c
		return true
	}

	client, server := dialTCPPair(t)
	defer client.Close()

	go func() {
		_, _ = client.Write([]byte("GET /hit HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	}()

	// The reactor's handoff wraps the conn in a prefixConn replay.
	wrapped := &prefixConn{Conn: server, prefix: nil}

	done := make(chan error, 1)
	go func() { done <- p.Serve(wrapped) }()

	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(client)
	resp := &fasthttp.Response{}
	require.NoError(t, resp.Read(reader))
	assert.Equal(t, 200, resp.StatusCode())

	select {
	case err := <-done:
		require.ErrorIs(t, err, errReactorReturned)
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return the conn to the reactor")
	}
	assert.Same(t, server, returned, "prefixConn layers must be stripped")
}

// TestServe_ReturnHookDeclinedKeepsBlocking asserts a false return
// (queue full, shutdown) degrades gracefully: Serve keeps serving the
// connection on the blocking path.
func TestServe_ReturnHookDeclinedKeepsBlocking(t *testing.T) {
	t.Parallel()
	var attempts int
	p := New(&mockFastPathHandler{}, func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("miss")
	})
	p.reactorReturn = func(net.Conn) bool {
		attempts++
		return false
	}

	client, server := dialTCPPair(t)
	defer client.Close()

	// Sequential keep-alive writes: the blocking path's fall-through
	// consumes buffered pipelined bytes with the request (pre-existing
	// framing semantics), so the second request must arrive after the
	// first response.
	done := make(chan error, 1)
	go func() { done <- p.Serve(server) }()

	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(client)
	for _, path := range []string{"/a", "/b"} {
		go func() {
			_, _ = client.Write([]byte("GET " + path + " HTTP/1.1\r\nHost: localhost\r\n\r\n"))
		}()
		resp := &fasthttp.Response{}
		require.NoError(t, resp.Read(reader))
		assert.Equal(t, "miss", string(resp.Body()))
	}
	_ = client.Close() // Serve exits on the read error like a client reset

	select {
	case err := <-done:
		require.False(t, errors.Is(err, errReactorReturned),
			"a declined return must never surface the sentinel")
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not exit after client close")
	}
	assert.Equal(t, 2, attempts, "the hook is consulted after every served request")
}

// TestServe_NoReactorReturnUnchanged asserts the sentinel never leaks
// when no reactor transport is wired (the plain blocking path).
func TestServe_NoReactorReturnUnchanged(t *testing.T) {
	t.Parallel()
	require.Nil(t, (&Parser{}).reactorReturn, "zero value must disable the return path")

	p := New(&mockFastPathHandler{}, func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("miss")
	})
	client, server := dialTCPPair(t)
	defer client.Close()

	go func() {
		_, _ = client.Write([]byte("GET /a HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
	}()

	done := make(chan error, 1)
	go func() { done <- p.Serve(server) }()

	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(client)
	resp := &fasthttp.Response{}
	require.NoError(t, resp.Read(reader))

	select {
	case err := <-done:
		require.False(t, errors.Is(err, errReactorReturned))
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not exit")
	}
}

// TestWithReactorMetrics verifies the option wiring.
func TestWithReactorMetrics(t *testing.T) {
	t.Parallel()
	rec := &fakeReactorMetrics{}
	p := New(nil, noopHandler, WithReactorMetrics(rec))
	require.NotNil(t, p.reactorMetrics)
	p.noteReactorHit()
	p.noteReactorHandoff(api.ReactorHandoffMiss)
	p.noteReactorDrop()
	assert.Equal(t, uint64(1), rec.hits)
	assert.Equal(t, uint64(1), rec.handoffs[api.ReactorHandoffMiss])
	assert.Equal(t, uint64(1), rec.drops)
}

// compile-time: the recording fake satisfies the capability interface.
var _ api.ReactorMetrics = (*fakeReactorMetrics)(nil)
