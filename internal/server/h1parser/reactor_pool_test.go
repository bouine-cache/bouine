package h1parser

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

// TestReactorConn_ResetClearsState asserts reset() leaves the state
// machine ready for a fresh request cycle: no residual request bytes,
// flush state, or retained response from the previous life.
func TestReactorConn_ResetClearsState(t *testing.T) {
	t.Parallel()
	rc, _ := newTestReactorConn(t, nil)
	now := rc.parser.nowFunc()

	// Dirty every field reset must clear.
	rc.rLen = 128
	rc.scanned = 64
	rc.writeLen = 32
	rc.retainResp = &api.FastPathResponse{}
	rc.retainFP = &releaseSpyFP{}
	rc.writeVec = [][]byte{{0}}
	rc.closeAfterFlush = true
	rc.state = rcWriting

	rc.reset(now)

	assert.Equal(t, rcReading, rc.state)
	assert.Zero(t, rc.rLen)
	assert.Zero(t, rc.scanned)
	assert.Zero(t, rc.writeLen)
	assert.Nil(t, rc.retainResp)
	assert.Nil(t, rc.retainFP)
	assert.Empty(t, rc.writeVec)
	assert.False(t, rc.closeAfterFlush)
	assert.Equal(t, now, rc.reqStart)
}

// TestReactorConn_RecycleReleasesAndWipes asserts recycle() releases
// the retained response and write buffer (their pools) and wipes the
// struct identity so it can never be confused with a live conn.
func TestReactorConn_RecycleReleasesAndWipes(t *testing.T) {
	t.Parallel()
	rc, _ := newTestReactorConn(t, nil)
	released := false
	rc.retainResp = &api.FastPathResponse{}
	rc.retainFP = &releaseSpyFP{onRelease: func() { released = true }}

	rc.recycle()

	assert.True(t, released, "recycle must release the retained response")
	assert.Nil(t, rc.writeBuf, "the write buffer must be back in its pool")
	assert.Nil(t, rc.conn)
	assert.Equal(t, -1, rc.fd)
	assert.Nil(t, rc.parser)
}

// TestServe_DeclinedReturnRearmsReadDeadline pins the slowloris fix: a
// declined reactor return must re-arm the read deadline from now. The
// hole: the next loop-head refresh only tops up the leftover window —
// a client pacing requests just under refreshThreshold could keep a
// connection alive indefinitely on a near-zero remaining window,
// because returnFromBlocking clears the OS deadline before declining.
func TestServe_DeclinedReturnRearmsReadDeadline(t *testing.T) {
	t.Parallel()
	p := New(nil, func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("miss")
		ctx.SetStatusCode(200)
	}, WithIdleReadTimeout(2*time.Second))
	p.reactorReturn = func(net.Conn, *reactorConn) bool { return false }

	client, server := dialTCPPair(t)
	defer client.Close()

	done := make(chan error, 1)
	go func() { done <- p.Serve(server) }()

	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(client)
	for range 2 {
		go func() {
			_, _ = client.Write([]byte("GET /slow HTTP/1.1\r\nHost: localhost\r\n\r\n"))
		}()
		resp := &fasthttp.Response{}
		require.NoError(t, resp.Read(reader), "every request must be served")
		assert.Equal(t, "miss", string(resp.Body()))
	}

	// The deadline was re-armed after the declined return: a third
	// request sent after a pause longer than the leftover window (but
	// shorter than idleRead) must still be served. Without the fix the
	// conn's read deadline had already expired and Serve would time out
	// before the third response.
	time.Sleep(1200 * time.Millisecond) // > leftover window, < idleRead
	go func() {
		_, _ = client.Write([]byte("GET /third HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	}()
	resp := &fasthttp.Response{}
	require.NoError(t, resp.Read(reader), "the re-armed deadline must keep the conn alive")
	assert.Equal(t, "miss", string(resp.Body()))

	_ = client.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not exit after client close")
	}
}

// TestServe_ReturnHookReceivesPooledRC pins the rc passthrough: a conn
// handed off from a reactor carries its reactorConn in the prefixConn;
// the return hook must receive it so the loop re-registers the same
// struct instead of allocating a fresh ~20 KiB one per miss.
func TestServe_ReturnHookReceivesPooledRC(t *testing.T) {
	t.Parallel()
	p := New(&mockSelectiveFastPath{}, func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("miss")
	})

	var hookRC *reactorConn
	var hookConn net.Conn
	p.reactorReturn = func(c net.Conn, rc *reactorConn) bool {
		hookConn = c
		hookRC = rc
		return true
	}

	client, server := dialTCPPair(t)
	defer client.Close()

	// The reactor handoff pattern: the prefix conn replays buffered
	// bytes and carries the rc the transport parked for reuse.
	rc := newReactorConn(server, p, nil, nil)
	wrapped := rc.handoffConn()
	require.NotNil(t, wrapped.(*prefixConn).rc, "handoffConn must attach the rc for reuse")

	go func() {
		_, _ = client.Write([]byte("GET /miss HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	}()

	done := make(chan error, 1)
	go func() { done <- p.Serve(wrapped) }()

	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp := &fasthttp.Response{}
	require.NoError(t, resp.Read(bufio.NewReader(client)))

	select {
	case err := <-done:
		require.ErrorIs(t, err, errReactorReturned)
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return to the reactor")
	}
	assert.Same(t, server, hookConn, "the hook must receive the unwrapped conn")
	assert.Same(t, rc, hookRC, "the hook must receive the pooled rc, not a rebuild")
}
