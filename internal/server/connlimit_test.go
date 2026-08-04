package server

import (
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/observability"
)

// pipeListener is an in-memory net.Listener for testing.
type pipeListener struct {
	conns chan net.Conn
	done  chan struct{}
}

func newPipeListener() *pipeListener {
	return &pipeListener{
		conns: make(chan net.Conn, 16),
		done:  make(chan struct{}),
	}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	select {
	case <-l.done:
		// already closed
	default:
		close(l.done)
	}
	return nil
}
func (l *pipeListener) Addr() net.Addr { return pipeAddr{} }

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }

// TestConnLimitListener_AllowsWithinLimit verifies that connections under
// the limit are accepted and the semaphore is released on close.
func TestConnLimitListener_AllowsWithinLimit(t *testing.T) {
	t.Parallel()

	pl := newPipeListener()
	lim := newConnLimitListener(pl, 2, observability.NoopLogger{})
	conns := make(chan net.Conn, 4)

	go func() {
		for i := 0; i < 3; i++ {
			c, err := lim.Accept()
			if err != nil {
				conns <- nil
				return
			}
			conns <- c
		}
	}()

	// Feed 2 connections — both should be accepted.
	for i := 0; i < 2; i++ {
		client, server := net.Pipe()
		pl.conns <- server
		_ = client.Close()
	}

	c1 := <-conns
	require.NotNil(t, c1)
	c2 := <-conns
	require.NotNil(t, c2)

	// Close first connection — should release the slot.
	_ = c1.Close()

	// Feed a third connection — should be accepted because the slot was freed.
	client3, server3 := net.Pipe()
	pl.conns <- server3
	_ = client3.Close()

	c3 := <-conns
	require.NotNil(t, c3)
	_ = c2.Close()
	_ = c3.Close()
	_ = server3.Close()
}

// TestConnLimitListener_RejectsOverLimit verifies that connections over the
// limit receive a 503 response and the Accept error is temporary.
func TestConnLimitListener_RejectsOverLimit(t *testing.T) {
	t.Parallel()

	pl := newPipeListener()
	lim := newConnLimitListener(pl, 1, observability.NoopLogger{})

	// Fill the limit with one connection.
	client1, server1 := net.Pipe()
	pl.conns <- server1

	c1, err := lim.Accept()
	require.NoErrorf(t, err, "first accept: %v", err)
	_ = c1

	// Second connection should be rejected with a temporary error.
	client2, server2 := net.Pipe()
	pl.conns <- server2

	// Read the 503 response concurrently — net.Pipe is synchronous, so
	// the Accept's write will block until we read.
	readCh := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		client2.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 256)
		n, err := client2.Read(buf)
		readCh <- struct {
			n   int
			err error
		}{n, err}
	}()

	_, err = lim.Accept()
	require.Error(t, err)

	// The error must implement net.Error with Temporary()=true.
	ne, ok := err.(net.Error)
	require.True(t, ok)
	//nolint:staticcheck // Temporary is deprecated but http.Server.Serve still checks it
	require.True(t, ne.Temporary())

	// The rejected client should receive a 503 response.
	res := <-readCh
	require.Nil(t, res.err)
	require.NotEqual(t, 0, res.n)

	_ = client1.Close()
	_ = client2.Close()
	_ = server1.Close()
	_ = server2.Close()
}

// TestConnLimitListener_HTTPServeSurvives verifies that http.Server.Serve
// does not exit when the connection limit is reached. This is the real-world
// scenario: an attacker fills the limit and the server must keep running.
func TestConnLimitListener_HTTPServeSurvives(t *testing.T) {
	t.Parallel()

	pl := newPipeListener()
	lim := newConnLimitListener(pl, 1, observability.NoopLogger{})

	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(lim)
	}()

	// First connection — accepted, serves a request.
	c1, s1 := net.Pipe()
	pl.conns <- s1
	c1.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = c1.Write([]byte("GET / HTTP/1.1\r\nHost: t\r\n\r\n"))
	resp := make([]byte, 256)
	n, _ := c1.Read(resp)
	require.Contains(t, string(resp[:n]), "200")

	// Second connection — rejected (limit=1), but Serve must NOT exit.
	c2, s2 := net.Pipe()
	pl.conns <- s2
	c2.SetReadDeadline(time.Now().Add(time.Second))
	n, _ = c2.Read(resp)
	require.Contains(t, string(resp[:n]), "503")
	_ = c2.Close()
	_ = s2.Close()

	// Close first connection to free the slot.
	_ = c1.Close()

	// Third connection — should succeed because Serve is still running.
	c3, s3 := net.Pipe()
	pl.conns <- s3
	c3.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = c3.Write([]byte("GET / HTTP/1.1\r\nHost: t\r\n\r\n"))
	n, _ = c3.Read(resp)
	require.Contains(t, string(resp[:n]), "200")

	// Verify Serve is still running.
	select {
	case err := <-serveErr:
		t.Fatalf("Serve exited unexpectedly: %v", err)
	default:
	}

	_ = c3.Close()
	_ = s3.Close()
	_ = pl.Close()
	_ = srv.Close()
}

// TestConnLimitConn_DoubleClose verifies that closing a connection twice
// releases the semaphore slot only once.
func TestConnLimitConn_DoubleClose(t *testing.T) {
	t.Parallel()

	sem := make(chan struct{}, 2)
	var open int32
	inner, outer := net.Pipe()
	c := &connLimitConn{Conn: inner, sem: sem, open: &open}

	// Simulate the Accept path: Accept sends to sem (acquire), Close receives (release).
	sem <- struct{}{}
	atomic.AddInt32(&open, 1)

	_ = c.Close()
	_ = c.Close() // must not panic, must not release twice

	// After Close, the slot should be available (can acquire again).
	select {
	case sem <- struct{}{}:
	default:
		t.Fatal("semaphore slot not released after close")
	}

	// Only one slot should have been released — the second Close is a no-op.
	// We acquired 1 above, and the channel had capacity 2 with 1 item before Close.
	// After Close (release 1) + our acquire (1), we're back to 1 item.
	// A second acquire should succeed (capacity 2, 1 item).
	select {
	case sem <- struct{}{}:
	default:
		t.Fatal("second acquire should succeed within capacity")
	}

	_ = outer.Close()
}

// TestConnLimitListener_ConcurrentClose verifies that concurrent Close
// calls on different connections don't race on the semaphore.
func TestConnLimitListener_ConcurrentClose(t *testing.T) {
	t.Parallel()

	const max = 8
	pl := newPipeListener()
	lim := newConnLimitListener(pl, max, observability.NoopLogger{})

	accepted := make([]net.Conn, max)
	for i := 0; i < max; i++ {
		_, s := net.Pipe()
		pl.conns <- s
		c, err := lim.Accept()
		require.NoErrorf(t, err, "accept %d: %v", i, err)
		accepted[i] = c
	}

	var wg sync.WaitGroup
	for _, c := range accepted {
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			_ = c.Close()
		}(c)
	}
	wg.Wait()

	// All slots should be free — verify by accepting max new connections.
	for i := 0; i < max; i++ {
		_, s := net.Pipe()
		pl.conns <- s
		c, err := lim.Accept()
		require.NoErrorf(t, err, "re-accept %d: %v (slots not released?)", i, err)
		_ = c.Close()
		_ = s.Close()
	}
}

// Ensure atomic import is used.
var _ = atomic.AddInt32
