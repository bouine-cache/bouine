package server

import (
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

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
	lim := newConnLimitListener(pl, 2, observability.NoopLogger{}, nil)
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
// limit receive a 503 response. Accept now loops internally instead of
// returning a temporary error, so we verify the 503 on the rejected client.
func TestConnLimitListener_RejectsOverLimit(t *testing.T) {
	t.Parallel()

	pl := newPipeListener()
	lim := newConnLimitListener(pl, 1, observability.NoopLogger{}, nil)

	// Fill the limit with one connection.
	client1, server1 := net.Pipe()
	pl.conns <- server1

	c1, err := lim.Accept()
	require.NoError(t, err, "first accept")
	_ = c1

	// Second connection should be rejected with a 503 response.
	// Start the reader before feeding the connection because net.Pipe
	// is synchronous (writes block until reads happen).
	client2, server2 := net.Pipe()

	readCh := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		client2.SetReadDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 256)
		n, err := client2.Read(buf)
		readCh <- struct {
			n   int
			err error
		}{n, err}
	}()

	pl.conns <- server2

	// Call Accept in a goroutine — it will reject server2 (503),
	// then loop and block waiting for the next connection.
	acceptCh := make(chan struct {
		conn net.Conn
		err  error
	}, 1)
	go func() {
		c, e := lim.Accept()
		acceptCh <- struct {
			conn net.Conn
			err  error
		}{c, e}
	}()

	// Wait for the 503 to arrive on client2.
	res := <-readCh
	require.Nil(t, res.err, "read from rejected connection")
	require.NotEqual(t, 0, res.n, "expected 503 response bytes")

	// Close c1 to free the slot, then feed a third connection to
	// unblock the Accept goroutine.
	_ = c1.Close()
	_ = client1.Close()
	_ = server1.Close()

	client3, server3 := net.Pipe()
	pl.conns <- server3

	accepted := <-acceptCh
	require.NoError(t, accepted.err, "accept after slot freed")

	_ = client2.Close()
	_ = server2.Close()
	_ = accepted.conn.Close()
	_ = client3.Close()
	_ = server3.Close()
}

// TestConnLimitListener_HTTPServeSurvives verifies that fasthttp.Server.Serve
// does not exit when the connection limit is reached. This is the real-world
// scenario: an attacker fills the limit and the server must keep running.
func TestConnLimitListener_HTTPServeSurvives(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	lim := newConnLimitListener(ln, 1, observability.NoopLogger{}, nil)
	addr := ln.Addr().String()

	srv := &fasthttp.Server{
		Handler: func(ctx *fasthttp.RequestCtx) {
			ctx.SetStatusCode(fasthttp.StatusOK)
		},
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(lim)
	}()

	// First connection — accepted, serves a request.
	c1, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	c1.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = c1.Write([]byte("GET / HTTP/1.1\r\nHost: t\r\n\r\n"))
	resp := make([]byte, 256)
	n, _ := c1.Read(resp)
	require.Contains(t, string(resp[:n]), "200")

	// Second connection — rejected (limit=1), but Serve must NOT exit.
	c2, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	c2.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ = c2.Read(resp)
	require.Contains(t, string(resp[:n]), "503")
	_ = c2.Close()

	// Close first connection to free the slot.
	_ = c1.Close()

	// Wait for the server to detect the close and release the semaphore.
	// fasthttp's keepalive read loop detects EOF asynchronously, so we
	// poll the atomic open counter instead of racing the next dial.
	cll := lim.(*connLimitListener)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&cll.open) == 0 {
			break
		}
		runtime.Gosched()
	}
	require.Equal(t, int32(0), atomic.LoadInt32(&cll.open), "slot not released after c1 close")

	// Third connection — should succeed because Serve is still running.
	c3, err := net.Dial("tcp", addr)
	require.NoError(t, err)
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
	_ = ln.Close()
	_ = srv.Shutdown()
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
	lim := newConnLimitListener(pl, max, observability.NoopLogger{}, nil)

	accepted := make([]net.Conn, max)
	for i := 0; i < max; i++ {
		_, s := net.Pipe()
		pl.conns <- s
		c, err := lim.Accept()
		require.NoErrorf(t, err, "accept %d", i)
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
		require.NoErrorf(t, err, "re-accept %d (slots not released?)", i)
		_ = c.Close()
		_ = s.Close()
	}
}

// Ensure atomic import is used.
var _ = atomic.AddInt32

// countingConnMetrics counts connection rejections through the same
// interface the listener receives in production.
type countingConnMetrics struct{ rejections atomic.Int64 }

func (m *countingConnMetrics) RecordHit(string, string, string, string, int, int, time.Duration) {
}
func (m *countingConnMetrics) IncrementSmugglingRejected() {}
func (m *countingConnMetrics) IncrementConnRejection()     { m.rejections.Add(1) }

// feedRejectedConn feeds one connection into pl that a saturated
// connLimitListener will refuse, draining the 503 the refusal writes
// (net.Pipe is synchronous: the write blocks until read).
func feedRejectedConn(t *testing.T, pl *pipeListener) {
	t.Helper()
	client, server := net.Pipe()
	go func() {
		buf := make([]byte, 128)
		_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, _ = client.Read(buf)
		_ = client.Close()
		_ = server.Close()
	}()
	pl.conns <- server
}

// TestConnLimitListener_RejectionMetric verifies that every refused
// connection increments the connection-rejection counter exactly once:
// the counter is the exact, alertable signal while the accompanying
// warn log is rate-limited. Before it existed, the only trace of the
// cliff was one warn line per refusal (24,040 lines in one measured
// connection flood) — log spending caused by the very overload the
// cap protects against.
func TestConnLimitListener_RejectionMetric(t *testing.T) {
	t.Parallel()
	m := &countingConnMetrics{}
	pl := newPipeListener()
	lim := newConnLimitListener(pl, 1, observability.NoopLogger{}, m)

	// Fill the single slot.
	client1, server1 := net.Pipe()
	pl.conns <- server1
	c1, err := lim.Accept()
	require.NoError(t, err, "first accept")

	// A looping Accept consumes the refused connections.
	go func() {
		for {
			_, err := lim.Accept()
			if err != nil {
				return
			}
		}
	}()

	feedRejectedConn(t, pl)
	feedRejectedConn(t, pl)
	require.Eventually(t, func() bool { return m.rejections.Load() == 2 },
		5*time.Second, 10*time.Millisecond, "both refusals must count exactly")

	_ = c1.Close()
	_ = client1.Close()
	_ = server1.Close()
}

// recordingLogger captures warn lines for assertions.
type recordingLogger struct {
	mu    sync.Mutex
	warns []string
}

func (l *recordingLogger) Info(string, ...any) {}
func (l *recordingLogger) Warn(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, msg)
}
func (l *recordingLogger) Error(string, ...any) {}
func (l *recordingLogger) Debug(string, ...any) {}

func (l *recordingLogger) warnCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.warns)
}

// TestConnLimitListener_RejectionWarnRateLimited verifies the warn
// rate limit: one warn line per interval, with the count of suppressed
// rejections folded into the next emitted line. The clock is injected
// (no sleeps in tests), and the exact counter keeps counting every
// refusal.
func TestConnLimitListener_RejectionWarnRateLimited(t *testing.T) {
	t.Parallel()
	var clock atomic.Int64 // unix seconds, starts far from zero
	now := func() time.Time { return time.Unix(clock.Load(), 0) }
	base := int64(1_000_000)
	rec := &recordingLogger{}

	cl := &connLimitListener{
		log:       rec,
		metrics:   &countingConnMetrics{},
		nowFn:     now,
		warnEvery: 10 * time.Second,
	}

	// First rejection at base warns immediately (lastWarn starts at
	// the zero time, so the first interval always elapses) and swaps
	// suppression back to zero.
	clock.Store(base)
	cl.reject()
	require.Equal(t, 1, rec.warnCount(), "first rejection warns")
	require.Zero(t, cl.suppressed.Load(), "the emitted line resets suppression")

	// Two more within the window: suppressed, no new warn line.
	cl.reject()
	cl.reject()
	require.Equal(t, 1, rec.warnCount(), "in-window rejections do not warn")
	require.Equal(t, int64(2), cl.suppressed.Load(), "in-window rejections accumulate")

	// After the window elapses, the next rejection warns and resets
	// the suppression count it just carried.
	clock.Store(base + 11)
	cl.reject()
	require.Equal(t, 2, rec.warnCount(), "first rejection after the window warns")
	require.Zero(t, cl.suppressed.Load(), "the emitted line resets suppression")

	// The exact counter saw every refusal: 4 total, 2 warn lines.
	require.Equal(t, int64(4), cl.metrics.(*countingConnMetrics).rejections.Load())
}
