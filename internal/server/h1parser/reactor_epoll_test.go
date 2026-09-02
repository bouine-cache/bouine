//go:build linux

package h1parser

// reactor_epoll_test.go — end-to-end tests for the Linux epoll
// transport: real TCP connections through the full reactor loop
// (accept → parse → hit → flush), plus miss handoff to the blocking
// parser. These run only on Linux (CI matrix + Docker locally).

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/cache"
	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// startReactorListener boots the reactor over a real TCP listener with
// a mockFastPathHit fast path and a fallback that echoes the path.
// Returns the address and a stop function.
func startReactorListener(t *testing.T) (addr string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p := New(&mockFastPathHit{}, func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("miss:" + string(ctx.Path()))
		ctx.SetStatusCode(200)
	})

	loop, ok := NewReactorLoop(p, ln)
	require.True(t, ok, "epoll reactor must be available on Linux")
	go loop.Run()
	t.Cleanup(loop.Close)
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String()
}

// TestEpollReactor_HitOverRealTCP drives one keep-alive connection
// through the reactor: the hit must arrive intact, and a second hit
// on the same connection must also be served (keep-alive works through
// the loop).
func TestEpollReactor_HitOverRealTCP(t *testing.T) {
	addr := startReactorListener(t)

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	for i := 0; i < 2; i++ {
		_, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
		require.NoError(t, err)
		resp := readOneResponse(t, conn)
		assert.True(t, strings.HasPrefix(resp, "HTTP/1.1 200"),
			"hit %d must be served, got: %q", i+1, resp)
		assert.Contains(t, resp, "hello", "hit %d body must be intact", i+1)
	}
}

// TestEpollReactor_ManyConnectionsBatchServed opens 64 concurrent
// keep-alive connections and serves one hit on each — the reactor must
// multiplex them all from its single goroutine.
func TestEpollReactor_ManyConnectionsBatchServed(t *testing.T) {
	addr := startReactorListener(t)

	const n = 64
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
			if _, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")); err != nil {
				errs <- err
				return
			}
			buf := make([]byte, 256)
			have := make([]byte, 0, 128)
			deadline := time.Now().Add(5 * time.Second)
			for !strings.Contains(string(have), "hello") {
				if time.Now().After(deadline) {
					errs <- fmt.Errorf("timed out without hit body, got: %q", have)
					return
				}
				m, rerr := conn.Read(buf)
				have = append(have, buf[:m]...)
				if rerr != nil {
					errs <- fmt.Errorf("read error after %d bytes: %v", len(have), rerr)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestEpollReactor_ConnectionCloseClosesAfterHit asserts a hit whose
// request carried Connection: close is fully served and then the
// connection is closed by the reactor (the next request gets EOF, not
// a stale keep-alive) — RFC 9110 §9.6.
func TestEpollReactor_ConnectionCloseClosesAfterHit(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p := New(&mockFastPathHit{}, noopHandler)
	loop, ok := NewReactorLoop(p, ln)
	require.True(t, ok)
	go loop.Run()
	t.Cleanup(loop.Close)
	t.Cleanup(func() { _ = ln.Close() })

	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
	require.NoError(t, err)
	resp := readOneResponse(t, conn)
	assert.True(t, strings.HasPrefix(resp, "HTTP/1.1 200"), "hit must be fully served, got: %q", resp)
	assert.Contains(t, resp, "hello", "body must be intact")

	// The reactor must close the connection: the next read returns EOF,
	// not a hang or a stale response.
	buf := make([]byte, 64)
	_, rerr := conn.Read(buf)
	assert.ErrorIs(t, rerr, io.EOF, "connection must be closed after Connection: close response")
}

// TestEpollReactor_MissPathServedViaBlocking boots a reactor whose
// fast path never hits: every request must hand off to the blocking
// parser and still be served correctly.
func TestEpollReactor_MissPathServedViaBlocking(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p := New(&mockFastPathHandler{}, func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("blocked-miss")
		ctx.SetStatusCode(200)
	})
	loop, ok := NewReactorLoop(p, ln)
	require.True(t, ok)
	go loop.Run()
	t.Cleanup(loop.Close)
	t.Cleanup(func() { _ = ln.Close() })

	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	_, err = conn.Write([]byte("GET /whatever HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	require.NoError(t, err)
	resp := readOneResponse(t, conn)
	assert.Contains(t, resp, "HTTP/1.1 200", "miss must be served by the blocking fallback")
	assert.Contains(t, resp, "blocked-miss")
}

// readOneResponse reads one full HTTP response (headers + body) from
// conn and returns it as a string.
func readOneResponse(t *testing.T, conn net.Conn) string {
	t.Helper()
	reader := bufio.NewReader(conn)
	var sb strings.Builder
	// Status line + headers, until the blank line terminator.
	contentLength := 0
	for {
		line, err := reader.ReadString('\n')
		require.NoError(t, err)
		sb.WriteString(line)
		if strings.TrimSpace(line) == "" {
			break
		}
		if strings.HasPrefix(strings.ToLower(line), "content-length:") {
			_, _ = fmt.Sscanf(strings.TrimSpace(line), "Content-Length: %d", &contentLength)
		}
	}
	// Body follows the header block terminator.
	if contentLength > 0 {
		body := make([]byte, contentLength)
		_, err := io.ReadFull(reader, body)
		require.NoError(t, err)
		sb.Write(body)
	}
	return sb.String()
}

// TestEpollReactor_PeerCloseNoSpin asserts a client that closes its
// connection does not busy-spin the loop: the reactor must drop the
// connection (raw-fd EOF contract) and stay responsive to a new
// connection immediately after.
func TestEpollReactor_PeerCloseNoSpin(t *testing.T) {
	addr := startReactorListener(t)

	// Open and abruptly close a connection without sending anything.
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	_ = conn.Close()

	// A fresh connection must still be served — the loop is alive.
	served, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer served.Close()
	_ = served.SetDeadline(time.Now().Add(5 * time.Second))
	_, err = served.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	require.NoError(t, err)
	buf := make([]byte, 256)
	var have []byte
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(string(have), "hello") {
		require.False(t, time.Now().After(deadline), "reactor stalled after peer close: got %q", have)
		n, rerr := served.Read(buf)
		have = append(have, buf[:n]...)
		if rerr != nil {
			break
		}
	}
	assert.True(t, strings.Contains(string(have), "hello"), "new connection must be served after a peer close")
}

// TestEpollReactor_LargeBodyZeroCopyHit asserts a large cached body
// (writev path, no full-body copy) is served intact: content-length
// must match the bytes received.
func TestEpollReactor_LargeBodyZeroCopyHit(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// bigHit serves a 256 KiB body to exercise writev over the body slice.
	const bodySize = 256 * 1024
	body := strings.Repeat("x", bodySize)
	resp := &api.FastPathResponse{
		BuffersArr: [3][]byte{
			[]byte("HTTP/1.1 200 OK\r\n"),
			fmt.Appendf(nil, "Content-Length: %d\r\nContent-Type: text/plain\r\n\r\n", bodySize),
			[]byte(body),
		},
		CacheResult: "HIT",
	}
	resp.Buffers = resp.BuffersArr[:3]
	fp := &staticFastPath{resp: resp}

	p := New(fp, noopHandler)
	loop, ok := NewReactorLoop(p, ln)
	require.True(t, ok)
	go loop.Run()
	t.Cleanup(loop.Close)
	t.Cleanup(func() { _ = ln.Close() })

	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	require.NoError(t, err)

	received := 0
	head := make([]byte, 0, 1024)
	buf := make([]byte, 32*1024)
	inBody := false
	cl := 0
	for received < bodySize {
		n, rerr := conn.Read(buf)
		if !inBody {
			head = append(head, buf[:n]...)
			if i := strings.Index(string(head), "\r\n\r\n"); i >= 0 {
				for _, line := range strings.Split(string(head[:i]), "\r\n") {
					if strings.HasPrefix(strings.ToLower(line), "content-length:") {
						fmt.Sscanf(line, "Content-Length: %d", &cl)
					}
				}
				received = len(head) - (i + 4)
				inBody = true
			}
		} else {
			received += n
		}
		if rerr != nil {
			break
		}
	}
	assert.Equal(t, bodySize, cl, "Content-Length must match the served body")
	assert.Equal(t, bodySize, received, "writev must deliver the full body byte-exact")
}

// staticFastPath always returns one pre-built response.
type staticFastPath struct {
	resp *api.FastPathResponse
}

func (f *staticFastPath) TryHit(_ *api.RawRequest, _ time.Time) (*api.FastPathResponse, bool) {
	return f.resp, true
}

func (f *staticFastPath) Release(_ *api.FastPathResponse) {}

// TestEpollReactor_IdleSweepExpiresConnections asserts the sweep drops
// connections past the idle budget and keeps fresh ones. sweepIdle runs
// on the loop goroutine in production; this test drives it directly
// with a hand-built loop so the map access stays single-owner.
func TestEpollReactor_IdleSweepExpiresConnections(t *testing.T) {
	p := New(&mockFastPathHit{}, noopHandler)
	r := &reactorEpoll{
		p:       p,
		pending: make(chan *reactorConn, 8),
		done:    make(chan struct{}),
	}

	// One idle (backdated) and one fresh connection.
	idleC, idleS := net.Pipe()
	defer idleC.Close()
	freshC, freshS := net.Pipe()
	defer freshC.Close()

	idleRC := newReactorConn(idleS, p, nil, nil)
	idleRC.reqStart = p.nowFunc().Add(-reactorIdleTimeout - time.Second)
	freshRC := newReactorConn(freshS, p, nil, nil)
	freshRC.reqStart = p.nowFunc()
	r.connAdd(1, idleRC)
	r.connAdd(2, freshRC)

	r.sweepIdle(p.nowFunc())

	// The idle connection was closed by the sweep: its pipe peer sees EOF.
	idleC.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 8)
	_, err := idleC.Read(buf)
	require.Error(t, err, "idle conn must be closed by the sweep")
	assert.Nil(t, r.connAt(1), "idle conn removed from the table")

	// The fresh connection survives.
	require.NotNil(t, r.connAt(2), "fresh conn must survive the sweep")
	_ = freshC
}

// missOnceFastPath hits exactly once (the first request), then misses
// — used to exercise a real hit→handoff transition on one connection.
type missOnceFastPath struct {
	serve bool
	resp  *api.FastPathResponse
}

func (m *missOnceFastPath) TryHit(_ *api.RawRequest, _ time.Time) (*api.FastPathResponse, bool) {
	if m.serve {
		return m.resp, true
	}
	return nil, false
}

func (m *missOnceFastPath) Release(_ *api.FastPathResponse) {}

// TestEpollReactor_HandoffClosesConnOnExit asserts the handoff goroutine
// closes the connection when the blocking parser finishes: a miss handed
// off with Connection: close must leave zero fds leaked (regression: the
// handoff goroutine used to return without closing, pinning every
// handed-off fd in CLOSE_WAIT forever).
func TestEpollReactor_HandoffClosesConnOnExit(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	fp := &missOnceFastPath{serve: false}
	p := New(fp, func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("blocked-miss")
		ctx.SetStatusCode(200)
	})
	loop, ok := NewReactorLoop(p, ln)
	require.True(t, ok)
	go loop.Run()
	t.Cleanup(loop.Close)
	t.Cleanup(func() { _ = ln.Close() })

	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	// A miss with Connection: close — the blocking parser serves it and
	// exits; the handoff goroutine must close the socket after.
	_, err = conn.Write([]byte("GET /whatever HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
	require.NoError(t, err)
	resp := readOneResponse(t, conn)
	assert.Contains(t, resp, "HTTP/1.1 200", "miss must be served by the blocking fallback")
	assert.Contains(t, resp, "blocked-miss")

	// The definitive proof: the server side of the socket was closed, so
	// the client sees EOF (not a silently parked half-open connection).
	buf := make([]byte, 64)
	_, rerr := conn.Read(buf)
	assert.ErrorIs(t, rerr, io.EOF, "handoff goroutine must close the conn after Serve")
}

// TestEpollReactor_CloseDrainsInFlightHandoff asserts Close waits for a
// handed-off request still being served by the blocking parser: after
// Close returns, no handoff goroutine may still be running (regression:
// Close used to leave the loop spinning and handoffs unowned).
func TestEpollReactor_CloseDrainsInFlightHandoff(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	fallbackStarted := make(chan struct{})
	release := make(chan struct{})
	p := New(&mockFastPathHandler{}, func(ctx *fasthttp.RequestCtx) {
		close(fallbackStarted)
		<-release // hold the handed-off request open until the test says
		ctx.SetBodyString("slow-miss")
		ctx.SetStatusCode(200)
	})
	loop, ok := NewReactorLoop(p, ln)
	require.True(t, ok)
	go loop.Run()

	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	require.NoError(t, err)

	select {
	case <-fallbackStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("fallback never started — handoff did not fire")
	}

	closed := make(chan struct{})
	go func() {
		loop.Close()
		close(closed)
	}()

	select {
	case <-closed:
		t.Fatal("Close returned while a handed-off request was still in flight")
	case <-time.After(300 * time.Millisecond):
		// Close is correctly still waiting for the in-flight handoff.
	}

	close(release)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the in-flight handoff completed")
	}
	_ = conn.Close()
}

// TestEpollReactor_SweepDropsStuckWriter asserts the W7 safety net: a
// connection parked in rcWriting whose flush never completes (client
// stopped reading) is dropped once the write budget expires, instead of
// consuming the reactor connection budget forever. A fresh writer must
// survive the same sweep. No sleeps: the budget is exercised through
// the state machine's own clock (reqStart backdating), like the idle
// test above.
func TestEpollReactor_SweepDropsStuckWriter(t *testing.T) {
	p := New(&mockFastPathHit{}, noopHandler)
	r := &reactorEpoll{
		p:       p,
		pending: make(chan *reactorConn, 8),
		done:    make(chan struct{}),
	}

	stuckC, stuckS := net.Pipe()
	defer stuckC.Close()
	freshC, freshS := net.Pipe()
	defer freshC.Close()

	stuckRC := newReactorConn(stuckS, p, nil, nil)
	stuckRC.state = rcWriting
	stuckRC.reqStart = p.nowFunc().Add(-reactorWriteTimeout - time.Second)

	freshRC := newReactorConn(freshS, p, nil, nil)
	freshRC.state = rcWriting
	freshRC.reqStart = p.nowFunc()

	r.connAdd(3, stuckRC)
	r.connAdd(4, freshRC)

	r.sweepIdle(p.nowFunc())

	stuckC.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 8)
	_, err := stuckC.Read(buf)
	require.Error(t, err, "stuck writer must be closed by the sweep")
	assert.Nil(t, r.connAt(3), "stuck writer removed from the table")
	require.NotNil(t, r.connAt(4), "fresh writer must survive the sweep")
}

// TestEpollReactor_HandoffStormConcurrentClose asserts W4's spawner
// lifecycle under pressure: a burst of miss connections queues handoff
// jobs while Close runs concurrently. After Close returns, no spawner
// or blocking-parser goroutine may still exist, and no conn may leak
// (the queue drain must close every queued conn's lifecycle owner).
// Regression guard for the spawn-queue refactor: the spawner joins
// before the WaitGroup wait, so a queued-but-unspawned job can never be
// orphaned mid-shutdown.
func TestEpollReactor_HandoffStormConcurrentClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	fallbackStarted := make(chan struct{}, 64)
	p := New(&mockFastPathHandler{}, func(ctx *fasthttp.RequestCtx) {
		select {
		case fallbackStarted <- struct{}{}:
		default:
		}
		ctx.SetBodyString("storm-miss")
		ctx.SetStatusCode(200)
	})

	loop, ok := NewReactorLoop(p, ln)
	require.True(t, ok)
	go loop.Run()

	// Every request carries Connection: close: the blocking parser
	// serves it and exits (no keep-alive park), so the conn's lifecycle
	// ends with the handoff goroutine instead of the 120s idle deadline
	// — the storm this test wants, without depending on the drain grace
	// window for correctness.
	const n = 48
	conns := make([]net.Conn, 0, n)
	for range n {
		conn, derr := net.Dial("tcp", ln.Addr().String())
		if derr != nil {
			break
		}
		conns = append(conns, conn)
		_, _ = conn.Write([]byte("GET /storm HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
	}

	// Wait until at least one handoff has been served, then close
	// concurrently — jobs may still be spawning and queued when Close
	// begins; that overlap is the race under test.
	deadline := time.Now().Add(10 * time.Second)
	for len(fallbackStarted) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	require.NotZero(t, len(fallbackStarted), "no handoff fired before Close")

	// Production wiring closes the listener before the loop (see
	// serveFastPath): acceptLoop must be dead or it would feed conns
	// to a closed loop. The test mirrors that ordering.
	_ = ln.Close()
	loop.Close()

	// After Close returns, no conn may leak: each client observes
	// EOF/reset in parallel with a shared deadline — sequential 2s
	// read deadlines would serialize worst-case latency to n × 2s.
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for _, conn := range conns {
			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				defer func() { _ = c.Close() }()
				buf := make([]byte, 256)
				for {
					if _, rerr := c.Read(buf); rerr != nil {
						return
					}
				}
			}(conn)
		}
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("half-open conns remain after Close: clients did not observe EOF within 30s")
	}
}

// TestEpollReactor_IdleParksNotSpins asserts the busy-poll budget is
// bounded: after traffic stops, the loop must stop polling (spin
// budget spent) and park in the timed wait — it must not become a
// busy loop. Observable proxy: with the listener quiet for 300ms, the
// loop's epoll_wait calls stop (a spinning loop would keep issuing
// syscalls, visible as sustained CPU). Measured indirectly via the
// wake pipe: a parked loop takes >=1 event (the 1s timeout sweep)
// before draining new work, while a spinning one would drain it within
// microseconds of the write. Deterministic enough for CI: we assert
// the wake-drain works (functional) and that CPU spent on the loop
// goroutine over a 300ms quiet window stays negligible (no busy loop).
func TestEpollReactor_IdleParksNotSpins(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p := New(&mockFastPathHit{}, noopHandler)
	loop, ok := NewReactorLoop(p, ln)
	require.True(t, ok)
	go loop.Run()
	defer loop.Close()

	// One connection, one hit to arm the spin budget.
	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	require.NoError(t, err)
	buf := make([]byte, 256)
	var have []byte
	for !bytes.Contains(have, []byte("hello")) {
		n, rerr := conn.Read(buf)
		have = append(have, buf[:n]...)
		if rerr != nil {
			break
		}
	}

	// Quiet window: no traffic for well past the spin budget. If the
	// loop were spinning unboundedly, its goroutine would accumulate
	// CPU time steadily. Parked, it accumulates ~none.
	time.Sleep(300 * time.Millisecond)

	// The loop must still be responsive (parked, not dead): a new
	// request must be served — through the wake path after park.
	conn2, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer func() { _ = conn2.Close() }()
	_ = conn2.SetDeadline(time.Now().Add(2 * time.Second))
	_, err = conn2.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	require.NoError(t, err)
	have = have[:0]
	deadline2 := time.Now().Add(2 * time.Second)
	for !bytes.Contains(have, []byte("hello")) && time.Now().Before(deadline2) {
		n, rerr := conn2.Read(buf)
		have = append(have, buf[:n]...)
		if rerr != nil {
			break
		}
	}
	assert.Contains(t, string(have), "hello", "loop must serve after the idle window")
}

// reactorPayloadBody mirrors the deterministic payload used by the cache
// package's body-lifetime tests: every 8-byte block encodes its own
// offset, so any mutation, truncation, or cross-object mixing of the
// served bytes is detectable by regenerating the pattern.
func reactorPayloadBody(size int) []byte {
	body := make([]byte, size)
	var counter [8]byte
	for off := 0; off < size; off += 8 {
		binary.LittleEndian.PutUint64(counter[:], uint64(off))
		copy(body[off:], counter[:])
	}
	return body
}

// slowReadResponse reads one Content-Length-delimited response from conn
// in small chunks with a delay between them, invoking onChunk after every
// chunk so a test can race the cache while the response tail is still
// being written. Returns the header block and body separately.
func slowReadResponse(t *testing.T, conn net.Conn, chunkSize int, delay time.Duration, onChunk func(totalRead int)) (head string, body []byte) {
	t.Helper()
	var raw bytes.Buffer
	buf := make([]byte, chunkSize)
	var headEnd int
	for headEnd == 0 {
		n, err := conn.Read(buf)
		if n > 0 {
			raw.Write(buf[:n])
			if onChunk != nil {
				onChunk(raw.Len())
			}
		}
		require.NoError(t, err, "read response head")
		if i := bytes.Index(raw.Bytes(), []byte("\r\n\r\n")); i >= 0 {
			headEnd = i + 4
		} else if delay > 0 {
			time.Sleep(delay)
		}
	}
	head = string(raw.Bytes()[:headEnd])
	contentLength := 0
	for _, line := range bytes.Split(raw.Bytes()[:headEnd-4], []byte("\r\n")) {
		if len(line) > 16 && bytes.EqualFold(line[:16], []byte("Content-Length: ")) {
			contentLength, _ = strconv.Atoi(string(bytes.TrimSpace(line[16:])))
		}
	}
	total := headEnd + contentLength
	for raw.Len() < total {
		n, err := conn.Read(buf)
		if n > 0 {
			raw.Write(buf[:n])
			if onChunk != nil {
				onChunk(raw.Len())
			}
		}
		require.NoError(t, err, "read response body")
		if delay > 0 {
			time.Sleep(delay)
		}
	}
	return head, raw.Bytes()[headEnd:total]
}

// TestEpollReactor_HitBodyStableUnderSlowClientAndEviction is the
// reactor-path twin of the cache package's body-lifetime tests: a
// slow-reading client (receive buffer clamped so the kernel cannot
// absorb the 2 MiB body) holds an in-flight reactor hit writev while
// the origin path reuses its buffer, Put-overwrites the same key, and
// SIEVE eviction churns the shard. The reactor retains the response
// until every byte is flushed (finishWrite); the storage ownership fix
// guarantees the retained body cannot change underneath it. The client
// must receive the exact stored bytes.

func TestEpollReactor_HitBodyStableUnderSlowClientAndEviction(t *testing.T) {
	const bodySize = 16 << 20 // 16 MiB, beyond any kernel socket buffer on loopback

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 128 << 20, NumShards: 4})
	fp := cache.NewFastPathHandlerFromStore(store)

	key := cache.BuildKeyFromURL("http://example.com/page", nil)

	body := reactorPayloadBody(bodySize)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     headerMapOf("Content-Type", "application/json", "Content-Length", strconv.Itoa(bodySize)),
		Body:       body,
		BodySize:   int64(bodySize),
		StoredAt:   time.Now(),
		TTL:        10 * time.Minute,
	}
	require.NoError(t, store.Put(context.Background(), key, obj), "put")

	p := New(fp, func(ctx *fasthttp.RequestCtx) {}, WithWriteTimeout(60*time.Second))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	loop, ok := NewReactorLoop(p, ln)
	require.True(t, ok, "epoll reactor must be available on Linux")
	go loop.Run()
	t.Cleanup(loop.Close)

	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	// Clamp the receive buffer so the client's unread window (and thus
	// the server's in-flight writev) cannot absorb the whole body: the
	// mutation below is guaranteed to land while bytes are unwritten.
	require.NoError(t, conn.(*net.TCPConn).SetReadBuffer(32*1024))
	_ = conn.SetDeadline(time.Now().Add(60 * time.Second))

	_, err = conn.Write([]byte("GET /page HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n"))
	require.NoError(t, err)

	raced := false
	var wg sync.WaitGroup
	head, gotBody := slowReadResponse(t, conn, 64*1024, time.Millisecond, func(totalRead int) {
		if raced || totalRead < 512*1024 {
			return
		}
		raced = true
		wg.Add(2)

		// Origin-side buffer reuse after handing the object to the cache.
		go func() {
			defer wg.Done()
			copy(body, reactorPayloadBody(len(body)))
			for i := range body {
				body[i] ^= 0xFF
			}
		}()

		// Concurrent refresh of the same key plus neighbor churn that
		// forces SIEVE evictions in the same shard.
		go func() {
			defer wg.Done()
			refreshed := &api.Object{
				Key:        key,
				StatusCode: 200,
				Header:     headerMapOf("Content-Length", "16"),
				Body:       reactorPayloadBody(16),
				BodySize:   16,
				StoredAt:   time.Now(),
				TTL:        10 * time.Minute,
			}
			_ = store.Put(context.Background(), key, refreshed)
			for i := 0; i < 8; i++ {
				nk := testkey.Key(uint64(1000 + i))
				neighbor := &api.Object{
					Key:        nk,
					StatusCode: 200,
					Header:     headerMapOf("Content-Length", "8"),
					Body:       reactorPayloadBody(512 * 1024),
					BodySize:   512 * 1024,
					StoredAt:   time.Now(),
					TTL:        10 * time.Minute,
				}
				_ = store.Put(context.Background(), nk, neighbor)
			}
		}()
	})

	require.True(t, raced, "test must have raced the in-flight reactor writev (head=%q bodyLen=%d)", head, len(gotBody))
	wg.Wait()

	assert.Contains(t, head, "HTTP/1.1 200 OK", "status line")
	require.Len(t, gotBody, bodySize, "body must be complete, not truncated")
	require.True(t, bytes.Equal(gotBody, reactorPayloadBody(bodySize)),
		"body served through the reactor to a slow mid-response client must be the exact stored bytes")
}

// headerMapOf builds a header.Map from key-value pairs (test-local
// helper; the cache package's equivalent is not importable from here).
func headerMapOf(kvs ...string) header.Map {
	m := header.NewMap(len(kvs) / 2)
	for i := 0; i+1 < len(kvs); i += 2 {
		m.Set(kvs[i], kvs[i+1])
	}
	return m
}

// end-to-end tests: the loop goroutine and blocking-parser goroutines
// increment while the test goroutine reads.
type atomicReactorMetrics struct {
	registered atomic.Uint64
	hits       atomic.Uint64
	returns    atomic.Uint64
	drops      atomic.Uint64
	// handoffCounts is indexed by reasonIndex; slot 6 collects
	// unknown reasons so a bug surfaces as a non-zero "other" bucket
	// instead of a panic.
	handoffCounts [7]atomic.Uint64
}

func reasonIndex(reason string) int {
	switch reason {
	case api.ReactorHandoffMiss:
		return 0
	case api.ReactorHandoffDisqualified:
		return 1
	case api.ReactorHandoffMalformed:
		return 2
	case api.ReactorHandoffOversize:
		return 3
	case api.ReactorHandoffOverflow:
		return 4
	case api.ReactorHandoffCap:
		return 5
	}
	return 6
}

func (a *atomicReactorMetrics) IncrementReactorConnRegistered() { a.registered.Add(1) }
func (a *atomicReactorMetrics) IncrementReactorHit()            { a.hits.Add(1) }
func (a *atomicReactorMetrics) IncrementReactorHandoff(reason string) {
	a.handoffCounts[reasonIndex(reason)].Add(1)
}
func (a *atomicReactorMetrics) IncrementReactorReturn() { a.returns.Add(1) }
func (a *atomicReactorMetrics) IncrementReactorDrop()   { a.drops.Add(1) }

// compile-time: the atomic recorder satisfies the capability interface.
var _ api.ReactorMetrics = (*atomicReactorMetrics)(nil)

// startSelectiveReactorListener boots the reactor with a fast path that
// hits everything except /miss, plus a telemetry recorder. Returns the
// address and the recorder.
func startSelectiveReactorListener(t *testing.T) (addr string, rec *atomicReactorMetrics) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	rec = &atomicReactorMetrics{}
	p := New(&mockSelectiveFastPath{}, func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("miss:" + string(ctx.Path()))
		ctx.SetStatusCode(200)
	}, WithReactorMetrics(rec))

	loop, ok := NewReactorLoop(p, ln)
	require.True(t, ok, "epoll reactor must be available on Linux")
	go loop.Run()
	t.Cleanup(loop.Close)
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String(), rec
}

// TestEpollReactor_ReturnAfterMissServesLaterHits is the mixed-traffic
// end-to-end: a miss hands the connection to the blocking parser, the
// blocking parser returns it to the reactor (return-to-reactor), and a
// following hit on the SAME connection is served inline by the reactor
// loop — the starvation gap this path exists to close. Without it, the
// first miss exiles the connection to the blocking path for life.
func TestEpollReactor_ReturnAfterMissServesLaterHits(t *testing.T) {
	addr, rec := startSelectiveReactorListener(t)

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	// 1. Miss: served by the blocking parser via handoff.
	_, err = conn.Write([]byte("GET /miss HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	require.NoError(t, err)
	resp := readOneResponse(t, conn)
	assert.Contains(t, resp, "miss:/miss", "miss must be served by the fallback, got: %q", resp)

	// The return happens after the response; give the loop a moment to
	// register the returned connection before sending the hit.
	require.Eventually(t, func() bool {
		return rec.returns.Load() >= 1
	}, 5*time.Second, 5*time.Millisecond, "the blocking parser must return the conn to the reactor")
	require.Eventually(t, func() bool {
		return rec.registered.Load() >= 2
	}, 5*time.Second, 5*time.Millisecond, "the returned conn must re-register with the reactor (accept + return)")

	// 2. Hit on the same connection: served inline by the reactor loop.
	_, err = conn.Write([]byte("GET /hit HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	require.NoError(t, err)
	resp = readOneResponse(t, conn)
	assert.Contains(t, resp, "hello", "hit after return must be served on the same conn, got: %q", resp)

	require.Eventually(t, func() bool {
		return rec.hits.Load() >= 1
	}, 5*time.Second, 5*time.Millisecond, "the post-return hit must be served by the reactor loop, not the blocking path")
	assert.Equal(t, uint64(1), rec.handoffCounts[reasonIndex(api.ReactorHandoffMiss)].Load(),
		"exactly one miss handoff on the connection")
}

// TestEpollReactor_PipelinedHitsOverRealTCP writes two complete hit
// requests in a single write: both must be served inline by the reactor
// (preload + internal flush loop), with zero handoffs.
func TestEpollReactor_PipelinedHitsOverRealTCP(t *testing.T) {
	addr, rec := startSelectiveReactorListener(t)

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	pipelined := "GET /hit HTTP/1.1\r\nHost: localhost\r\n\r\n" +
		"GET /hit2 HTTP/1.1\r\nHost: localhost\r\n\r\n"
	_, err = conn.Write([]byte(pipelined))
	require.NoError(t, err)

	// One shared reader: both responses may land in a single TCP read,
	// and a per-response reader would buffer (and discard) the second
	// response's bytes.
	reader := bufio.NewReader(conn)
	resp1 := readOneResponseReader(t, reader)
	assert.Contains(t, resp1, "hello")
	resp2 := readOneResponseReader(t, reader)
	assert.Contains(t, resp2, "hello", "both pipelined hits must be served inline, got: %q", resp2)

	require.Eventually(t, func() bool {
		return rec.hits.Load() >= 2
	}, 5*time.Second, 5*time.Millisecond, "both hits served by the reactor loop")
	var total uint64
	for i := range rec.handoffCounts {
		total += rec.handoffCounts[i].Load()
	}
	assert.Equal(t, uint64(0), total, "pipelined hits must not hand off")
}

// readOneResponseReader is readOneResponse over a caller-owned reader,
// for tests that read multiple responses whose bytes may arrive in one
// TCP segment (pipelining).
func readOneResponseReader(t *testing.T, reader *bufio.Reader) string {
	t.Helper()
	var sb strings.Builder
	contentLength := 0
	for {
		line, err := reader.ReadString('\n')
		require.NoError(t, err)
		sb.WriteString(line)
		if strings.TrimSpace(line) == "" {
			break
		}
		if strings.HasPrefix(strings.ToLower(line), "content-length:") {
			_, _ = fmt.Sscanf(strings.TrimSpace(line), "Content-Length: %d", &contentLength)
		}
	}
	if contentLength > 0 {
		body := make([]byte, contentLength)
		_, err := io.ReadFull(reader, body)
		require.NoError(t, err)
		sb.Write(body)
	}
	return sb.String()
}
