//go:build linux

package h1parser

import (
	"net"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

// benchMissHandler serves the miss response with a fixed body.
func benchMissHandler(ctx *fasthttp.RequestCtx) {
	ctx.SetBodyString("miss-body")
	ctx.SetStatusCode(200)
}

// missRoundTripConn drives one miss round trip without a socket: the
// request bytes are the handoff prefix (how a real miss arrives), the
// response writes are absorbed, and a second read signals the client
// hangup that ends Serve. Deterministic, no goroutines, no timers —
// the gate measures the machinery (pool dispatch, Serve miss cycle,
// return-hook reuse, recycle), not kernel delivery.
type missRoundTripConn struct {
	prefix []byte
	reads  int
}

func (m *missRoundTripConn) Read(b []byte) (int, error) {
	m.reads++
	if m.reads == 1 {
		n := copy(b, m.prefix)
		m.prefix = m.prefix[n:]
		return n, nil
	}
	// The keep-alive read after the served request: EOF ends Serve.
	return 0, net.ErrClosed
}

func (m *missRoundTripConn) Write(b []byte) (int, error) { return len(b), nil }
func (m *missRoundTripConn) Close() error                { return nil }
func (m *missRoundTripConn) LocalAddr() net.Addr         { return nil }
func (m *missRoundTripConn) RemoteAddr() net.Addr        { return nil }
func (m *missRoundTripConn) SetDeadline(time.Time) error { return nil }
func (m *missRoundTripConn) SetReadDeadline(time.Time) error {
	return nil
}
func (m *missRoundTripConn) SetWriteDeadline(time.Time) error { return nil }

// BenchmarkGate_Reactor_MissRoundTrip measures the full miss cycle
// cost and its allocation contract: a handed-off connection serves a
// miss via the blocking parser, is offered back to the reactor
// (return-to-reactor, ADR-0042), the conn declines (simulating the
// pending-queue full / shutdown arms), Serve keeps serving until the
// client hangs up, and the worker recycles the rc into
// reactorConnPool. Before the W2 pool-and-reuse work every round trip
// paid a fresh ~20 KiB reactorConn, a fresh ~10 KiB fasthttp
// RequestCtx (escape analysis heaps the stack literal), a 16 KiB
// bufio.Reader, the rebuilt head, and a goroutine spawn — ~55 MB/s of
// churn at 1.2k misses/s. The gate pins the machinery's own
// allocations to the small set fasthttp's parser still owns.
func BenchmarkGate_Reactor_MissRoundTrip(b *testing.B) {
	p := New(nil, benchMissHandler, WithScheme("http"))

	tracker := &handoffTracker{}
	tracker.startSpawner()
	defer tracker.stopSpawner()

	// The return hook mirrors returnFromBlocking's decline arm (no fd
	// on this conn): the rc stays with the worker and is recycled at
	// conn death — both arms of the real hook's allocation contract.
	p.reactorReturn = func(_ net.Conn, _ *reactorConn) bool { return false }

	reqBytes := []byte("GET /miss HTTP/1.1\r\nHost: localhost\r\n\r\n")

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		m := &missRoundTripConn{prefix: reqBytes}
		rc := newReactorConn(m, p, nil, nil)
		tracker.serveJob(handoffJob{p: p, conn: rc.handoffConn()})
	}
}

// BenchmarkGate_FallThrough_Pooled measures the fall-through's pooled
// allocation contract on its own: pooled RequestCtx + rebuilt head into
// the pooled buffer + pooled bufio reader + owned leftover copy. Before
// pooling this paid a fresh RequestCtx (~10 KiB), a bufio.Reader
// (~16 KiB), and a head per miss; the gate allows only the small set
// fasthttp's header parser still allocates per request.
func BenchmarkGate_FallThrough_Pooled(b *testing.B) {
	p := New(nil, benchMissHandler, WithScheme("http"))

	follower := "GET /next HTTP/1.1\r\nHost: localhost\r\n\r\n"
	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/miss",
		HTTPVersion: "HTTP/1.1",
		Host:        "localhost",
		NHeaders:    0,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		closeConn, leftover, err := p.handleFallThrough(&discardConn{}, req, []byte(follower))
		if err != nil {
			b.Fatalf("fall-through: %v", err)
		}
		if closeConn || string(leftover) != follower {
			b.Fatalf("leftover contract: close=%v leftover=%q", closeConn, leftover)
		}
	}
}

// discardConn absorbs fall-through writes without I/O.
type discardConn struct {
	net.Conn
}

func (d *discardConn) Read(b []byte) (int, error)       { return 0, errAgain }
func (d *discardConn) Write(b []byte) (int, error)      { return len(b), nil }
func (d *discardConn) SetDeadline(time.Time) error      { return nil }
func (d *discardConn) SetReadDeadline(time.Time) error  { return nil }
func (d *discardConn) SetWriteDeadline(time.Time) error { return nil }
