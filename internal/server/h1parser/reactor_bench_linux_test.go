//go:build linux

package h1parser

// reactor_bench_linux_test.go — gating benchmarks for the Linux epoll
// transport's per-event costs: the dispatch path (fd lookup, idle
// check, mod elision, action switch) over fake connections with no
// syscalls. Part of make bench-gate via the BUDGETS platform branch
// in bench/run.sh.

import (
	"fmt"
	"net"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"

	"golang.org/x/sys/unix"
)

// BenchmarkGate_Reactor_Dispatch measures the transport's per-event
// dispatch cost: the connection-table lookup, the per-dispatch idle
// check, the state-machine advance over a full hit, and the mod()
// epoll_ctl elision on the return to read interest. The connections
// are fake (no real fds): epoll_ctl is never invoked — the gate is
// the CPU and allocation contract of the dispatch machinery itself.
func BenchmarkGate_Reactor_Dispatch(b *testing.B) {
	p := New(nil, noopHandler, WithScheme("http"))
	r := &reactorEpoll{
		p:       p,
		pending: make(chan *reactorConn, 8),
		done:    make(chan struct{}),
		epfd:    -1, // invalid on purpose: register/mod are never reached
		wakeR:   -1,
		wakeW:   -1,
	}

	const n = 64
	rcs := make([]*reactorConn, n)
	events := make([]unix.EpollEvent, n)
	for i := range n {
		fio := &fakeIO{}
		rc := newReactorConn(&mockIOConn{fio: fio}, p, fio.read, fio.write)
		defer rc.release() //nolint:staticcheck // deferred in loop is fine for a benchmark
		rc.fd = i
		rc.epollInterest = unix.EPOLLIN | unix.EPOLLRDHUP
		// Wire the I/O once, outside the timed loop: the gate is the
		// dispatch machinery's own allocation contract, not closure
		// construction. readFn hands back a full request per call; the
		// writev sink discards by total length (no socket involved).
		rc.parser.fastPath = &hitFastPath{}
		rc.writeVecFn = func(iovs [][]byte) (int, error) {
			total := 0
			for _, bb := range iovs {
				total += len(bb)
			}
			return total, nil
		}
		rc.readFn = func(dst []byte) (int, error) {
			return copy(dst, benchHitReq), nil
		}
		rcs[i] = rc
		r.connAdd(int32(i), rc)
		events[i] = unix.EpollEvent{Events: unix.EPOLLIN | unix.EPOLLRDHUP, Fd: int32(i)}
	}

	now := p.nowFunc()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		// One epoll_wait batch worth of events, each a full hit cycle
		// through advance (read→parse→hit→flush→mod-elision).
		for i := range events {
			rc := rcs[i]
			rc.state = rcReading
			rc.rLen = 0
			rc.scanned = 0
			rc.reqStart = now
			r.dispatch(rc, events[i].Events, now)
		}
	}
}

// benchHitReq is the request fed to each dispatched connection: a
// minimal GET whose header block completes in one read.
var benchHitReq = []byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")

// benchRespLen returns the FULL serialized length of the fixed RTT-arm
// response for body: status line + Content-Length/Content-Type header
// block + body. The client read loops must consume exactly this many
// bytes per cycle — counting only the body (the old accounting) let a
// single Read swallow a partial response chunk (the status line alone)
// and exit early, stranding the rest of the response in the socket; the
// next cycle's parse then read stale response bytes as a request and
// the arm wedged until the idle deadline or died on EOF. Every arm and
// the echo control build exactly this response shape.
func benchRespLen(body string) int {
	return len("HTTP/1.1 200 OK\r\n") +
		len("Content-Length: "+strconv.Itoa(len(body))+"\r\n") +
		len("Content-Type: text/plain\r\n\r\n") +
		len(body)
}

// freshFastPath mirrors the production fast path's per-hit response
// lifecycle: TryHit returns a FRESH response on every call. This is
// load-bearing for the blocking arm — serveHit flushes via
// net.Buffers.WriteTo, which DRAINS the response's buffer slices, so
// a shared response (staticFastPath) serves empty bodies from the
// second hit on and the arm wedges until the idle deadline. The
// reactor never drains (writev copies slice headers), but both sides
// of every RTT pair use this same stub so the pair's delta is purely
// the serving model.
type freshFastPath struct {
	body string
}

func (f *freshFastPath) TryHit(_ *api.RawRequest, _ time.Time) (*api.FastPathResponse, bool) {
	resp := &api.FastPathResponse{
		BuffersArr: [3][]byte{
			[]byte("HTTP/1.1 200 OK\r\n"),
			[]byte("Content-Length: " + strconv.Itoa(len(f.body)) + "\r\nContent-Type: text/plain\r\n\r\n"),
			[]byte(f.body),
		},
	}
	resp.Buffers = resp.BuffersArr[:3]
	return resp, true
}

func (f *freshFastPath) Release(_ *api.FastPathResponse) {}

// BenchmarkSingle_Reactor_EpollE2E is a single-shot end-to-end latency
// measurement of the full reactor loop over a real TCP listener:
// accept → epoll register → hit flush, one connection per sample. Use
// -benchtime=1x -count=10 for benchstat samples. It self-skips under
// time-driven benchtime per the BenchmarkSingle_* convention (the
// second iteration would measure an already-registered conn).
func BenchmarkSingle_Reactor_EpollE2E(b *testing.B) {
	first := true
	b.ReportAllocs()
	for b.Loop() {
		if !first {
			b.Skip("single-shot benchmark: use -benchtime=1x -count=10")
		}
		first = false
		startReactorBenchListener(b)
	}
}

// startReactorBenchListener boots one connection cycle against a real
// listener and reports the per-connection wall time as a metric.
func startReactorBenchListener(b *testing.B) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	p := New(nil, noopHandler, WithScheme("http"))
	body := "e2e-body"
	p.fastPath = &freshFastPath{body: body}
	loop, ok := NewReactorLoop(p, ln)
	if !ok {
		b.Fatal("epoll reactor unavailable")
	}
	go loop.Run()
	defer loop.Close()

	const samples = 128
	start := time.Now()
	buf := make([]byte, 512)
	respFull := benchRespLen(body)
	for range samples {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			b.Fatalf("dial: %v", err)
		}
		if _, err := conn.Write(benchHitReq); err != nil {
			b.Fatalf("write: %v", err)
		}
		got := 0
		for got < respFull {
			n, rerr := conn.Read(buf)
			got += n
			if rerr != nil {
				b.Fatalf("read: %v", rerr)
			}
		}
		_ = conn.Close()
	}
	elapsed := time.Since(start)
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(samples), "conn-ns")
}

// BenchmarkSingle_Reactor_KeepAliveRTT measures the user-standpoint
// latency of the hit path: request write → response read, round-tripped
// over one established keep-alive connection through the real epoll
// loop. This is the per-request floor a client feels on every
// subsequent request (the loop-CPU gates measure server-side cost
// only; the RTT adds kernel delivery, epoll wakeup, and both
// goroutine/syscall crossings). Single-shot: -benchtime=1x -count=10.
func BenchmarkSingle_Reactor_KeepAliveRTT(b *testing.B) {
	first := true
	for b.Loop() {
		if !first {
			b.Skip("single-shot benchmark: use -benchtime=1x -count=10")
		}
		first = false
		measureReactorRTT(b, false, benchHitReq)
	}
}

// BenchmarkSingle_Reactor_KeepAliveRTT_Headers8 is the same
// user-standpoint RTT measurement on a production-shaped request head
// (long request line + the 8 canonical browser/proxy headers, ~500 B):
// the shape where per-request parse cost is real. Paired with
// BenchmarkSingle_BlockingServeRTTControl_Headers8 — both arms use the
// identical freshFastPath stub, so the delta is purely transport,
// scheduling, and parse machinery. Single-shot: -benchtime=1x
// -count=10.
func BenchmarkSingle_Reactor_KeepAliveRTT_Headers8(b *testing.B) {
	first := true
	for b.Loop() {
		if !first {
			b.Skip("single-shot benchmark: use -benchtime=1x -count=10")
		}
		first = false
		measureReactorRTT(b, false, benchRealisticHead(8))
	}
}

// BenchmarkSingle_Reactor_ConnectRTT is the same measurement for the
// fresh-connection path a user feels on connect+request (curl, health
// checks, any client that opens a connection per request): TCP connect,
// accept, registration, and the first hit — including the pending-queue
// hop from the accept goroutine to the loop.
func BenchmarkSingle_Reactor_ConnectRTT(b *testing.B) {
	first := true
	for b.Loop() {
		if !first {
			b.Skip("single-shot benchmark: use -benchtime=1x -count=10")
		}
		first = false
		measureReactorRTT(b, true, benchHitReq)
	}
}

// measureReactorRTT drives N request/response cycles through a real
// listener and reports per-cycle RTT plus p50/p99 as metrics. freshConns
// re-dials per cycle (connect path); otherwise one keep-alive conn. req
// is the request bytes written each cycle.
func measureReactorRTT(b *testing.B, freshConns bool, req []byte) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	p := New(nil, noopHandler, WithScheme("http"))
	body := "rtt-body"
	p.fastPath = &freshFastPath{body: body}
	loop, ok := NewReactorLoop(p, ln)
	if !ok {
		b.Fatal("epoll reactor unavailable")
	}
	go loop.Run()
	defer loop.Close()

	respFull := benchRespLen(body)

	const samples = 2000
	buf := make([]byte, 512)
	rtts := make([]time.Duration, 0, samples)

	var conn net.Conn
	if !freshConns {
		conn, err = net.Dial("tcp", ln.Addr().String())
		if err != nil {
			b.Fatalf("dial: %v", err)
		}
		defer func() { _ = conn.Close() }()
		// Warm: one full cycle so the first sample does not pay
		// accept/registration.
		_, _ = conn.Write(req)
		got := 0
		for got < respFull {
			n, rerr := conn.Read(buf)
			got += n
			if rerr != nil {
				b.Fatalf("warm read: %v", rerr)
			}
		}
	}

	for range samples {
		if freshConns {
			conn, err = net.Dial("tcp", ln.Addr().String())
			if err != nil {
				b.Fatalf("dial: %v", err)
			}
		}
		start := time.Now()
		if _, err := conn.Write(req); err != nil {
			b.Fatalf("write: %v", err)
		}
		got := 0
		for got < respFull {
			n, rerr := conn.Read(buf)
			got += n
			if rerr != nil {
				b.Fatalf("read: %v", rerr)
			}
		}
		rtts = append(rtts, time.Since(start))
		if freshConns {
			_ = conn.Close()
		}
	}

	slices.Sort(rtts)
	p50 := rtts[len(rtts)/2]
	p99 := rtts[len(rtts)*99/100]
	b.ReportMetric(float64(rtts[0].Nanoseconds()), "min-ns")
	b.ReportMetric(float64(p50.Nanoseconds()), "p50-ns")
	b.ReportMetric(float64(p99.Nanoseconds()), "p99-ns")
}

// BenchmarkSingle_EchoRTTControl is the control arm for the RTT
// benchmarks: the same request/response ping-pong over a plain
// blocking goroutine-per-connection echo server, no reactor, no
// epoll, no fast path. Its p50 is the host's loopback RTT floor
// (kernel + scheduler); the delta between this floor and
// Reactor_KeepAliveRTT's p50 is what the reactor machinery actually
// costs the user. Single-shot: -benchtime=1x -count=10.
func BenchmarkSingle_EchoRTTControl(b *testing.B) {
	first := true
	for b.Loop() {
		if !first {
			b.Skip("single-shot benchmark: use -benchtime=1x -count=10")
		}
		first = false
		measureEchoRTT(b)
	}
}

// measureEchoRTT is measureReactorRTT's twin over a blocking echo
// server: one goroutine per connection reading the request and
// writing a fixed response, the minimal possible Go TCP server.
func measureEchoRTT(b *testing.B) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				buf := make([]byte, 512)
				resp := []byte("HTTP/1.1 200 OK\r\nContent-Length: 8\r\nContent-Type: text/plain\r\n\r\nrtt-body")
				for {
					n, rerr := c.Read(buf)
					if rerr != nil {
						return
					}
					if _, werr := c.Write(resp); werr != nil {
						return
					}
					_ = n
				}
			}(conn)
		}
	}()

	respFull := benchRespLen("rtt-body")
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	const samples = 2000
	buf := make([]byte, 512)
	rtts := make([]time.Duration, 0, samples)
	for range samples {
		start := time.Now()
		if _, err := conn.Write(benchHitReq); err != nil {
			b.Fatalf("write: %v", err)
		}
		got := 0
		for got < respFull {
			n, rerr := conn.Read(buf)
			got += n
			if rerr != nil {
				b.Fatalf("read: %v", rerr)
			}
		}
		rtts = append(rtts, time.Since(start))
	}

	slices.Sort(rtts)
	b.ReportMetric(float64(rtts[0].Nanoseconds()), "min-ns")
	b.ReportMetric(float64(rtts[len(rtts)/2].Nanoseconds()), "p50-ns")
	b.ReportMetric(float64(rtts[len(rtts)*99/100].Nanoseconds()), "p99-ns")
}

// BenchmarkSingle_BlockingServeRTTControl is the second control arm:
// the same fast-path hit served by the blocking Parser (goroutine per
// connection, runtime netpoller park/wake) instead of the reactor loop.
// Reactor_KeepAliveRTT minus this number is the reactor's true
// scheduling-path cost to the user — the thing ADR-0041 traded
// park/unpark per request for wakeup-per-batch.
func BenchmarkSingle_BlockingServeRTTControl(b *testing.B) {
	first := true
	for b.Loop() {
		if !first {
			b.Skip("single-shot benchmark: use -benchtime=1x -count=10")
		}
		first = false
		measureBlockingServeRTT(b, benchHitReq)
	}
}

// BenchmarkSingle_BlockingServeRTTControl_Headers8 is the twin of
// BenchmarkSingle_Reactor_KeepAliveRTT_Headers8: the same
// production-shaped head, the same freshFastPath stub, the same
// response bytes — served by the blocking Parser instead of the
// reactor loop. The pair's delta is the reactor's net effect on the
// hit RTT at real-world request sizes. Single-shot: -benchtime=1x
// -count=10.
func BenchmarkSingle_BlockingServeRTTControl_Headers8(b *testing.B) {
	first := true
	for b.Loop() {
		if !first {
			b.Skip("single-shot benchmark: use -benchtime=1x -count=10")
		}
		first = false
		measureBlockingServeRTT(b, benchRealisticHead(8))
	}
}

// measureBlockingServeRTT is measureReactorRTT's twin over Parser.Serve:
// the same freshFastPath stub, the same "rtt-body" response bytes, and the
// same full-response read accounting as the reactor arm, so the pair's
// delta is purely the serving model (goroutine-per-connection park/wake
// vs the epoll loop).
func measureBlockingServeRTT(b *testing.B, req []byte) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	p := New(nil, noopHandler, WithScheme("http"))
	body := "rtt-body"
	p.fastPath = &freshFastPath{body: body}
	respFull := benchRespLen(body)
	go func() {
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				_ = p.Serve(c)
			}(conn)
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	const samples = 2000
	buf := make([]byte, 512)
	rtts := make([]time.Duration, 0, samples)
	for range samples {
		start := time.Now()
		if _, err := conn.Write(req); err != nil {
			b.Fatalf("write: %v", err)
		}
		got := 0
		for got < respFull {
			n, rerr := conn.Read(buf)
			got += n
			if rerr != nil {
				b.Fatalf("read: %v", rerr)
			}
		}
		rtts = append(rtts, time.Since(start))
	}

	slices.Sort(rtts)
	b.ReportMetric(float64(rtts[0].Nanoseconds()), "min-ns")
	b.ReportMetric(float64(rtts[len(rtts)/2].Nanoseconds()), "p50-ns")
	b.ReportMetric(float64(rtts[len(rtts)*99/100].Nanoseconds()), "p99-ns")
}

// BenchmarkSingle_Reactor_ConcurrencySweep measures keep-alive RTT
// against the reactor at 1, 4, 16 concurrent clients. A single loop
// amortizes its epoll_wait wake across the batch, so per-request RTT
// should fall as concurrency rises (each batch serves many requests
// for one wake) — the test that separates batch-amortized scheduling
// cost from fixed per-request cost.
func BenchmarkSingle_Reactor_ConcurrencySweep(b *testing.B) {
	for _, clients := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("clients-%d", clients), func(b *testing.B) {
			first := true
			for b.Loop() {
				if !first {
					b.Skip("single-shot benchmark: use -benchtime=1x -count=10")
				}
				first = false
				measureReactorRTTConcurrent(b, clients)
			}
		})
	}
}

// measureReactorRTTConcurrent is the multi-client variant of
// measureReactorRTT: N keep-alive clients each drive sequential
// request/response cycles; per-request p50/p99 across all clients is
// reported.
func measureReactorRTTConcurrent(b *testing.B, clients int) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	p := New(nil, noopHandler, WithScheme("http"))
	body := "rtt-body"
	p.fastPath = &freshFastPath{body: body}
	loop, ok := NewReactorLoop(p, ln)
	if !ok {
		b.Fatal("epoll reactor unavailable")
	}
	go loop.Run()
	defer loop.Close()

	respFull := benchRespLen(body)

	const perClient = 2000
	results := make(chan []time.Duration, clients)
	var wg sync.WaitGroup
	for range clients {
		conn, derr := net.Dial("tcp", ln.Addr().String())
		if derr != nil {
			b.Fatalf("dial: %v", derr)
		}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer func() { _ = c.Close() }()
			buf := make([]byte, 512)
			rtts := make([]time.Duration, 0, perClient)
			// Warm cycle.
			_, _ = c.Write(benchHitReq)
			got := 0
			for got < respFull {
				n, _ := c.Read(buf)
				got += n
			}
			for range perClient {
				start := time.Now()
				if _, werr := c.Write(benchHitReq); werr != nil {
					return
				}
				got := 0
				for got < respFull {
					n, rerr := c.Read(buf)
					if rerr != nil {
						return
					}
					got += n
				}
				rtts = append(rtts, time.Since(start))
			}
			results <- rtts
		}(conn)
	}
	wg.Wait()
	close(results)
	var all []time.Duration
	for r := range results {
		all = append(all, r...)
	}
	slices.Sort(all)
	b.ReportMetric(float64(all[len(all)/2].Nanoseconds()), "p50-ns")
	b.ReportMetric(float64(all[len(all)*99/100].Nanoseconds()), "p99-ns")
}

// BenchmarkSingle_BlockingServe_ConcurrencySweep is the blocking-path
// twin of BenchmarkSingle_Reactor_ConcurrencySweep: the same N
// keep-alive clients driving sequential request/response cycles, served
// by goroutine-per-connection Parser.Serve with the same freshFastPath
// stub and request bytes. Comparing the two sweeps at the same client
// count is the reactor-vs-blocking answer under load: closed-loop RPS
// is clients divided by per-request RTT, so whichever path's p50/p99 is
// lower serves more requests. Single-shot: -benchtime=1x -count=10.
func BenchmarkSingle_BlockingServe_ConcurrencySweep(b *testing.B) {
	for _, clients := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("clients-%d", clients), func(b *testing.B) {
			first := true
			for b.Loop() {
				if !first {
					b.Skip("single-shot benchmark: use -benchtime=1x -count=10")
				}
				first = false
				measureBlockingServeRTTConcurrent(b, clients)
			}
		})
	}
}

// measureBlockingServeRTTConcurrent is measureReactorRTTConcurrent's
// twin over Parser.Serve (goroutine per connection, runtime netpoller
// park/wake — the scheduling model ADR-0041 replaced with the reactor).
func measureBlockingServeRTTConcurrent(b *testing.B, clients int) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	p := New(nil, noopHandler, WithScheme("http"))
	body := "rtt-body"
	p.fastPath = &freshFastPath{body: body}
	respFull := benchRespLen(body)
	go func() {
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				_ = p.Serve(c)
			}(conn)
		}
	}()

	const perClient = 2000
	results := make(chan []time.Duration, clients)
	var wg sync.WaitGroup
	for range clients {
		conn, derr := net.Dial("tcp", ln.Addr().String())
		if derr != nil {
			b.Fatalf("dial: %v", derr)
		}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer func() { _ = c.Close() }()
			buf := make([]byte, 512)
			rtts := make([]time.Duration, 0, perClient)
			// Warm cycle.
			_, _ = c.Write(benchHitReq)
			got := 0
			for got < respFull {
				n, _ := c.Read(buf)
				got += n
			}
			for range perClient {
				start := time.Now()
				if _, werr := c.Write(benchHitReq); werr != nil {
					return
				}
				got := 0
				for got < respFull {
					n, rerr := c.Read(buf)
					if rerr != nil {
						return
					}
					got += n
				}
				rtts = append(rtts, time.Since(start))
			}
			results <- rtts
		}(conn)
	}
	wg.Wait()
	close(results)
	var all []time.Duration
	for r := range results {
		all = append(all, r...)
	}
	slices.Sort(all)
	b.ReportMetric(float64(all[len(all)/2].Nanoseconds()), "p50-ns")
	b.ReportMetric(float64(all[len(all)*99/100].Nanoseconds()), "p99-ns")
}
