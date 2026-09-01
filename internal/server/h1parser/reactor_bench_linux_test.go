//go:build linux

package h1parser

// reactor_bench_linux_test.go — gating benchmarks for the Linux epoll
// transport's per-event costs: the dispatch path (fd lookup, idle
// check, mod elision, action switch) over fake connections with no
// syscalls. Part of make bench-gate via the BUDGETS platform branch
// in bench/run.sh.

import (
	"net"
	"strconv"
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
	fp := &staticFastPath{}
	p.fastPath = fp
	loop, ok := NewReactorLoop(p, ln)
	if !ok {
		b.Fatal("epoll reactor unavailable")
	}
	go loop.Run()
	defer loop.Close()

	body := "e2e-body"
	resp := &api.FastPathResponse{
		BuffersArr: [3][]byte{
			[]byte("HTTP/1.1 200 OK\r\n"),
			[]byte("Content-Length: " + strconv.Itoa(len(body)) + "\r\nContent-Type: text/plain\r\n\r\n"),
			[]byte(body),
		},
	}
	resp.Buffers = resp.BuffersArr[:3]
	fp.resp = resp

	const samples = 128
	start := time.Now()
	buf := make([]byte, 512)
	for range samples {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			b.Fatalf("dial: %v", err)
		}
		if _, err := conn.Write(benchHitReq); err != nil {
			b.Fatalf("write: %v", err)
		}
		got := 0
		for got < len(body) {
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

// coarseNowBench is a cheap clock for the E2E loop.
func coarseNowBench() time.Time { return time.Now() }
