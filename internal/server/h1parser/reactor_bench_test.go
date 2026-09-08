package h1parser

import (
	"testing"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
)

// sliceReader is a zero-allocation resettable byte source for the
// reactor benchmarks: it resets per iteration without constructing a
// new bytes.Reader (which would dominate the profile).
type sliceReader struct {
	buf []byte
	off int
}

// hitFastPath is a minimal FastPathHandler that returns a fixed
// pre-serialized hit response (pool-free to keep the benchmark loop
// allocation-free; the production handler's pooling is measured by
// BenchmarkGate_FastPath_Hit in internal/cache).
type hitFastPath struct {
	resp api.FastPathResponse
}

func (f *hitFastPath) TryHit(_ *api.RawRequest, _ time.Time) (*api.FastPathResponse, bool) {
	return &f.resp, true
}

func (f *hitFastPath) Release(_ *api.FastPathResponse) {}

// BenchmarkGate_Reactor_Hit measures the reactor's per-request core
// cost: parse from buffered bytes, TryHit, serialize into the pooled
// write buffer, flush through the injected I/O. This is the batch
// serving primitive that replaces goroutine park/unpark per request
// (see docs/decisions/0041-h1-epoll-reactor.md) — the per-request
// cost must stay allocation-free.
func BenchmarkGate_Reactor_Hit(b *testing.B) {
	p := New(nil, noopHandler, WithScheme("http"))
	fp := &hitFastPath{}
	p.fastPath = fp

	reqBytes := []byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
	fio := &fakeIO{}
	rc := newReactorConn(&mockIOConn{fio: fio}, p, fio.read, fio.write)
	defer rc.release()

	// Pre-fill one response in the fast path.
	fp.resp = api.FastPathResponse{
		BuffersArr: [3][]byte{
			[]byte("HTTP/1.1 200 OK\r\n"),
			[]byte("Content-Length: 5\r\nContent-Type: text/plain\r\n\r\n"),
			[]byte("hello"),
		},
	}
	fp.resp.Buffers = fp.resp.BuffersArr[:3]

	// sliceReader drives the loop from a resettable byte source.
	src := &sliceReader{}
	rc.readFn = func(b2 []byte) (int, error) {
		if src.off >= len(src.buf) {
			return 0, errAgain
		}
		n := copy(b2, src.buf[src.off:])
		src.off += n
		return n, nil
	}

	// Wire the writev path exactly like the Linux transport: the
	// response flush is zero-copy over the retained buffers.
	rc.writeVecFn = func(iovs [][]byte) (int, error) {
		total := 0
		for _, b := range iovs {
			total += len(b)
		}
		return total, nil
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		// Simulate the socket delivering a full request: reset the
		// source, advance the machine through parse→hit→writev-flush.
		src.buf = reqBytes
		src.off = 0
		rc.rLen = 0
		if act := rc.advance(); act != actWaitRead {
			b.Fatalf("advance = %d, want %d (flush completes inline)", act, actWaitRead)
		}
	}
}

// BenchmarkGate_Reactor_Hit_Metrics measures the reactor hit core with
// production's metrics wiring: the metrics ring (W3), not a direct
// hook call — the ring push is what the loop goroutine actually pays
// per hit, and this is the allocation contract of that path (0
// allocs). The drainer goroutine is not part of the gate; the hook
// cost behind it is profiled live.
func BenchmarkGate_Reactor_Hit_Metrics(b *testing.B) {
	p := New(nil, noopHandler, WithScheme("http"),
		WithMetricsHook(func(pool, cacheResult, source string, status, bytesOut int, duration time.Duration) {
			_ = pool + cacheResult + source
		}))
	p.metricsRing = &metricsRing{}
	fp := &hitFastPath{}
	p.fastPath = fp

	reqBytes := []byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
	fio := &fakeIO{}
	rc := newReactorConn(&mockIOConn{fio: fio}, p, fio.read, fio.write)
	defer rc.release()

	fp.resp = api.FastPathResponse{
		BuffersArr: [3][]byte{
			[]byte("HTTP/1.1 200 OK\r\n"),
			[]byte("Content-Length: 5\r\nContent-Type: text/plain\r\n\r\n"),
			[]byte("hello"),
		},
	}
	fp.resp.Buffers = fp.resp.BuffersArr[:3]

	src := &sliceReader{}
	rc.readFn = func(b2 []byte) (int, error) {
		if src.off >= len(src.buf) {
			return 0, errAgain
		}
		n := copy(b2, src.buf[src.off:])
		src.off += n
		return n, nil
	}
	rc.writeVecFn = func(iovs [][]byte) (int, error) {
		total := 0
		for _, bb := range iovs {
			total += len(bb)
		}
		return total, nil
	}

	b.ReportAllocs()
	b.ResetTimer()
	// The ring holds 2048 slots; without periodic draining the benchmark
	// measures the overflow path (every push drops) instead of steady
	// state. Draining every 1024 iterations amortizes the consumer the
	// same way production's 20ms drainer goroutine does.
	var drainBuf [512]hitMetricsRecord
	var n int
	for b.Loop() {
		src.buf = reqBytes
		src.off = 0
		rc.rLen = 0
		if act := rc.advance(); act != actWaitRead {
			b.Fatalf("advance = %d, want %d", act, actWaitRead)
		}
		if n++; n&1023 == 0 {
			for p.metricsRing.drain(drainBuf[:]) == len(drainBuf) {
			}
		}
	}
}
