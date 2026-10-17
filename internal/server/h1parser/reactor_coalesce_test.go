package h1parser

import (
	"bytes"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
)

// batchFastPath serves distinct-path hits, tracking Release calls so
// the coalescing tests can assert the retain contract.
type batchFastPath struct {
	resp     api.FastPathResponse
	released int
}

func (f *batchFastPath) TryHit(req *api.RawRequest, _ time.Time) (*api.FastPathResponse, bool) {
	f.resp.BuffersArr = [3][]byte{
		[]byte("HTTP/1.1 200 OK\r\n"),
		[]byte("Content-Length: 5\r\nX-Path: " + req.Path + "\r\n\r\n"),
		[]byte("hello"),
	}
	f.resp.Buffers = f.resp.BuffersArr[:3]
	return &f.resp, true
}

func (f *batchFastPath) Release(_ *api.FastPathResponse) { f.released++ }

// countingFastPath hits /hit and misses /miss, counting both.
type countingFastPath struct {
	hits     atomic.Int32
	misses   atomic.Int32
	released atomic.Int32
}

func (f *countingFastPath) TryHit(req *api.RawRequest, _ time.Time) (*api.FastPathResponse, bool) {
	if req.Path == "/miss" {
		f.misses.Add(1)
		return nil, false
	}
	f.hits.Add(1)
	body := "hello"
	resp := &api.FastPathResponse{
		BuffersArr: [3][]byte{
			[]byte("HTTP/1.1 200 OK\r\n"),
			[]byte("Content-Length: 5\r\nX-Path: " + req.Path + "\r\n\r\n"),
			[]byte(body),
		},
	}
	resp.Buffers = resp.BuffersArr[:3]
	return resp, true
}

func (f *countingFastPath) Release(_ *api.FastPathResponse) { f.released.Add(1) }

// closeEchoFastPath echoes the path and honors Connection: close on
// paths carrying the -close suffix.
type closeEchoFastPath struct {
	released atomic.Int32
}

func (f *closeEchoFastPath) TryHit(req *api.RawRequest, _ time.Time) (*api.FastPathResponse, bool) {
	closeConn := len(req.Path) > 6 && req.Path[len(req.Path)-6:] == "-close"
	head := "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-Path: " + req.Path + "\r\n"
	if closeConn {
		head += "Connection: close\r\n"
	}
	resp := &api.FastPathResponse{
		BuffersArr: [3][]byte{
			[]byte(head),
			[]byte("\r\n"),
			[]byte("hello"),
		},
		CloseConn: closeConn,
	}
	resp.Buffers = resp.BuffersArr[:3]
	return resp, true
}

func (f *closeEchoFastPath) Release(_ *api.FastPathResponse) { f.released.Add(1) }

// TestReactor_CoalescedBatchFlushesAsOneWritev pins the W4 core: a
// pipelined batch of hits is flushed as ONE writev call carrying every
// response, with every retained response released exactly once at
// completion. Before coalescing, each hit paid its own writev syscall.
func TestReactor_CoalescedBatchFlushesAsOneWritev(t *testing.T) {
	t.Parallel()
	p := New(nil, noopHandler, WithScheme("http"))
	fp := &batchFastPath{}
	p.fastPath = fp

	// Three complete pipelined requests in one read.
	fio := &fakeIO{readSrc: bytes.NewReader([]byte(
		"GET /one HTTP/1.1\r\nHost: localhost\r\n\r\n" +
			"GET /two HTTP/1.1\r\nHost: localhost\r\n\r\n" +
			"GET /three HTTP/1.1\r\nHost: localhost\r\n\r\n"))}
	rc := newReactorConn(&mockIOConn{fio: fio}, p, fio.read, fio.write)
	t.Cleanup(rc.release)

	var writevCalls int
	rc.writeVecFn = func(iovs [][]byte) (int, error) {
		writevCalls++
		fio.written.Reset()
		total := 0
		for _, b := range iovs {
			total += len(b)
			fio.written.Write(b)
		}
		return total, nil
	}

	require.Equal(t, actWaitRead, rc.advance(), "the whole batch serves in one advance")

	assert.Equal(t, 1, writevCalls, "a 3-hit pipelined batch must flush as ONE writev")
	assert.Equal(t, 3, fp.released, "every coalesced response released exactly once")
	out := fio.written.Bytes()
	assert.Contains(t, string(out), "X-Path: /one")
	assert.Contains(t, string(out), "X-Path: /two")
	assert.Contains(t, string(out), "X-Path: /three",
		"responses must land on the wire in request order")
	assert.Equal(t, rcReading, rc.state)
	assert.Zero(t, rc.retainCount)
}

// TestReactor_CoalescedBatchFlushesBeforeMissFollower pins the
// data-loss guard: when a pipelined follower needs the blocking path
// (miss/disqualified), the already-served hit responses in the batch
// MUST reach the socket before the handoff — advance intercepts the
// handoff and flushes first, then hands off with the follower's bytes.
func TestReactor_CoalescedBatchFlushesBeforeMissFollower(t *testing.T) {
	t.Parallel()
	p := New(nil, noopHandler, WithScheme("http"))
	fp := &countingFastPath{} // hits /hit, misses /miss
	p.fastPath = fp

	fio := &fakeIO{readSrc: bytes.NewReader([]byte(
		"GET /hit HTTP/1.1\r\nHost: localhost\r\n\r\n" +
			"GET /hit HTTP/1.1\r\nHost: localhost\r\n\r\n" +
			"GET /miss HTTP/1.1\r\nHost: localhost\r\n\r\n"))}
	rc := newReactorConn(&mockIOConn{fio: fio}, p, fio.read, fio.write)
	t.Cleanup(rc.release)

	var writevCalls int
	rc.writeVecFn = func(iovs [][]byte) (int, error) {
		writevCalls++
		total := 0
		for _, b := range iovs {
			total += len(b)
			fio.written.Write(b)
		}
		return total, nil
	}

	act := rc.advance()
	require.Equal(t, actHandoff, act, "the miss follower hands off")
	require.Equal(t, rcHandoff, rc.state)

	// Both hit responses reached the socket BEFORE the handoff; the
	// handoff prefix holds the miss's bytes (one flush for the batch).
	out := fio.written.String()
	assert.Equal(t, int32(2), fp.hits.Load(), "two hits served inline")
	assert.Contains(t, out, "hello")
	assert.Equal(t, bytes.Count(fio.written.Bytes(), []byte("hello")), 2,
		"both hit responses flushed before the miss handoff")
	assert.Equal(t, int32(2), fp.released.Load(), "both batch responses released")
	assert.Equal(t, 1, writevCalls, "the batch flushed as one writev despite the miss follower")
}

// TestReactor_CoalescedCloseFollowerFlushesBatch pins the ordering
// when a Connection: close hit follows coalesced hits: the batch
// flushes first, then the close response, then the conn closes —
// no bytes stranded in either phase.
func TestReactor_CoalescedCloseFollowerFlushesBatch(t *testing.T) {
	t.Parallel()
	p := New(nil, noopHandler, WithScheme("http"))
	fp := &closeEchoFastPath{} // closes on the close-prefixed path
	p.fastPath = fp

	fio := &fakeIO{readSrc: bytes.NewReader([]byte(
		"GET /hit HTTP/1.1\r\nHost: localhost\r\n\r\n" +
			"GET /hit-close HTTP/1.1\r\nHost: localhost\r\n\r\n"))}
	rc := newReactorConn(&mockIOConn{fio: fio}, p, fio.read, fio.write)
	t.Cleanup(rc.release)

	var writevCalls int
	rc.writeVecFn = func(iovs [][]byte) (int, error) {
		writevCalls++
		total := 0
		for _, b := range iovs {
			total += len(b)
			fio.written.Write(b)
		}
		return total, nil
	}

	act := rc.advance()
	require.Equal(t, actCloseAfterFlush, act, "the close response terminates the conn")

	out := fio.written.String()
	assert.Contains(t, out, "X-Path: /hit\r\n", "the batch hit flushed")
	assert.Contains(t, out, "X-Path: /hit-close", "the close response flushed after it")
	assert.Contains(t, out, "Connection: close", "the close header reached the wire")
	assert.Equal(t, int32(2), fp.released.Load(), "batch and close responses both released")
}

// TestReactor_CoalescedBatchCapFlushesAtMax pins the batch capacity:
// the sixth pipelined hit in one advance cannot join a full batch —
// the batch flushes at capacity and the remaining hits coalesce into a
// fresh batch.
func TestReactor_CoalescedBatchCapFlushesAtMax(t *testing.T) {
	t.Parallel()
	p := New(nil, noopHandler, WithScheme("http"))
	fp := &batchFastPath{}
	p.fastPath = fp

	// 7 pipelined hits: 5 fill one batch, flush; 2 coalesce in the next.
	src := bytes.NewBuffer(nil)
	for i := range 7 {
		src.WriteString("GET /n" + string(rune('a'+i)) + " HTTP/1.1\r\nHost: localhost\r\n\r\n")
	}
	fio := &fakeIO{readSrc: bytes.NewReader(src.Bytes())}
	rc := newReactorConn(&mockIOConn{fio: fio}, p, fio.read, fio.write)
	t.Cleanup(rc.release)

	var writevCalls, hits int
	rc.writeVecFn = func(iovs [][]byte) (int, error) {
		writevCalls++
		total := 0
		for _, b := range iovs {
			total += len(b)
			fio.written.Write(b)
		}
		return total, nil
	}

	require.Equal(t, actWaitRead, rc.advance())
	hits = 7

	out := fio.written.String()
	for i := range hits {
		assert.Contains(t, out, "X-Path: /n"+string(rune('a'+i)),
			"hit %d flushed in order", i)
	}
	assert.Equal(t, 2, writevCalls, "7 hits flush as a capped batch of 5 + a batch of 2")
	assert.Equal(t, 7, fp.released, "every response released exactly once")
}
