package h1parser

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
)

// errFakeClosed is a hard read/write error for fakeIO.failNext.
var errFakeClosed = errors.New("connection closed")

// fakeIO simulates a non-blocking socket: a queued read payload and a
// write buffer, with controllable would-block, EOF, and error
// injection. A drained read source behaves like an empty socket
// (EAGAIN); an explicit eofNext returns the raw-fd EOF contract
// (0, nil), which the state machine treats as close.
type fakeIO struct {
	readSrc    *bytes.Reader
	written    bytes.Buffer
	againRead  bool // when true, next read returns errAgain
	againWrite bool // when true, next write returns errAgain
	failNext   error
	eofNext    bool // when true, next read returns (0, nil) — raw-fd EOF
}

func (f *fakeIO) read(b []byte) (int, error) {
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return 0, err
	}
	if f.eofNext {
		f.eofNext = false
		return 0, nil
	}
	if f.againRead {
		f.againRead = false
		return 0, errAgain
	}
	if f.readSrc == nil || f.readSrc.Len() == 0 {
		return 0, errAgain
	}
	return f.readSrc.Read(b)
}

func (f *fakeIO) write(b []byte) (int, error) {
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return 0, err
	}
	if f.againWrite {
		f.againWrite = false
		return 0, errAgain
	}
	return f.written.Write(b)
}

// newTestReactorConn builds a reactorConn over fakeIO with a
// mockFastPathHit parser (the shared options_test.go mock).
func newTestReactorConn(t *testing.T, readPayload []byte) (*reactorConn, *fakeIO) {
	t.Helper()
	p := New(&mockFastPathHit{}, noopHandler, WithScheme("http"))
	fio := &fakeIO{readSrc: bytes.NewReader(readPayload)}
	conn := &mockIOConn{fio: fio}
	rc := newReactorConn(conn, p, fio.read, fio.write)
	t.Cleanup(rc.release)
	return rc, fio
}

// TestReactor_HitServedInline drives the full state machine for one
// request over one connection: read → parse → hit → write, asserting
// the exact response bytes and the transition back to reading.
func TestReactor_HitServedInline(t *testing.T) {
	t.Parallel()
	req := []byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
	rc, fio := newTestReactorConn(t, req)

	require.Equal(t, actWaitRead, rc.advance(), "hit response flushed inline; back to reading")
	require.Equal(t, rcReading, rc.state)
	resp := fio.written.Bytes()
	assert.True(t, bytes.HasPrefix(resp, []byte("HTTP/1.1 200 OK\r\n")),
		"response must be the serialized hit, got: %q", resp)
	assert.True(t, bytes.HasSuffix(resp, []byte("hello")), "body must follow the headers")
	assert.NotContains(t, resp, "\x00", "no partial-buffer garbage")
}

// TestReactor_KeepAliveSecondHit serves two sequential hits on the
// same connection: after the first response the machine must be ready
// to parse the second request from freshly buffered bytes.
func TestReactor_KeepAliveSecondHit(t *testing.T) {
	t.Parallel()
	req := []byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
	rc, fio := newTestReactorConn(t, req)

	require.Equal(t, actWaitRead, rc.advance())
	firstLen := fio.written.Len()

	fio.readSrc = bytes.NewReader(req)
	require.Equal(t, actWaitRead, rc.advance())
	assert.Greater(t, fio.written.Len(), firstLen, "second hit must be served")
}

// TestReactor_PartialReadsWouldBlock feeds the request in two chunks
// with an errAgain between them: the machine must wait for read
// readiness and then complete.
func TestReactor_PartialReadsWouldBlock(t *testing.T) {
	t.Parallel()
	full := []byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
	rc, fio := newTestReactorConn(t, full)
	fio.readSrc = bytes.NewReader(full[:10]) // only the first chunk available

	require.Equal(t, actWaitRead, rc.advance(), "incomplete header block must want read")

	fio.readSrc = bytes.NewReader(full[10:])
	require.Equal(t, actWaitRead, rc.advance(), "second chunk completes the request and serves the hit")
	assert.True(t, bytes.HasPrefix(fio.written.Bytes(), []byte("HTTP/1.1 200 OK\r\n")))
}

// TestReactor_MissHandsOff asserts a fast-path miss (mock returns
// nil,false) hands off to the blocking parser with the buffered bytes
// replayed.
func TestReactor_MissHandsOff(t *testing.T) {
	t.Parallel()
	p := New(&mockFastPathHandler{}, noopHandler, WithScheme("http")) // never hits
	fio := &fakeIO{readSrc: bytes.NewReader([]byte("GET /miss HTTP/1.1\r\nHost: localhost\r\n\r\n"))}
	rc := newReactorConn(&mockIOConn{fio: fio}, p, fio.read, fio.write)
	t.Cleanup(rc.release)

	require.Equal(t, actHandoff, rc.advance())
	require.Equal(t, rcHandoff, rc.state)
}

// TestReactor_PipelinedBytesAfterHit asserts bytes pipelined after a
// hit request's header block are served, not discarded and not forced
// through a handoff: the hit flushes inline, the partial next request
// stays buffered, and its bytes are consumed once the rest arrives.
func TestReactor_PipelinedBytesAfterHit(t *testing.T) {
	t.Parallel()
	first := "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
	second := "GET /next HTTP/1.1\r\nHost: localhost\r\n\r\n"
	rc, fio := newTestReactorConn(t, []byte(first+second[:20]))

	require.Equal(t, actWaitRead, rc.advance(),
		"hit must flush inline; the partial pipelined request must wait for the rest")
	resp := fio.written.Bytes()
	assert.True(t, bytes.HasPrefix(resp, []byte("HTTP/1.1 200 OK\r\n")),
		"first response must be the serialized hit, got: %q", resp)
	assert.Len(t, bytes.Split(resp, []byte("HTTP/1.1 200 OK\r\n")), 2,
		"exactly one response so far")

	fio.readSrc = bytes.NewReader([]byte(second[20:]))
	require.Equal(t, actWaitRead, rc.advance(), "second hit must be served inline")
	assert.Len(t, bytes.Split(fio.written.Bytes(), []byte("HTTP/1.1 200 OK\r\n")), 3,
		"both hits served on the reactor, no handoff")
	assert.Equal(t, rcReading, rc.state)
}

// TestReactor_PipelinedHitsServedInOnePass feeds two complete pipelined
// hit requests in a single read: one advance() call must serve both —
// the flush of the first transitions internally (actFlushed) and the
// preloaded second request is parsed before any socket read.
func TestReactor_PipelinedHitsServedInOnePass(t *testing.T) {
	t.Parallel()
	first := "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
	second := "GET /next HTTP/1.1\r\nHost: localhost\r\n\r\n"
	rc, fio := newTestReactorConn(t, []byte(first+second))

	require.Equal(t, actWaitRead, rc.advance())
	assert.Len(t, bytes.Split(fio.written.Bytes(), []byte("HTTP/1.1 200 OK\r\n")), 3,
		"both pipelined hits served inline by one advance")
	assert.Equal(t, rcReading, rc.state)
}

// TestReactor_PipelinedMissHandsOffWithReplay asserts a complete
// pipelined miss after a hit: the hit is served inline, then the miss
// hands off with the pipelined request's bytes in the replay prefix.
func TestReactor_PipelinedMissHandsOffWithReplay(t *testing.T) {
	t.Parallel()
	first := "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
	second := "GET /miss HTTP/1.1\r\nHost: localhost\r\n\r\n"
	p := New(&mockSelectiveFastPath{}, noopHandler, WithScheme("http"))
	fio := &fakeIO{readSrc: bytes.NewReader([]byte(first + second))}
	rc := newReactorConn(&mockIOConn{fio: fio}, p, fio.read, fio.write)
	t.Cleanup(rc.release)

	require.Equal(t, actHandoff, rc.advance(), "pipelined miss must hand off")
	require.Equal(t, rcHandoff, rc.state)
	assert.True(t, bytes.HasPrefix(fio.written.Bytes(), []byte("HTTP/1.1 200 OK\r\n")),
		"the first (hit) response must still have been served inline")
	assert.Equal(t, second, string(rc.handoffConn().(*prefixConn).prefix),
		"the replay prefix must hold the pipelined miss request, not the served hit")
}

// TestReactor_PipelinedConnectionCloseDiscards asserts a hit whose
// response ends with Connection: close drops the connection after the
// flush even when pipelined bytes followed (RFC 9110 §9.6: the client
// promised no further requests).
func TestReactor_PipelinedConnectionCloseDiscards(t *testing.T) {
	t.Parallel()
	first := "GET /close HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
	second := "GET /next HTTP/1.1\r\nHost: localhost\r\n\r\n"
	rc, fio := newTestReactorConn(t, []byte(first+second))
	rc.parser = New(&mockConnCloseFastPathHit{}, noopHandler, WithScheme("http"))

	require.Equal(t, actCloseAfterFlush, rc.advance())
	assert.Len(t, bytes.Split(fio.written.Bytes(), []byte("HTTP/1.1 200 OK\r\n")), 2,
		"exactly one response; the pipelined bytes are discarded with the close")
}

// TestReactor_PipelinedFillThenOversizeHandsOff asserts the oversize
// path still works when the buffer fills across a preloaded pipelined
// batch: the hit is served, the partial next request waits, and once
// reads fill the buffer without a terminator the connection hands off
// instead of misreading an empty raw-fd read slice as EOF.
func TestReactor_PipelinedFillThenOversizeHandsOff(t *testing.T) {
	t.Parallel()
	first := "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
	payload := append([]byte(first), bytes.Repeat([]byte("X"), readBufferSize-len(first))...)
	rc, fio := newTestReactorConn(t, payload)

	require.Equal(t, actWaitRead, rc.advance(),
		"hit served inline; the unterminated pipelined bytes wait for more")
	assert.True(t, bytes.HasPrefix(fio.written.Bytes(), []byte("HTTP/1.1 200 OK\r\n")))

	fio.readSrc = bytes.NewReader(bytes.Repeat([]byte("X"), 64))
	require.Equal(t, actHandoff, rc.advance(),
		"buffer full without a second terminator must hand off (oversize), not close")
}

// TestReactor_TelemetryCounts verifies the reactor lifecycle counters:
// inline hits and handoff reasons are reported to the injected
// api.ReactorMetrics sink.
func TestReactor_TelemetryCounts(t *testing.T) {
	t.Parallel()
	rec := &fakeReactorMetrics{}
	p := New(&mockSelectiveFastPath{}, noopHandler, WithScheme("http"), WithReactorMetrics(rec))
	fio := &fakeIO{readSrc: bytes.NewReader(nil)}
	rc := newReactorConn(&mockIOConn{fio: fio}, p, fio.read, fio.write)
	t.Cleanup(rc.release)

	// Hit request, then a miss request, pipelined in one read.
	hit := "GET /hit HTTP/1.1\r\nHost: localhost\r\n\r\n"
	miss := "GET /miss HTTP/1.1\r\nHost: localhost\r\n\r\n"
	fio.readSrc = bytes.NewReader([]byte(hit + miss))
	require.Equal(t, actHandoff, rc.advance())

	assert.Equal(t, uint64(1), rec.hits, "one inline hit reported")
	assert.Equal(t, uint64(1), rec.handoffs[api.ReactorHandoffMiss], "one miss handoff reported")
	assert.Empty(t, rec.connsRegistered, "registration is the transport's job, not the state machine's")
}

// mockConnCloseFastPathHit always serves a Connection: close hit.
type mockConnCloseFastPathHit struct{}

func (m *mockConnCloseFastPathHit) TryHit(_ *api.RawRequest, _ time.Time) (*api.FastPathResponse, bool) {
	resp := &api.FastPathResponse{
		BuffersArr: [3][]byte{
			[]byte("HTTP/1.1 200 OK\r\n"),
			[]byte("Content-Length: 5\r\nConnection: close\r\n\r\n"),
			[]byte("hello"),
		},
		CloseConn: true,
	}
	resp.Buffers = resp.BuffersArr[:]
	return resp, true
}

func (m *mockConnCloseFastPathHit) Release(_ *api.FastPathResponse) {}

// mockSelectiveFastPath hits every path except "/miss".
type mockSelectiveFastPath struct{}

func (m *mockSelectiveFastPath) TryHit(req *api.RawRequest, _ time.Time) (*api.FastPathResponse, bool) {
	if req.Path == "/miss" {
		return nil, false
	}
	resp := &api.FastPathResponse{
		BuffersArr: [3][]byte{
			[]byte("HTTP/1.1 200 OK\r\n"),
			[]byte("Content-Length: 5\r\n\r\n"),
			[]byte("hello"),
		},
	}
	resp.Buffers = resp.BuffersArr[:]
	return resp, true
}

func (m *mockSelectiveFastPath) Release(_ *api.FastPathResponse) {}

// fakeReactorMetrics records api.ReactorMetrics calls for assertions.
type fakeReactorMetrics struct {
	hits            uint64
	handoffs        map[string]uint64
	returns         uint64
	connsRegistered uint64
	drops           uint64
}

func (f *fakeReactorMetrics) IncrementReactorConnRegistered() { f.connsRegistered++ }
func (f *fakeReactorMetrics) IncrementReactorHit()            { f.hits++ }
func (f *fakeReactorMetrics) IncrementReactorHandoff(reason string) {
	if f.handoffs == nil {
		f.handoffs = make(map[string]uint64)
	}
	f.handoffs[reason]++
}
func (f *fakeReactorMetrics) IncrementReactorReturn() { f.returns++ }
func (f *fakeReactorMetrics) IncrementReactorDrop()   { f.drops++ }

// TestReactor_SmugglingHandOff asserts CL+TE smuggling hands off to
// the blocking path (which writes the 400 and closes).
func TestReactor_SmugglingHandOff(t *testing.T) {
	t.Parallel()
	payload := []byte("POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n")
	rc, _ := newTestReactorConn(t, payload)

	require.Equal(t, actHandoff, rc.advance())
}

// TestReactor_OversizeHeadersHandOff asserts a header block filling
// the 16 KiB read buffer without a terminator hands off (the blocking
// path serves it via handleFallThroughRaw).
func TestReactor_OversizeHeadersHandOff(t *testing.T) {
	t.Parallel()
	head := []byte("GET /big HTTP/1.1\r\nHost: localhost\r\nX-Fill: ")
	payload := append(append([]byte{}, head...), bytes.Repeat([]byte("a"), readBufferSize)...)
	rc, _ := newTestReactorConn(t, payload)

	require.Equal(t, actHandoff, rc.advance())
}

// TestReactor_PartialWriteWouldBlock forces EAGAIN mid-response: the
// machine must return actWaitWrite, keep the unsent bytes, and
// complete them on the next advance.
func TestReactor_PartialWriteWouldBlock(t *testing.T) {
	t.Parallel()
	req := []byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
	rc, fio := newTestReactorConn(t, req)

	// First advance parses + tries hit + starts writing; the write
	// hits would-block after 0 bytes.
	fio.againWrite = true
	require.Equal(t, actWaitWrite, rc.advance())
	require.Equal(t, rcWriting, rc.state)
	sent := fio.written.Len()
	require.Equal(t, 0, sent)

	// Readiness arrives; the remainder flushes.
	require.Equal(t, actWaitRead, rc.advance())
	require.Equal(t, rcReading, rc.state)
	assert.True(t, bytes.HasSuffix(fio.written.Bytes(), []byte("hello")))
}

// TestReactor_WriteErrorCloses asserts a hard write error closes the
// connection instead of hanging in rcWriting.
func TestReactor_WriteErrorCloses(t *testing.T) {
	t.Parallel()
	req := []byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
	rc, fio := newTestReactorConn(t, req)
	fio.failNext = errFakeClosed

	require.Equal(t, actClose, rc.advance())
}

// TestReactor_ReadErrorCloses asserts a hard read error (not EAGAIN)
// closes the connection.
func TestReactor_ReadErrorCloses(t *testing.T) {
	t.Parallel()
	rc, fio := newTestReactorConn(t, nil)
	fio.failNext = errFakeClosed

	require.Equal(t, actClose, rc.advance())
}

// TestReactor_IdleExpired asserts the per-request idle budget: the
// clock runs from when the request began, not from the last byte —
// a dribbling client must not reset it.
func TestReactor_IdleExpired(t *testing.T) {
	t.Parallel()
	req := []byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
	rc, _ := newTestReactorConn(t, req)
	require.Equal(t, actWaitRead, rc.advance())

	assert.True(t, rc.idleExpired(rc.reqStart.Add(reactorIdleTimeout+time.Second)),
		"now beyond reqStart + budget must expire")
	assert.False(t, rc.idleExpired(rc.reqStart), "now at reqStart must not expire")
}

// TestReactor_IdleDribbleDoesNotReset asserts a slowloris-style client
// dribbling bytes cannot extend its window: idleExpired is measured
// from reqStart, so bytes arriving after the budget started do not
// refresh it. The sweep's dispatch check would drop the connection.
func TestReactor_IdleDribbleDoesNotReset(t *testing.T) {
	t.Parallel()
	// First half of the request arrives fresh; the machine wants more.
	partial := []byte("GET / HTTP/1.1\r\nHost: loc")
	rc, fio := newTestReactorConn(t, partial)
	require.Equal(t, actWaitRead, rc.advance())

	// Backdate the request start past the budget; more bytes arrive
	// (the dribble). The connection must still read as expired.
	rc.reqStart = rc.reqStart.Add(-reactorIdleTimeout - time.Second)
	fio.readSrc = bytes.NewReader([]byte("alhost\r\n\r\n"))
	assert.True(t, rc.idleExpired(rc.parser.nowFunc()),
		"dribbled bytes must not reset the idle window")
}

// TestReactor_HandoffPrefixReplay asserts the handoff conn replays the
// buffered bytes: reading from it yields exactly readBuf[:rLen] first.
func TestReactor_HandoffPrefixReplay(t *testing.T) {
	t.Parallel()
	payload := []byte("GET /miss HTTP/1.1\r\nHost: localhost\r\n\r\n")
	rc, _ := newTestReactorConn(t, payload)
	// Buffer the request bytes without consuming them via advance:
	rc.rLen = copy(rc.readBuf[:], payload)

	conn := rc.handoffConn()
	buf := make([]byte, len(payload))
	n, err := conn.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
	assert.Equal(t, payload, buf)
}

// mockIOConn makes fakeIO usable where a net.Conn is required but
// never actually used (state machine only touches readFn/writeFn).
type mockIOConn struct{ fio *fakeIO }

func (m *mockIOConn) Read(b []byte) (int, error)       { return m.fio.read(b) }
func (m *mockIOConn) Write(b []byte) (int, error)      { return m.fio.write(b) }
func (m *mockIOConn) Close() error                     { return nil }
func (m *mockIOConn) LocalAddr() net.Addr              { return mockAddr{} }
func (m *mockIOConn) RemoteAddr() net.Addr             { return mockAddr{} }
func (m *mockIOConn) SetDeadline(time.Time) error      { return nil }
func (m *mockIOConn) SetReadDeadline(time.Time) error  { return nil }
func (m *mockIOConn) SetWriteDeadline(time.Time) error { return nil }

// TestReactor_ZeroCopyWritevRetainsRelease asserts the writev flush
// path: the response buffers are retained (not copied), flushed as one
// iov, and the fast-path Release is called only after the flush.
func TestReactor_ZeroCopyWritevRetainsRelease(t *testing.T) {
	t.Parallel()
	p := New(&mockFastPathHit{}, noopHandler, WithScheme("http"))
	fio := &fakeIO{readSrc: bytes.NewReader([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))}
	rc := newReactorConn(&mockIOConn{fio: fio}, p, fio.read, fio.write)
	t.Cleanup(rc.release)

	released := false
	rc.retainResp = &api.FastPathResponse{}
	rc.retainFP = &releaseSpyFP{onRelease: func() { released = true }}

	var iovSeen [][]byte
	rc.writeVecFn = func(iovs [][]byte) (int, error) {
		// Concatenate like the kernel would; no copy of the response.
		iovSeen = iovs
		total := 0
		for _, b := range iovs {
			total += len(b)
		}
		return total, nil
	}

	rc.state = rcWriting
	rc.writeVec = [][]byte{[]byte("HTTP/1.1 200 OK\r\n"), []byte("Content-Length: 5\r\n\r\n"), []byte("hello")}
	require.Equal(t, actWaitRead, rc.advance())
	assert.True(t, released, "Release must fire after the flush completes")
	require.Len(t, iovSeen, 3, "all three buffers flushed as one iov, zero-copy")
}

// TestReactor_EofClosesNotSpins asserts the raw-fd read contract: a
// zero-byte read with no error means peer EOF — the machine must close
// the connection, never return want-read (which would busy-spin the
// loop on level-triggered epoll).
func TestReactor_EofClosesNotSpins(t *testing.T) {
	t.Parallel()
	p := New(&mockFastPathHit{}, noopHandler, WithScheme("http"))
	fio := &fakeIO{} // no read source: read returns (0, errAgain)? No — drained => EAGAIN per fakeIO
	rc := newReactorConn(&mockIOConn{fio: fio}, p, fio.read, fio.write)
	t.Cleanup(rc.release)

	// A real EOF per the raw-fd contract: (0, nil).
	fio.readSrc = nil
	fio.eofNext = true
	require.Equal(t, actClose, rc.advance(), "EOF (0-byte read, no error) must close, not want-read")
}

// TestReactor_WritevPartialFlush asserts a partial writev is resumed
// from the exact intra-buffer offset: the second flush must start with
// the unwritten bytes, and Release fires only after completion.
func TestReactor_WritevPartialFlush(t *testing.T) {
	t.Parallel()
	p := New(&mockFastPathHit{}, noopHandler, WithScheme("http"))
	fio := &fakeIO{}
	rc := newReactorConn(&mockIOConn{fio: fio}, p, fio.read, fio.write)
	t.Cleanup(rc.release)

	released := false
	rc.retainResp = &api.FastPathResponse{}
	rc.retainFP = &releaseSpyFP{onRelease: func() { released = true }}

	var written bytes.Buffer
	calls := 0
	rc.writeVecFn = func(iovs [][]byte) (int, error) {
		calls++
		if calls == 1 {
			// First call: write only 10 bytes.
			n := 0
			for _, b := range iovs {
				take := min(len(b), 10-n)
				written.Write(b[:take])
				n += take
				if n == 10 {
					break
				}
			}
			return n, nil
		}
		if calls == 2 {
			// Immediate retry in the same advance: socket full.
			return 0, errAgain
		}
		// Write-readiness arrived: flush the remainder byte-exactly.
		n := 0
		for _, b := range iovs {
			written.Write(b)
			n += len(b)
		}
		return n, nil
	}
	rc.state = rcWriting
	rc.writeVec = [][]byte{[]byte("HTTP/1.1 200 OK\r\n"), []byte("hello")}

	require.Equal(t, actWaitWrite, rc.advance(), "partial writev must re-arm write readiness")
	assert.False(t, released, "no release mid-flush")
	require.Equal(t, actWaitRead, rc.advance(), "resume must complete the flush")
	assert.True(t, released, "release after full flush")
	assert.Equal(t, "HTTP/1.1 200 OK\r\nhello", written.String(), "byte-exact continuation")
}

// releaseSpyFP is a fast-path stub whose Release fires a callback.
type releaseSpyFP struct {
	onRelease func()
}

func (f *releaseSpyFP) TryHit(_ *api.RawRequest, _ time.Time) (*api.FastPathResponse, bool) {
	return nil, false
}

func (f *releaseSpyFP) Release(_ *api.FastPathResponse) {
	if f.onRelease != nil {
		f.onRelease()
	}
}

// TestReactor_ConnectionCloseAfterFlush asserts a hit whose request
// carried Connection: close fully flushes and then signals the
// transport to close (actCloseAfterFlush) instead of returning to
// read readiness — the state-machine half of RFC 9110 §9.6.
func TestReactor_ConnectionCloseAfterFlush(t *testing.T) {
	t.Parallel()
	req := []byte("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	p := New(&mockFastPathHit{}, noopHandler, WithScheme("http"))
	fio := &fakeIO{readSrc: bytes.NewReader(req)}
	rc := newReactorConn(&mockIOConn{fio: fio}, p, fio.read, fio.write)
	t.Cleanup(rc.release)

	require.Equal(t, actCloseAfterFlush, rc.advance(),
		"close-request hit must finish the flush, then close")
	resp := fio.written.Bytes()
	assert.True(t, bytes.HasPrefix(resp, []byte("HTTP/1.1 200 OK\r\n")),
		"response must be fully flushed before close")
	assert.Contains(t, string(resp), "hello", "body must be flushed")
}
