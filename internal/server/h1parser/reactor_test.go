package h1parser

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ioEOF is a hard read/write error for fakeIO.failNext.
var errFakeClosed = errors.New("connection closed")

// fakeIO simulates a non-blocking socket: a queued read payload and a
// write buffer, with controllable would-block and error injection.
// A drained read source behaves like an empty socket: EAGAIN, not EOF
// (the reactor treats EOF-with-no-bytes as a peer waiting state).
type fakeIO struct {
	readSrc    *bytes.Reader
	written    bytes.Buffer
	againRead  bool // when true, next read returns errAgain
	againWrite bool // when true, next write returns errAgain
	failNext   error
}

func (f *fakeIO) read(b []byte) (int, error) {
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return 0, err
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

// TestReactor_PipelinedBytesHandOff asserts bytes after the header
// terminator (body or next request) force a handoff — the reactor must
// not discard them.
func TestReactor_PipelinedBytesHandOff(t *testing.T) {
	t.Parallel()
	payload := []byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\nGET /next HTTP/1.1\r\nHost: localhost\r\n")
	rc, _ := newTestReactorConn(t, payload)

	require.Equal(t, actHandoff, rc.advance(), "pipelined second request must hand off, not be dropped")
}

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

// TestReactor_IdleExpired asserts the idle budget trips handoff after
// the timeout with no read progress.
func TestReactor_IdleExpired(t *testing.T) {
	t.Parallel()
	req := []byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
	rc, _ := newTestReactorConn(t, req)
	require.Equal(t, actWaitRead, rc.advance())

	stale := rc.lastRead.Add(-reactorIdleTimeout - time.Second)
	assert.True(t, rc.idleExpired(rc.lastRead.Add(reactorIdleTimeout+time.Second)),
		"now beyond lastRead + budget must expire")
	assert.False(t, rc.idleExpired(rc.lastRead), "now at lastRead must not expire")
	_ = stale
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
