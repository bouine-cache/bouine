// Package h1parser implements a zero-allocation HTTP/1.1 request parser
// for the cache hit fast path. It parses request lines and headers from
// a net.Conn into a stack-allocated RawRequest struct, avoiding the
// *http.Request allocation that net/http imposes on every request.
//
// The parser handles keep-alive in a loop: parse → try fast path →
// serve or fall through. On fall-through (miss path), the parser
// constructs a *fasthttp.RequestCtx from the parsed request and calls
// the fallback fasthttp.RequestHandler directly — no byte
// reconstruction or net/http handoff needed.
//
// Unstable.
package h1parser

// reactor.go — the portable per-connection state machine for the
// single-goroutine hit-path event loop (see reactor_epoll_linux.go for
// the Linux transport). One reactor goroutine multiplexes many
// connections: it parses requests from bytes already buffered, serves
// cache hits inline, and tracks partial writes — with no per-request
// goroutine park/unpark and one readiness wakeup per batch instead of
// one per request. That batching is the structural advantage nginx's
// worker event loop holds over a goroutine-per-connection server (see
// docs/plans/hit-path-p99-optimization.md).
//
// Anything that is not a plain cache hit — miss, conditional, range,
// pipelined body bytes, oversize headers, malformed input — hands off
// to a per-connection goroutine running the existing blocking Parser
// with the buffered bytes replayed through a prefixConn, so every
// correctness behavior (fall-through framing, smuggling 400, SWR) is
// shared with the blocking path. Handoff only happens before any
// response byte is written; a partially-written hit is completed by
// the reactor itself via write readiness.

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
)

// rcState is the per-connection state machine state.
type rcState uint8

const (
	rcReading rcState = iota // accumulating bytes toward a full request
	rcWriting                // flushing a serialized hit response
	rcHandoff                // left the reactor; blocking path owns it
)

// rcAction tells the transport what to do after one advance step.
type rcAction uint8

const (
	actWaitRead  rcAction = iota // re-arm read readiness
	actWaitWrite                 // re-arm write readiness
	actHandoff                   // start blocking parser with buffered bytes
	actClose                     // close the connection
)

// errAgain is returned by the injected I/O functions when the socket
// has no data (read) or no buffer space (write) right now. The Linux
// transport maps EAGAIN/EWOULDBLOCK to it.
var errAgain = errors.New("reactor: would block")

// reactorConn is the per-connection state machine. Exactly one
// reactor goroutine owns it; after handoff nobody may touch it.
//
// The I/O is injected (read/write funcs over the raw fd, never
// net.Conn.Read/Write — those park the goroutine on EAGAIN via the
// Go runtime poller, which is exactly what the reactor exists to
// avoid).
// reactorConn is the per-connection state machine. Exactly one
// reactor goroutine owns it; after handoff nobody may touch it.
//
// The I/O is injected (read/write funcs over the raw fd, never
// net.Conn.Read/Write — those park the goroutine on EAGAIN via the
// Go runtime poller, which is exactly what the reactor exists to
// avoid).
//
// interleave the value-typed bulk (16 KiB readBuf, ~4 KiB scratch) with
// the hot scalar fields to save 40 bytes of padding on a struct that is
// allocated once per connection; keeping the hot state in the leading
// cache lines is the deliberate choice here.
//
//nolint:govet // fieldalignment: the tool's "optimal" ordering would
type reactorConn struct {
	parser   *Parser
	conn     net.Conn
	readFn   func([]byte) (int, error)
	writeFn  func([]byte) (int, error)
	fd       int // raw fd, cached at accept; -1 when unknown
	writeBuf []byte
	// writeVec holds the fast-path response's net.Buffers slices for
	// zero-copy writev flushing (writeBuf alone is used for the small
	// header-serialized case and for tests via writeFn).
	writeVec [][]byte
	// writeVecOffs tracks the per-slice flushed offset when writev
	// completes partially.
	writeVecOffs []int
	// retainResp/retainFP hold the fast-path response and its owning
	// handler between parsed() and the completed flush: writev aliases
	// the cache object body, so the response must not return to the
	// pool until every byte is on the socket. The handler (not a
	// method value — assigning fp.Release boxes the receiver and costs
	// an allocation per hit) is stored to call Release at completion.
	retainResp *api.FastPathResponse
	retainFP   api.FastPathHandler
	// writeVecFn, when non-nil, flushes the retained response buffers
	// zero-copy (the Linux transport wires unix.Writev). Nil in tests
	// and on non-Linux — the state machine then uses the writeBuf +
	// writeFn path.
	writeVecFn func([][]byte) (int, error)
	state      rcState
	rLen       int
	writeLen   int
	// epollInterest is the currently-armed readiness mask (0 = read,
	// i.e. the registration default). mod() skips the epoll_ctl when
	// the interest is unchanged — one syscall per hit saved in the
	// common full-flush case.
	epollInterest uint32
	lastRead      time.Time
	scratch       api.RawRequest

	// readBuf sits last: the 16 KiB inline array dominates the struct;
	// the bulk value fields trail the pointer and scalar fields so the
	// hot state stays in the first cache lines.
	readBuf [readBufferSize]byte
}

// newReactorConn builds the state machine. The transport wires the
// raw-fd I/O functions; they must return errAgain instead of blocking.
func newReactorConn(conn net.Conn, p *Parser, readFn, writeFn func([]byte) (int, error)) *reactorConn {
	rc := &reactorConn{
		conn:     conn,
		parser:   p,
		readFn:   readFn,
		writeFn:  writeFn,
		fd:       -1,
		state:    rcReading,
		lastRead: p.nowFunc(),
		writeBuf: (*reactorWritePool.Get().(*[]byte))[:0],
		scratch:  api.RawRequest{Scheme: p.scheme},
	}
	// Pre-size the writev backing array (3 slots: status line, header
	// block, body — the FastPathResponse layout) so parsed() never
	// grows it mid-hit.
	rc.writeVec = make([][]byte, 0, 3)
	rc.writeVecOffs = make([]int, 0, 3)
	return rc
}

// release returns the pooled write buffer. Called exactly once, when
// the connection leaves the reactor (close or handoff). Nothing may
// touch rc.writeBuf after the Put — the pool may hand the buffer to
// another connection's newReactorConn immediately on another goroutine
// (release runs on the loop goroutine, accept on the accept
// goroutine), so the pointer is nulled before Put, never after.
func (rc *reactorConn) release() {
	buf := rc.writeBuf
	rc.writeBuf = nil
	if buf != nil && cap(buf) <= reactorWriteCap {
		buf = buf[:0]
		reactorWritePool.Put(&buf)
	}
}

// advance drives the state machine one step. It never blocks: all
// reads/writes go through the injected non-blocking funcs.
func (rc *reactorConn) advance() rcAction {
	switch rc.state {
	case rcReading:
		act := rc.advanceReading()
		if act == actHandoff {
			// Terminal for the reactor: mark before the transport reads
			// state (tests and idle checks rely on it).
			rc.state = rcHandoff
		}
		return act
	case rcWriting:
		act := rc.advanceWriting()
		if act == actHandoff {
			rc.state = rcHandoff
		}
		return act
	default:
		return actClose
	}
}

// advanceReading drains socket bytes into readBuf; once a full header
// block is buffered it parses, tries the fast path, and on a hit
// serializes the response into writeBuf for flushing.
// advanceReading drains socket bytes into readBuf; once a full header
// block is buffered it parses, tries the fast path, and on a hit
// serializes the response into writeBuf for flushing.
func (rc *reactorConn) advanceReading() rcAction {
	for {
		n, err := rc.readFn(rc.readBuf[rc.rLen:])
		rc.rLen += n
		if n > 0 {
			rc.lastRead = rc.parser.nowFunc()
			if idx := findHeaderEnd(rc.readBuf[:rc.rLen]); idx >= 0 {
				return rc.parsed(idx)
			}
			if rc.rLen >= len(rc.readBuf) {
				// Headers exceed the 16 KiB buffer: the blocking path
				// serves these via handleFallThroughRaw.
				return actHandoff
			}
			continue
		}
		if err != nil {
			if errors.Is(err, errAgain) {
				return actWaitRead
			}
			return actClose
		}
		// n == 0, err == nil: the raw-fd read contract reports peer EOF
		// this way. Treating it as want-read would busy-spin the loop
		// on level-triggered readiness; close instead.
		return actClose
	}
}

// parsed handles a complete header block at idx: parse, qualify, and
// either serve the hit inline or hand off. Split from advanceReading
// to stay under the complexity/funlen gates.
// parsed handles a complete header block at idx: parse, qualify, and
// either serve the hit inline or hand off. Split from advanceReading
// to stay under the complexity/funlen gates.
func (rc *reactorConn) parsed(idx int) rcAction {
	req, fallThrough, excess, err := rc.parser.parseBuffer(
		rc.readBuf[:rc.rLen], idx, &rc.scratch)
	if err != nil || fallThrough {
		// Malformed request line/headers, or smuggling detection: the
		// blocking path re-reads the buffered bytes and writes the
		// error/400/fallback response.
		return actHandoff
	}
	if len(excess) > 0 {
		// Pipelined bytes past the header block (body or next request):
		// the blocking path consumes them with full framing.
		return actHandoff
	}

	fp := rc.parser.fastPath
	if fp == nil {
		return actHandoff
	}
	now := rc.parser.nowFunc()
	resp, hit := fp.TryHit(req, now)
	if !hit || resp == nil {
		return actHandoff
	}

	if rc.writeVecFn != nil {
		// Zero-copy path (Linux transport): retain the response's
		// net.Buffers slices for writev. Release is deferred until the
		// flush completes — Buffers[2] is the cache object body, so
		// copying it would cost a full-body memcpy per hit.
		rc.writeVec = append(rc.writeVec[:0], resp.Buffers...)
		rc.writeVecOffs = rc.writeVecOffs[:0]
		rc.retainResp = resp
		rc.retainFP = fp
		rc.writeBuf = rc.writeBuf[:0]
	} else {
		// No transport writev (tests, non-Linux stubs): copy into the
		// pooled buffer and release immediately.
		rc.writeBuf = rc.writeBuf[:0]
		for _, b := range resp.Buffers {
			rc.writeBuf = append(rc.writeBuf, b...)
		}
		rc.retainResp = nil
		rc.retainFP = nil
	}
	rc.writeLen = 0

	if hook := rc.parser.metricsHook; hook != nil {
		dur := rc.parser.nowFunc().Sub(now)
		hook(req.Method, resp.Route, resp.CacheResult,
			resp.Source, resp.StatusCode, resp.BytesOut, dur)
	}

	// Reset for the next request on this connection.
	rc.rLen = 0
	rc.scratch = api.RawRequest{Scheme: rc.parser.scheme}
	rc.state = rcWriting
	return rc.advanceWriting()
}

// advanceWriting flushes writeBuf; on completion the connection goes
// back to reading. Partial writes keep the connection in rcWriting
// with write readiness re-armed — the reactor finishes every response
// it started (handoff mid-write is not allowed).
// advanceWriting flushes the response; on completion the connection
// goes back to reading. When the transport supplied a writev func
// (writeVecFn) and the response was retained as buffers, the flush is
// zero-copy writev; otherwise it falls back to the single-buffer
// writeBuf + writeFn path. Partial writes keep the connection in
// rcWriting with write readiness re-armed — the reactor finishes
// every response it started (handoff mid-write is not allowed), and
// the retained fast-path response is released only after the last
// byte is written.
func (rc *reactorConn) advanceWriting() rcAction {
	if rc.writeVecFn != nil && len(rc.writeVec) > 0 {
		return rc.advanceWritingVec()
	}
	for rc.writeLen < len(rc.writeBuf) {
		n, err := rc.writeFn(rc.writeBuf[rc.writeLen:])
		rc.writeLen += n
		if err != nil {
			if errors.Is(err, errAgain) {
				return actWaitWrite
			}
			return actClose
		}
		if n == 0 {
			return actWaitWrite
		}
	}
	rc.finishWrite()
	return actWaitRead
}

// advanceWritingVec flushes the retained response buffers via the
// transport's writev func. Completion is tracked by writeLen against
// the total response size: a partial writev resumes from the exact
// unflushed byte, and the retained response is released only when
// every byte is on the socket.
func (rc *reactorConn) advanceWritingVec() rcAction {
	total := 0
	for _, b := range rc.writeVec {
		total += len(b)
	}
	for rc.writeLen < total {
		iov := rc.pendingVec()
		n, err := rc.writeVecFn(iov)
		rc.writeLen += n
		if err != nil {
			if errors.Is(err, errAgain) {
				return actWaitWrite
			}
			return actClose
		}
		if n == 0 {
			return actWaitWrite
		}
	}
	rc.finishWrite()
	return actWaitRead
}

// pendingVec returns the unflushed portion of the response as an iov
// slice. Rebuilt from writeLen each call: fully flushed slices are
// dropped, the straddling slice is re-sliced at its intra-slice
// offset. No allocation — the backing array is writeVec itself.
func (rc *reactorConn) pendingVec() [][]byte {
	if rc.writeLen == 0 {
		// Common case: nothing flushed yet — the full iov, no copy.
		return rc.writeVec
	}
	remaining := rc.writeLen
	i := 0
	for i < len(rc.writeVec) && remaining >= len(rc.writeVec[i]) {
		remaining -= len(rc.writeVec[i])
		i++
	}
	if i >= len(rc.writeVec) {
		return nil
	}
	if remaining == 0 {
		// Slice-boundary resume: still no copy.
		return rc.writeVec[i:]
	}
	// Straddling slice: one small allocation (partial-resume only —
	// the hot full-flush path never lands here).
	iov := make([][]byte, 0, len(rc.writeVec)-i)
	iov = append(iov, rc.writeVec[i][remaining:])
	return append(iov, rc.writeVec[i+1:]...)
}

// finishWrite completes a flush: release the retained fast-path
// response (its body bytes were aliased by writev), reset the write
// state, and return the connection to reading.
func (rc *reactorConn) finishWrite() {
	if rc.retainFP != nil && rc.retainResp != nil {
		rc.retainFP.Release(rc.retainResp)
		rc.retainResp = nil
		rc.retainFP = nil
	}
	rc.writeVec = rc.writeVec[:0]
	rc.writeVecOffs = rc.writeVecOffs[:0]
	rc.writeBuf = rc.writeBuf[:0]
	rc.writeLen = 0
	rc.state = rcReading
}

// handoffConn wraps the connection with the buffered bytes so the
// blocking parser re-reads the request from the buffer and continues
// from the live socket. The blocking parser takes exclusive ownership
// of the net.Conn from here.
func (rc *reactorConn) handoffConn() net.Conn {
	return &prefixConn{Conn: rc.conn, prefix: rc.readBuf[:rc.rLen]}
}

// handoff starts the blocking Parser on a goroutine, replaying the
// buffered bytes. Called by the transport on actHandoff.
func (rc *reactorConn) handoff() {
	conn := rc.handoffConn()
	rc.state = rcHandoff
	go func() { _ = rc.parser.Serve(conn) }()
}

// idleExpired reports whether the connection has made no read
// progress for longer than the reactor idle budget.
func (rc *reactorConn) idleExpired(now time.Time) bool {
	return now.Sub(rc.lastRead) > reactorIdleTimeout
}

// reactorMaxConns caps connections per reactor loop; beyond it, new
// connections go straight to the blocking path, which scales across
// Ps. Bounds the per-loop fd set and single-goroutine parse capacity.
const reactorMaxConns = 4096

// reactorIdleTimeout is the idle budget before handoff (the blocking
// parser then applies its own deadline handling). Mirrors the blocking
// parser's default idleRead.
const reactorIdleTimeout = 120 * time.Second

// reactorWritePool pools serialized-response buffers across reactor
// connections.
var reactorWritePool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 4096)
		return &buf
	},
}

// reactorWriteCap is the pooled-buffer retention cap; larger
// serialized responses are dropped rather than pinned in the pool.
const reactorWriteCap = 64 * 1024
