// Package h1parser implements a zero-allocation HTTP/1.1 request parser
// for the cache hit fast path. It parses request lines and headers from
// a net.Conn into a stack-allocated RawRequest struct, bypassing
// fasthttp's pooled *fasthttp.RequestCtx machinery on the hit path.
//
// The parser handles keep-alive in a loop: parse → try fast path →
// serve or fall through. On fall-through (miss path), the parser
// constructs a *fasthttp.RequestCtx from the parsed request and calls
// the fallback fasthttp.RequestHandler directly — no byte
// reconstruction or net/http handoff needed.
//
// Unstable.
package h1parser

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/bouine-cache/bouine/internal/platform"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// readBufferSize is the size of the pooled read buffer. 16 KB covers
// 99.9%+ of real-world HTTP/1.1 request headers. Requests exceeding
// this fall through to the fallback handler.
const readBufferSize = 16 * 1024

// oversizeBufferSize is the bufio.Reader buffer size for the
// oversize-header fallback (headers that exceeded readBufferSize): the
// 16 KiB already buffered plus the remainder still on the wire must fit,
// and the per-header caps (AGENTS.md §6) bound the head at 64 KiB.
const oversizeBufferSize = 64 * 1024

// refreshThreshold is the minimum remaining read-deadline window below
// which Serve re-arms the read deadline: one setsockopt per interval
// instead of one per request.
const refreshThreshold = 2 * time.Second

// writeRefreshThreshold is the minimum remaining write-deadline window
// below which serveHit re-arms the safety-net write deadline. Mirrors
// the read-deadline refresh strategy: one setsockopt per threshold
// interval instead of one per request. Each hit response is guaranteed
// at least this much write budget, and the full safety-net window
// (p.writeTime, 5 minutes) is far larger.
const writeRefreshThreshold = time.Minute

// Parser parses HTTP/1.1 requests from a net.Conn and dispatches to
// the fast path or falls through to the fasthttp.RequestHandler.
type Parser struct {
	fastPath      api.FastPathHandler
	fallback      fasthttp.RequestHandler
	nowFunc       func() time.Time
	metricsHook   func(pool, trafficClass, cacheResult, source string, status, bytesOut int, duration time.Duration)
	smugglingHook func()
	// connState, when set, is invoked around each request: active=true
	// after a request head is parsed, active=false once the response is
	// fully written and the connection goes back to idle keep-alive. The
	// listener uses it to distinguish idle from in-flight connections
	// during graceful shutdown (fasthttp.Server.Shutdown parity).
	connState func(conn net.Conn, active bool)
	// fallReaderPool caches *bufio.Readers with readBufferSize-byte
	// buffers for the fall-through request replays. handleFallThrough
	// allocates a fresh 16 KiB reader per fall-through request without
	// it; at a ~43% fall-through rate under load that alone was ~50% of
	// all allocated bytes (~25-60 MB/s per pod), driving GC cycles every
	// few hundred ms and burning 10-40% CPU in scan/mark. Entries are
	// Reset on checkout (re-pointed at the per-request prefixConn,
	// stale buffered bytes discarded) and on release (reference dropped
	// so a pooled entry never pins a connection). The Parser is shared
	// by every connection of a listener, so the pool is process-wide
	// and entry count tracks in-flight fall-throughs, not open conns.
	// Pooled readers are safe to reuse: fasthttp copies header values
	// out of the peek buffer into its own byte buckets
	// (header.go parseHeaders: append(h.host[:0], s.value...)).
	fallReaderPool sync.Pool

	// oversizeReaderPool caches the 64 KiB readers for the
	// oversize-header fallback (handleFallThroughRaw). Same reset
	// contract as fallReaderPool; a separate pool so the common path
	// does not pay 64 KiB per entry.
	oversizeReaderPool sync.Pool

	scheme    string
	idleRead  time.Duration
	writeTime time.Duration
}

// New creates a Parser. fastPath may be nil — when nil, all requests
// fall through to the fallback handler. fallback must not be nil.
func New(fastPath api.FastPathHandler, fallback fasthttp.RequestHandler, opts ...Option) *Parser {
	p := &Parser{
		fastPath:  fastPath,
		fallback:  fallback,
		nowFunc:   time.Now,
		idleRead:  120 * time.Second, // matches the fasthttp IdleTimeout; nginx uses 65s
		writeTime: 5 * time.Minute,
	}
	for _, opt := range opts {
		opt(p)
	}
	p.fallReaderPool.New = func() any { return bufio.NewReaderSize(eofReader{}, readBufferSize) }
	p.oversizeReaderPool.New = func() any { return bufio.NewReaderSize(eofReader{}, oversizeBufferSize) }
	return p
}

// Option configures a Parser.
type Option func(*Parser)

// WithNowFunc sets the time function (default time.Now).
func WithNowFunc(fn func() time.Time) Option {
	return func(p *Parser) { p.nowFunc = fn }
}

// WithIdleReadTimeout sets the read deadline for parsing a request.
func WithIdleReadTimeout(d time.Duration) Option {
	return func(p *Parser) { p.idleRead = d }
}

// WithWriteTimeout sets the write deadline for responses.
func WithWriteTimeout(d time.Duration) Option {
	return func(p *Parser) { p.writeTime = d }
}

// WithScheme sets the URL scheme ("http" or "https") used to build
// cache keys. The listener sets this based on whether the connection
// is TLS.
func WithScheme(scheme string) Option {
	return func(p *Parser) { p.scheme = scheme }
}

// WithMetricsHook sets a callback invoked after each fast-path hit.
func WithMetricsHook(fn func(pool, trafficClass, cacheResult, source string, status, bytesOut int, duration time.Duration)) Option {
	return func(p *Parser) { p.metricsHook = fn }
}

// WithSmugglingHook sets a callback invoked when the parser detects an
// HTTP smuggling attempt.
func WithSmugglingHook(fn func()) Option {
	return func(p *Parser) { p.smugglingHook = fn }
}

// WithConnStateHook sets a callback invoked with active=true after a
// request head is parsed and active=false once its response is fully
// written. Called on every request including fast-path hits; it must
// not allocate (the hit path's allocs/op budget is zero).
func WithConnStateHook(fn func(conn net.Conn, active bool)) Option {
	return func(p *Parser) { p.connState = fn }
}

// markActive reports the connection as serving a request. Zero-alloc.
func (p *Parser) markActive(conn net.Conn) {
	if p.connState != nil {
		p.connState(conn, true)
	}
}

// markIdle reports the connection as back to idle keep-alive. Zero-alloc.
func (p *Parser) markIdle(conn net.Conn) {
	if p.connState != nil {
		p.connState(conn, false)
	}
}

// serveResult tells the Serve keep-alive loop what to do next.
type serveResult int

const (
	// serveContinue means the connection is ready for the next request.
	serveContinue serveResult = iota
	// serveClose means the connection must be closed and Serve returns.
	serveClose
)

// refreshReadDeadline advances the read deadline when the remaining
// window is too small to read another request — one setsockopt per
// refreshThreshold interval instead of one per request.
func (p *Parser) refreshReadDeadline(conn net.Conn, deadline *time.Time) error {
	if remaining := deadline.Sub(p.nowFunc()); remaining >= refreshThreshold {
		return nil
	}
	*deadline = p.nowFunc().Add(p.idleRead)
	return conn.SetReadDeadline(*deadline)
}

// rearmAfterFallThrough restores the parser's deadline ownership after
// the fallback handler managed its own timeouts: the read deadline is
// re-armed for the next keep-alive request (slowloris protection) and
// the write-deadline tracker is reset so the next hit re-arms it.
func (p *Parser) rearmAfterFallThrough(conn net.Conn, deadline *time.Time, wd *time.Time) error {
	*deadline = p.nowFunc().Add(p.idleRead)
	if err := conn.SetReadDeadline(*deadline); err != nil {
		return err
	}
	*wd = time.Time{}
	return nil
}

// serveFallThroughRequest runs the fallback handler for a parsed request
// and reports whether the connection may continue. Smuggling requests
// are rejected with 400 (RFC 9110 §6.6.2) — an ambiguously framed body
// cannot be safely delimited for keep-alive reuse, so the connection
// always closes after a 400.
func (p *Parser) serveFallThroughRequest(conn net.Conn, req *api.RawRequest, excess []byte, deadline *time.Time, wd *time.Time) (serveResult, error) {
	if req == nil {
		// Oversize headers (>16 KiB): excess holds the buffered prefix.
		// Hand the bytes to the fallback handler via a prefix conn so the
		// request is served, not dropped. The fallback owns the
		// connection's read state afterwards, so the connection closes.
		if len(excess) == 0 {
			return serveClose, nil
		}
		p.handleFallThroughRaw(conn, excess)
		return serveClose, nil
	}
	if smugglingDetected(req) {
		_ = writeBadRequest(conn)
		return serveClose, nil
	}
	close, err := p.handleFallThrough(conn, req, excess)
	if err != nil {
		return serveClose, err
	}
	if close {
		return serveClose, nil
	}
	if err := p.rearmAfterFallThrough(conn, deadline, wd); err != nil {
		return serveClose, err
	}
	return serveContinue, nil
}

// Serve handles a single connection: parse HTTP/1.1 requests in a
// keep-alive loop, dispatching hits to the fast path and misses to the
// fallback handler. The connection stays alive across both hits
// and misses until the client sends Connection: close, the parser
// hits a read error, or the idle deadline expires.
func (p *Parser) Serve(conn net.Conn) error {
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetNoDelay(true) // Critical: prevents Nagle's 40ms delay on small hit responses.
	}
	// TCP_QUICKACK tells the kernel to ACK received packets immediately
	// instead of delaying, reducing perceived latency for keep-alive
	// clients. No-op on non-Linux.
	platform.SetTCPQuickAckConn(conn)

	var readBuf [readBufferSize]byte

	// scratch is the per-connection request struct, reset and refilled by
	// every parseRequest call. Allocating a fresh RawRequest per request
	// (the struct embeds a [100]RawHeader array, ~4 KB) was the dominant
	// allocation on the hit path — 99.3% of bytes allocated under load —
	// driving ~63 GC cycles/s and stealing CPU from request goroutines as
	// mark-assist. The header strings alias readBuf, which is already
	// overwritten by the next request, so reusing the struct keeps the
	// same lifetime semantics: nothing may retain a request beyond its
	// iteration, which the fall-through contract already required.
	var scratch api.RawRequest

	// Set the initial read deadline once. The deadline is refreshed
	// lazily: only when the remaining time drops below the refresh
	// threshold, avoiding a setsockopt syscall on every request.
	deadline := p.nowFunc().Add(p.idleRead)
	if err := conn.SetReadDeadline(deadline); err != nil {
		return err
	}

	// writeDeadline mirrors the read-deadline refresh strategy for the
	// hit-path write safety net. The zero value means "not armed" — the
	// first hit arms it, and it is re-armed only when the remaining
	// window drops below writeRefreshThreshold. Fall-through paths
	// clear and set the OS write deadline themselves, so the tracker is
	// reset to zero after each fall-through.
	var writeDeadline time.Time

	for {
		if err := p.refreshReadDeadline(conn, &deadline); err != nil {
			return err
		}

		req, fallThrough, excess, err := p.parseRequest(conn, &readBuf, &scratch)
		if err != nil {
			return err
		}
		p.markActive(conn)
		if fallThrough {
			res, err := p.serveFallThroughRequest(conn, req, excess, &deadline, &writeDeadline)
			if res == serveClose || err != nil {
				return err
			}
			p.markIdle(conn)
			continue
		}

		// Try the fast path.
		if p.fastPath != nil {
			res, err := p.serveFastHit(conn, req, excess, &deadline, &writeDeadline)
			switch {
			case err != nil:
				return err
			case res == serveClose:
				return nil
			case res == serveContinue:
				p.markIdle(conn)
				continue
			}
		}

		// Miss path: call the fallback handler with a fasthttp.RequestCtx.
		if res, err := p.serveFastMiss(conn, req, excess, &deadline, &writeDeadline); res == serveClose || err != nil {
			return err
		}
		p.markIdle(conn)
	}
}

// serveFastMiss runs the blocking fallback for a request the fast path
// declined, then re-arms the parser's deadline ownership for the next
// keep-alive request. Returns serveClose when the connection ends.
func (p *Parser) serveFastMiss(conn net.Conn, req *api.RawRequest, excess []byte, deadline *time.Time, wd *time.Time) (serveResult, error) {
	close, ftErr := p.handleFallThrough(conn, req, excess)
	if ftErr != nil {
		return serveClose, ftErr
	}
	if close {
		return serveClose, nil
	}
	// Re-arm the read deadline for the next keep-alive request.
	*deadline = p.nowFunc().Add(p.idleRead)
	if err := conn.SetReadDeadline(*deadline); err != nil {
		return serveClose, err
	}
	// handleFallThrough cleared the OS write deadline so the fallback
	// handler could manage its own timeouts; reset the tracker so the
	// next hit re-arms it.
	*wd = time.Time{}
	return serveContinue, nil
}

// serveFastHit attempts the fast path for a parsed request. A hit serves
// the response directly (serveContinue), unless the client pipelined
// bytes past the header block — those must be consumed by the fallback
// handler with proper framing, so the result falls through. When the
// fast path does not hit, the caller falls through to the miss path.
func (p *Parser) serveFastHit(conn net.Conn, req *api.RawRequest, excess []byte, deadline *time.Time, wd *time.Time) (serveResult, error) {
	now := p.nowFunc()
	resp, hit := p.fastPath.TryHit(req, now)
	if !hit || resp == nil {
		// Signal the caller to run the miss path by returning a distinct
		// sentinel: fall through via serveFallThroughRequest below.
		return p.fastPathMissResult(conn, req, excess, deadline, wd)
	}
	if err := p.serveHit(conn, resp, now, wd); err != nil {
		p.fastPath.Release(resp)
		return serveClose, err
	}
	if p.metricsHook != nil {
		dur := p.nowFunc().Sub(now)
		p.metricsHook(resp.Pool, resp.TrafficClass, resp.CacheResult,
			resp.Source, resp.StatusCode, resp.BytesOut, dur)
	}
	closeConn := resp.CloseConn
	p.fastPath.Release(resp)
	if closeConn {
		// The request asked for Connection: close (RFC 9112 §9.6) and
		// the serialized response ended with "Connection: close" — the
		// connection must not be reused after this response.
		return serveClose, nil
	}
	if len(excess) == 0 {
		return serveContinue, nil
	}
	// The client pipelined bytes past this request's header block (a
	// body or the start of the next request). They are still unread on
	// the socket. Re-enter the fallback handler with the buffered bytes
	// so it consumes them with proper framing instead of the next
	// parseRequest iteration discarding them.
	return p.serveFallThroughRequest(conn, req, excess, deadline, wd)
}

// fastPathMissResult runs the fallback handler for a request the fast
// path declined (miss, conditional, range). It converts the fall-through
// outcome into a serveResult.
func (p *Parser) fastPathMissResult(conn net.Conn, req *api.RawRequest, excess []byte, deadline *time.Time, wd *time.Time) (serveResult, error) {
	close, err := p.handleFallThrough(conn, req, excess)
	if err != nil {
		return serveClose, err
	}
	if close {
		return serveClose, nil
	}
	if err := p.rearmAfterFallThrough(conn, deadline, wd); err != nil {
		return serveClose, err
	}
	return serveContinue, nil
}

// badRequestResponse is the pre-serialized 400 response written when
// the parser detects an HTTP smuggling attempt (RFC 9110 §6.6.2).
var badRequestResponse = []byte("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")

// writeBadRequest writes the pre-serialized 400 response and closes the
// connection — an ambiguously framed request cannot be safely delimited
// for keep-alive reuse.
func writeBadRequest(conn net.Conn) error {
	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := conn.Write(badRequestResponse)
	return err
}

// parseRequest reads and parses a single HTTP/1.1 request from conn.
// scratch is the caller's reusable RawRequest; the returned request
// aliases it and is valid only until the next parseRequest call on the
// same connection (the header strings alias readBuf).
func (p *Parser) parseRequest(conn net.Conn, readBuf *[readBufferSize]byte, scratch *api.RawRequest) (*api.RawRequest, bool, []byte, error) {
	buf := readBuf[:0]

	headerEnd := -1
	for {
		n, err := conn.Read(buf[len(buf):cap(buf)])
		if n > 0 {
			buf = buf[:len(buf)+n]
		}
		if err != nil {
			if err == io.EOF && len(buf) > 0 {
				break
			}
			return nil, true, nil, err
		}
		if idx := findHeaderEnd(buf); idx >= 0 {
			headerEnd = idx
			break
		}
		if len(buf) >= cap(buf) {
			// Headers exceeded readBufferSize. Hand the buffered bytes back
			// so the caller can wrap the connection with oversizeHeaderConn
			// and let the fallback handler parse the full request from the
			// buffer + live socket, instead of dropping it.
			return nil, true, buf, nil
		}
	}

	req, fallThrough, excess, err := p.parseBuffer(buf, headerEnd, scratch)
	if err != nil || fallThrough {
		return req, fallThrough, excess, err
	}
	return req, false, excess, nil
}

// parseBuffer parses one request from a fully-read header block.
// headerEnd is the index just past the \r\n\r\n terminator, or -1
// when the buffer ended at EOF without
// one. The returned request aliases scratch and buf and is valid only
// until the next parse call.
func (p *Parser) parseBuffer(buf []byte, headerEnd int, scratch *api.RawRequest) (*api.RawRequest, bool, []byte, error) {
	req := scratch
	// Soft reset: assign the scalar fields instead of zeroing the
	// whole struct — the [100]RawHeader array (~3.3 KB) is not read
	// past NHeaders by any consumer, and the struct-literal assignment
	// would memset it plus evict ~56 cache lines the header writes
	// immediately refill. parseHeaders re-derives every flag field, so
	// clearing them here is unnecessary work.
	req.Method = ""
	req.Path = ""
	req.Query = ""
	req.Host = ""
	req.HTTPVersion = ""
	req.CacheControlRaw = ""
	req.ScanFlags = 0
	req.NHeaders = 0
	req.ConnectionClose = false
	req.OwnerMiss = false
	req.OwnerGateReject = false
	req.OwnerShed = false
	req.OwnerResolver = nil
	req.Scheme = p.scheme
	if err := parseRequestLine(buf, req); err != nil {
		return nil, true, nil, err
	}
	if err := parseHeaders(buf, req); err != nil {
		return nil, true, nil, err
	}

	if smugglingDetected(req) {
		if p.smugglingHook != nil {
			p.smugglingHook()
		}
		return req, true, nil, nil
	}

	var excess []byte
	if headerEnd >= 0 && headerEnd < len(buf) {
		excess = buf[headerEnd:]
	}
	return req, false, excess, nil
}

// headerTerm is the request-head terminator, hoisted to a package var.
// A per-call []byte("\r\n\r\n") conversion is stack-allocated only as
// long as the enclosing function is not inlined; when PGO raises the
// inline budget the conversion escapes to the heap — measured as
// +1 allocs/op on the hit path with the committed profile.
// A shared slice keeps the terminator scan allocation-free at any
// inline depth.
var headerTerm = []byte("\r\n\r\n")

// findHeaderEnd searches for the \r\n\r\n terminator in buf.
func findHeaderEnd(buf []byte) int {
	idx := bytes.Index(buf, headerTerm)
	if idx < 0 {
		return -1
	}
	return idx + 4
}

// parseRequestLine parses the first line: "METHOD SP PATH SP VERSION\r\n".
func parseRequestLine(buf []byte, req *api.RawRequest) error {
	lineEnd := 0
	for lineEnd < len(buf) && buf[lineEnd] != '\r' {
		lineEnd++
	}
	if lineEnd >= len(buf)-1 || buf[lineEnd+1] != '\n' {
		return errors.New("h1parser: malformed request line")
	}
	line := buf[:lineEnd]

	sp1 := 0
	for sp1 < len(line) && line[sp1] != ' ' {
		sp1++
	}
	if sp1 == len(line) {
		return errors.New("h1parser: missing path")
	}
	req.Method = header.BytesToString(line[:sp1])

	sp2 := sp1 + 1
	for sp2 < len(line) && line[sp2] != ' ' {
		sp2++
	}
	if sp2 == len(line) {
		return errors.New("h1parser: missing version")
	}
	fullPath := header.BytesToString(line[sp1+1 : sp2])

	if q := indexByte(fullPath, '?'); q >= 0 {
		req.Path = fullPath[:q]
		req.Query = fullPath[q+1:]
	} else {
		req.Path = fullPath
	}

	req.HTTPVersion = header.BytesToString(line[sp2+1:])

	return nil
}

// parseHeaders parses header lines from buf, starting after the
// request line. The single pass also derives everything downstream
// consumers need — Host (first wins), Cache-Control raw value (last
// wins), scan flags (conditional/precondition/TE/CL/duplicate-CL/
// Pragma: no-cache, per api.ScanFlagForHeader), and the Connection
// close token — so no consumer re-scans the header array on the hit
// path (the fused scan).
func parseHeaders(buf []byte, req *api.RawRequest) error {
	pos := skipRequestLine(buf)

	for pos < len(buf) {
		if buf[pos] == '\r' && pos+1 < len(buf) && buf[pos+1] == '\n' {
			break
		}

		lineEnd := pos
		for lineEnd < len(buf)-1 && (buf[lineEnd] != '\r' || buf[lineEnd+1] != '\n') {
			lineEnd++
		}
		if lineEnd >= len(buf)-1 {
			break
		}

		if buf[pos] == ' ' || buf[pos] == '\t' {
			return errors.New("h1parser: obs-fold not supported")
		}

		if req.NHeaders >= api.MaxRawHeaders {
			return errors.New("h1parser: too many headers")
		}

		appendHeader(req, buf[pos:lineEnd])

		pos = lineEnd + 2
	}

	// Connection: close detection (RFC 9110 §7.6.1 / RFC 9112 §9.6): the request
	// asked to terminate the connection after the response. The flag is
	// read by the fast path (Connection trailer + CloseConn) and by
	// Serve to leave its keep-alive loop after a hit. appendHeader sets
	// the flag when any Connection header carries a close token (the
	// !ConnectionClose guard skips already-decided requests), so it
	// needs no reset here; a request with no Connection header at all
	// leaves the soft reset's false in place.
	return nil
}

// ParseHeadersForTest exposes the parseHeaders header stage to other
// packages' test suites (cross-parser parity tests in internal/cache):
// it runs the real production header parser over a full request head,
// not a reimplementation. Not for production use.
func ParseHeadersForTest(buf []byte, req *api.RawRequest) error {
	return parseHeaders(buf, req)
}

// connectionCloseValue reports whether a Connection header value
// contains a "close" token (case-insensitive, comma-separated list per
// RFC 9110 §7.6.1). Zero allocation — tokens are subslices of the
// header value, which itself aliases the read buffer.
func connectionCloseValue(val string) bool {
	if val == "" {
		return false
	}
	for _, token := range splitHeaderTokens(val) {
		if api.EqualFold(token, "close") {
			return true
		}
	}
	return false
}

// skipRequestLine advances past the first \r\n in buf.
func skipRequestLine(buf []byte) int {
	pos := 0
	for pos < len(buf)-1 && (buf[pos] != '\r' || buf[pos+1] != '\n') {
		pos++
	}
	return pos + 2
}

// smugglingDetected checks for HTTP request smuggling indicators per
// RFC 9110 §6.6.2 and AGENTS.md §6: CL+TE together, or a duplicate
// Content-Length. Both facts were derived by the parser's fused scan
// (FlagHasCL / FlagHasTE / FlagDuplicateCL); hand-built requests that
// skip RecomputeScanFlags must call it first.
func smugglingDetected(req *api.RawRequest) bool {
	f := req.ScanFlags
	if f&api.FlagHasCL != 0 && f&api.FlagHasTE != 0 {
		return true
	}
	return f&api.FlagDuplicateCL != 0
}

// appendHeader parses a single header line, appends it to req, and
// folds the scan-flag/Host/Cache-Control/Connection-close derivation
// into the same pass (W2: no consumer re-scans the header array).
// Header-name dispatch is length-guarded before EqualFold: every
// matched name has a distinct length, so the common non-matching
// header pays only a length compare, not a case-insensitive compare.
func appendHeader(req *api.RawRequest, line []byte) {
	colon := 0
	for colon < len(line) && line[colon] != ':' {
		colon++
	}
	if colon == len(line) {
		return
	}

	key := header.BytesToString(line[:colon])
	valStart := colon + 1
	for valStart < len(line) && (line[valStart] == ' ' || line[valStart] == '\t') {
		valStart++
	}
	value := header.BytesToString(line[valStart:])

	req.Headers[req.NHeaders] = api.RawHeader{
		Key:   key,
		Value: value,
	}
	req.NHeaders++

	// Decide from the pre-merge flag state (first-Host-wins reads
	// FlagHostSeen as "was a Host already recorded"), then merge this
	// header's flags — merging first would make every Host look like
	// a duplicate. Duplicate Content-Length is per-request state no
	// single-header helper can derive: the second CL sets the flag,
	// matching RecomputeScanFlags' saturated count (2+ duplicates are
	// smuggling either way).
	flags := api.ScanFlagForHeader(key, value)
	hostSeen := req.ScanFlags&api.FlagHostSeen != 0
	if flags&api.FlagHasCL != 0 && req.ScanFlags&api.FlagHasCL != 0 {
		flags |= api.FlagDuplicateCL
	}
	req.ScanFlags |= flags
	switch {
	case flags&api.FlagHostSeen != 0 && !hostSeen:
		req.Host = value
	case api.EqualFold(key, header.CacheControl):
		// Last Cache-Control wins, matching the pre-fusion scan
		// (each occurrence overwrote ccRaw in header order).
		req.CacheControlRaw = value
	case flags&api.FlagHasConnection != 0 && !req.ConnectionClose:
		req.ConnectionClose = connectionCloseValue(value)
	}
}

// serveHit writes the fast path response to the connection via
// net.Buffers.WriteTo (single writev syscall). The write deadline is
// armed lazily via wd: it is set only on the first hit and re-armed
// when the remaining window drops below writeRefreshThreshold — one
// setsockopt per threshold interval instead of one per request. Each
// hit write is guaranteed at least min(writeTime, writeRefreshThreshold)
// of budget. The caller is responsible for calling Release on resp
// after this returns.
func (p *Parser) serveHit(conn net.Conn, resp *api.FastPathResponse, now time.Time, wd *time.Time) error {
	if wd.IsZero() || wd.Sub(now) < writeRefreshThreshold {
		*wd = now.Add(p.writeTime)
		if err := conn.SetWriteDeadline(*wd); err != nil {
			return err
		}
	}
	_, err := resp.Buffers.WriteTo(conn)
	return err
}

// eofReader is the placeholder io.Reader pooled *bufio.Readers are Reset
// to on release: it never yields data, so a reader left pointing at it
// can never read stale bytes if used before its next checkout, and the
// Reset drops the previous prefixConn (and its net.Conn) reference.
type eofReader struct{}

func (eofReader) Read([]byte) (int, error) { return 0, io.EOF }

// acquireReader checks out a pooled *bufio.Reader and re-points it at rd.
// Reset discards any buffered bytes left over from the previous checkout —
// pipelined bytes buffered past a previous request's body must never leak
// into the next request's parse — and keeps the existing buffer, so the
// 16 KiB/64 KiB allocation happens only when the pool is cold.
func acquireReader(pool *sync.Pool, rd io.Reader) *bufio.Reader {
	br := pool.Get().(*bufio.Reader)
	br.Reset(rd)
	return br
}

// releaseReader returns br to pool after a full reset: buffered bytes are
// discarded and the underlying reader reference is dropped so a pooled
// entry never pins a prefixConn (and through it a net.Conn) past its
// request's lifetime (AGENTS.md §7: always reset on put).
func releaseReader(pool *sync.Pool, br *bufio.Reader) {
	br.Reset(eofReader{})
	pool.Put(br)
}

// handleFallThrough serves a miss-path request via the fallback
// fasthttp.RequestHandler. Instead of copying pre-buffered bytes into the
// ctx and truncating bodies that span multiple TCP reads, it replays the
// buffered request bytes through a prefixConn so fasthttp's own parser
// re-reads the request with full body framing (Content-Length, chunked,
// trailers, Expect: 100-continue) from the live socket. The head rebuild
// and the pooled *bufio.Reader are the only per-request allocations —
// the hit path is untouched.
//
// Returns (close, err). When close is true the caller should return
// from the keep-alive loop — the client requested Connection: close
// or the response indicates the connection should be closed.
func (p *Parser) handleFallThrough(conn net.Conn, req *api.RawRequest, excess []byte) (bool, error) {
	if req == nil {
		return false, errors.New("h1parser: nil request on fall-through")
	}

	head := rebuildRequestHead(req, excess)

	clientClose := isConnectionClose(req)

	// Reset deadlines so the fallback handler manages its own timeouts.
	_ = conn.SetReadDeadline(time.Time{})
	_ = conn.SetWriteDeadline(time.Time{})

	var ctx fasthttp.RequestCtx
	ctx.Init2(conn, nil, false)

	rc := &prefixConn{Conn: conn, prefix: head}
	// 16 KiB matches readBufferSize: the rebuilt head plus any buffered
	// excess can reach that size, and fasthttp errors with "small read
	// buffer" when the bufio.Reader cannot hold the full header block.
	// The reader is pooled — see fallReaderPool — so this is a checkout,
	// not a per-request allocation.
	br := acquireReader(&p.fallReaderPool, rc)
	defer releaseReader(&p.fallReaderPool, br)
	if err := ctx.Request.Read(br); err != nil {
		// Malformed input beyond what the fast parser rejected — the
		// connection state is now indeterminate; close it without
		// surfacing a read error (Serve treats all read failures as
		// connection termination, not listener failure).
		return true, nil //nolint:nilerr // close-connection outcome, not an error to propagate
	}
	if ctx.Request.MayContinue() {
		// Mirror fasthttp's serve loop (server.go:2566): send 100 Continue
		// before reading the body. maxBodySize=0 means unlimited — the
		// route's body limits are enforced downstream by the cache layer.
		if _, err := conn.Write([]byte("HTTP/1.1 100 Continue\r\n\r\n")); err != nil {
			return true, err
		}
		if err := ctx.Request.ContinueReadBody(br, 0, true); err != nil {
			// Body framing failed mid-request: the connection is
			// indeterminate, so close — not an error to propagate.
			return true, nil //nolint:nilerr // close-connection outcome, not an error to propagate
		}
	}

	// Transfer the fast path's peer-branch hints (if any) to the fallback
	// handler: handleCacheMiss reads them to skip duplicate owner lookups
	// and peer RPCs, and reuses the fetched resolver instead of repeating
	// the identical bare ask. Cheap: only set when the peer branch ran.
	transferOwnerMissHint(&ctx, req)

	p.fallback(&ctx)

	return p.writeFallThroughResponse(conn, &ctx, clientClose)
}

// writeFallThroughResponse writes the fallback handler's response and
// reports whether the connection may continue (false) or must close
// (true). Shared by both fall-through paths.
func (p *Parser) writeFallThroughResponse(conn net.Conn, ctx *fasthttp.RequestCtx, clientClose bool) (bool, error) {
	// Propagate Connection: close from the request to the response so
	// the client knows the connection will not be reused. The fasthttp
	// server's own serve loop does this automatically (server.go:2678),
	// but the fall-through paths bypass that loop.
	if clientClose {
		ctx.Response.Header.SetConnectionClose()
	}

	// Write the response to the connection. Streamed responses (body
	// set via SetBodyStreamWriter — SSE, unbuffered passthrough) manage
	// their own lifetime: an absolute write deadline would cut every
	// long-lived stream at p.writeTime. Wrap the conn so each Write
	// re-arms the deadline instead (idle semantics): a stream that keeps
	// writing lives as long as it flows; a client that stops reading is
	// still dropped after one idle budget, preserving the slowloris net.
	writeConn := conn
	if ctx.Response.IsBodyStream() {
		writeConn = &idleWriteConn{Conn: conn, budget: p.writeTime}
	}
	if err := conn.SetWriteDeadline(p.nowFunc().Add(p.writeTime)); err != nil {
		return false, err
	}
	if _, err := ctx.Response.WriteTo(writeConn); err != nil {
		return false, err
	}

	// If the handler itself set Connection: close (e.g. via
	// ctx.SetConnectionClose), honour that too.
	return clientClose || ctx.Response.Header.ConnectionClose(), nil
}

// transferOwnerMissHint forwards the fast path's peer-branch hints
// (api.RawRequest.OwnerMiss, OwnerGateReject, and OwnerShed) to the fallback
// RequestCtx so handleCacheMiss can skip duplicate owner lookups, peer RPCs,
// or shed peer retries — and the fetched metadata-only Vary resolver
// (api.RawRequest.OwnerResolver, ADR-0068) so the slow path runs the two-step
// variant resolution without repeating the identical bare ask. Cheap: only
// set when the fast-path peer branch ran.
func transferOwnerMissHint(ctx *fasthttp.RequestCtx, req *api.RawRequest) {
	if req.OwnerMiss {
		ctx.SetUserValue(api.OwnerMissContextKey, true)
	}
	if req.OwnerGateReject {
		ctx.SetUserValue(api.OwnerGateRejectContextKey, true)
	}
	if req.OwnerShed {
		ctx.SetUserValue(api.OwnerShedContextKey, true)
	}
	if req.OwnerResolver != nil {
		ctx.SetUserValue(api.OwnerResolverContextKey, req.OwnerResolver)
	}
}

// rebuildRequestHead rebuilds the wire bytes of the parsed request head
// so fasthttp's parser can re-read it: method, path, version, headers,
// terminator, plus any buffered excess bytes. The bytes are rebuilt into
// a fresh buffer (miss path only) because readBuf is reused by the next
// request on this connection after the handler returns. Host is
// re-emitted first so the fallback sees it even if the original header
// block lacked one.
func rebuildRequestHead(req *api.RawRequest, excess []byte) []byte {
	head := make([]byte, 0, 256+len(req.Path)+len(req.Query)+len(req.Host))
	head = append(head, req.Method...)
	head = append(head, ' ')
	head = append(head, req.Path...)
	if req.Query != "" {
		head = append(head, '?')
		head = append(head, req.Query...)
	}
	head = append(head, ' ')
	head = append(head, req.HTTPVersion...)
	head = append(head, '\r', '\n')
	head = append(head, "Host: "...)
	head = append(head, req.Host...)
	head = append(head, '\r', '\n')
	for i := 0; i < req.NHeaders; i++ {
		if api.EqualFold(req.Headers[i].Key, header.Host) {
			// Already re-emitted above from req.Host; replaying the original
			// would duplicate it and fasthttp rejects multiple Host headers.
			continue
		}
		head = append(head, req.Headers[i].Key...)
		head = append(head, ": "...)
		head = append(head, req.Headers[i].Value...)
		head = append(head, '\r', '\n')
	}
	head = append(head, '\r', '\n')
	if len(excess) > 0 {
		head = append(head, excess...)
	}
	return head
}

// handleFallThroughRaw serves a request whose headers exceeded the
// fast parser's 16 KiB read buffer: buffered holds the bytes already
// consumed from the socket; the rest of the request (headers and body)
// is still on the wire. The fallback handler parses the complete request
// via fasthttp with proper framing. The connection always closes after
// the response: the fallback owns the read state afterwards, so
// pipelining across the boundary is not safe. Failures are
// per-connection outcomes (close), nothing to propagate.
//
//nolint:nilerr // malformed/unreadable oversize requests close the connection; nothing to propagate
func (p *Parser) handleFallThroughRaw(conn net.Conn, buffered []byte) {
	// Reset deadlines so the fallback handler manages its own timeouts.
	_ = conn.SetReadDeadline(time.Time{})
	_ = conn.SetWriteDeadline(time.Time{})

	var ctx fasthttp.RequestCtx
	ctx.Init2(conn, nil, false)

	rc := &prefixConn{Conn: conn, prefix: buffered}
	// The buffered bytes alone can approach 16 KiB (the h1parser read
	// buffer), so the bufio.Reader must be large enough to hold them plus
	// the remainder from the socket; otherwise fasthttp fails with
	// "small read buffer". Pooled like the fall-through reader — see
	// oversizeReaderPool.
	br := acquireReader(&p.oversizeReaderPool, rc)
	defer releaseReader(&p.oversizeReaderPool, br)
	if err := ctx.Request.Read(br); err != nil {
		_ = writeBadRequest(conn)
		return
	}

	p.fallback(&ctx)
	// clientClose=true: the fallback owned the read state afterwards, so
	// this connection always closes after the response.
	_, _ = p.writeFallThroughResponse(conn, &ctx, true)
}

// idleWriteConn re-arms the write deadline before every Write, turning
// an absolute write-time budget into an idle budget. Long-lived streamed
// responses (SSE) are cut by an absolute deadline even while actively
// delivering events; with per-Write re-arming they survive as long as
// they keep making progress, while a client that stops reading entirely
// is dropped after one budget — the slowloris protection is preserved.
//
// Only the fall-through response write path uses this wrapper; the hit
// path's zero-copy writev is untouched.
type idleWriteConn struct {
	net.Conn
	budget time.Duration
}

func (c *idleWriteConn) Write(p []byte) (int, error) {
	// SetWriteDeadline is not overridden, so the selector-free call
	// resolves to the embedded conn; c.Conn.Write IS deliberate — Write is
	// overridden here, so the embedded-field selector is required to avoid
	// infinite recursion.
	_ = c.SetWriteDeadline(time.Now().Add(c.budget))
	return c.Conn.Write(p)
}

// prefixConn serves Read calls from prefix before falling through to the
// wrapped net.Conn. It lets the fallback handler re-parse the buffered
// request bytes and then continue reading body bytes from the socket
// with correct framing.
type prefixConn struct {
	net.Conn
	prefix []byte
}

func (c *prefixConn) Read(b []byte) (int, error) {
	if len(c.prefix) > 0 {
		n := copy(b, c.prefix)
		c.prefix = c.prefix[n:]
		return n, nil
	}
	return c.Conn.Read(b)
}

// indexByte is a simple byte search.
func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// isConnectionClose reports whether the request contains a
// Connection: close token (RFC 9110 §7.6.1). The parser's fused header
// scan already derived the token decision into req.ConnectionClose
// (any Connection header carrying a close token sets it); reading
// the flag here replaces a header-array scan per fall-through.
// Hand-built requests that set Headers directly must derive the flag
// themselves — tests construct ConnectionClose directly.
func isConnectionClose(req *api.RawRequest) bool {
	return req.ConnectionClose
}

// splitHeaderTokens splits a comma-separated header value into trimmed
// tokens. It does not allocate — it returns subslices of the input.
func splitHeaderTokens(val string) []string {
	var tokens []string
	for len(val) > 0 {
		comma := indexByte(val, ',')
		var token string
		if comma < 0 {
			token = val
			val = ""
		} else {
			token = val[:comma]
			val = val[comma+1:]
		}
		// Trim OWS (optional whitespace).
		for len(token) > 0 && (token[0] == ' ' || token[0] == '\t') {
			token = token[1:]
		}
		for len(token) > 0 && (token[len(token)-1] == ' ' || token[len(token)-1] == '\t') {
			token = token[:len(token)-1]
		}
		if token != "" {
			tokens = append(tokens, token)
		}
	}
	return tokens
}
