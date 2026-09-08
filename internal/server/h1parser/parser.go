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

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
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
	metricsHook   func(pool, cacheResult, source string, status, bytesOut int, duration time.Duration)
	smugglingHook func()
	// metricsRing, when non-nil, redirects the reactor's hit metrics
	// through the async SPSC ring (reactor_metrics.go). Set by the
	// reactor transport (newReactorLoop) only — the blocking path
	// always calls metricsHook directly, and tests that want sync
	// observation leave it nil.
	metricsRing *metricsRing
	scheme      string
	idleRead    time.Duration
	writeTime   time.Duration
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
func WithMetricsHook(fn func(pool, cacheResult, source string, status, bytesOut int, duration time.Duration)) Option {
	return func(p *Parser) { p.metricsHook = fn }
}

// WithSmugglingHook sets a callback invoked when the parser detects an
// HTTP smuggling attempt.
func WithSmugglingHook(fn func()) Option {
	return func(p *Parser) { p.smugglingHook = fn }
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
		if fallThrough {
			res, err := p.serveFallThroughRequest(conn, req, excess, &deadline, &writeDeadline)
			if res == serveClose || err != nil {
				return err
			}
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
				continue
			}
		}

		// Miss path: call the fallback handler with a fasthttp.RequestCtx.
		if res, err := p.serveFastMiss(conn, req, excess, &deadline, &writeDeadline); res == serveClose || err != nil {
			return err
		}
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
		p.metricsHook(resp.Pool, resp.CacheResult,
			resp.Source, resp.StatusCode, resp.BytesOut, dur)
	}
	closeConn := resp.CloseConn
	p.fastPath.Release(resp)
	if closeConn {
		// The request asked for Connection: close (RFC 9110 §9.6) and
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

// parseBuffer parses one request from a fully-read header block. It is
// the parser half of parseRequest, shared with the reactor: the reactor
// accumulates bytes itself (raw non-blocking reads) and calls this once
// the header terminator is present. headerEnd is the index just past
// the \r\n\r\n terminator, or -1 when the buffer ended at EOF without
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

// findHeaderEnd searches for the \r\n\r\n terminator in buf.
func findHeaderEnd(buf []byte) int {
	idx := bytes.Index(buf, []byte("\r\n\r\n"))
	if idx < 0 {
		return -1
	}
	return idx + 4
}

// headerTermOverlap is how far before the current buffer end a
// terminator search must resume after a miss: a terminator that began
// in the last len-1 bytes of the previous segment could still
// complete in the next one. Searching from rLen-overlap is O(1)
// resumption instead of rescanning consumed bytes per read.
const headerTermOverlap = len("\r\n\r\n") - 1

// findHeaderEndFrom searches for the \r\n\r\n terminator in buf,
// starting at byte offset from. Callers that have already searched
// buf[:from] pass the previously searched length so multi-segment
// accumulation stays O(n) overall instead of O(n²) rescan per read.
func findHeaderEndFrom(buf []byte, from int) int {
	if from < len(buf)-1 {
		if idx := bytes.Index(buf[from:], []byte("\r\n\r\n")); idx >= 0 {
			return from + idx + 4
		}
		return -1
	}
	return findHeaderEnd(buf)
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
// path (the fused scan, W2 of docs/plans/h1-reactor-perf-round-4.md).
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

	// Connection: close detection (RFC 9110 §7.6.1/§9.6): the request
	// asked to terminate the connection after the response. The flag is
	// read by the fast path (Connection trailer + CloseConn) and by
	// Serve to leave its keep-alive loop after a hit. appendHeader sets
	// the flag when any Connection header carries a close token (the
	// !ConnectionClose guard skips already-decided requests), so it
	// needs no reset here; a request with no Connection header at all
	// leaves the soft reset's false in place.
	return nil
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

// handleFallThrough serves a miss-path request via the fallback
// fasthttp.RequestHandler. Instead of copying pre-buffered bytes into the
// ctx and truncating bodies that span multiple TCP reads, it replays the
// buffered request bytes through a prefixConn so fasthttp's own parser
// re-reads the request with full body framing (Content-Length, chunked,
// trailers, Expect: 100-continue) from the live socket. This costs one
// small allocation on the miss path only — the hit path is untouched.
//
// Returns (close, err). When close is true the caller should return
// from the keep-alive loop — the client requested Connection: close
// or the response indicates the connection should be closed.
func (p *Parser) handleFallThrough(conn net.Conn, req *api.RawRequest, excess []byte) (bool, error) {
	if req == nil {
		return false, errors.New("h1parser: nil request on fall-through")
	}

	// Rebuild the wire bytes of the request head so fasthttp's parser can
	// re-read it: method, path, version, headers, terminator. The bytes
	// are rebuilt into a fresh buffer (miss path only) because readBuf is
	// reused by the next request on this connection after the handler
	// returns. Host is re-emitted first so the fallback sees it even if
	// the original header block lacked one.
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

	// Check if the client requested Connection: close.
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
	br := bufio.NewReaderSize(rc, readBufferSize)
	if err := ctx.Request.Read(br); err != nil {
		// Malformed input beyond what the fast parser rejected — the
		// connection state is now indeterminate; close it without
		// surfacing a read error (Serve treats all read failures as
		// connection termination, not listener failure).
		return true, nil //nolint:nilerr // close-connection outcome, not an error to propagate
	}
	if ctx.Request.MayContinue() {
		// Mirror fasthttp's serve loop (server.go:2546): send 100 Continue
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

	// Call the fallback handler.
	p.fallback(&ctx)

	// Propagate Connection: close from the request to the response so
	// the client knows the connection will not be reused. The fasthttp
	// server's own serve loop does this automatically (server.go:2653),
	// but handleFallThrough bypasses that loop.
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
	// "small read buffer".
	br := bufio.NewReaderSize(rc, 64*1024)
	if err := ctx.Request.Read(br); err != nil {
		_ = writeBadRequest(conn)
		return
	}

	p.fallback(&ctx)
	ctx.Response.Header.SetConnectionClose()

	// Streamed responses re-arm the write deadline per Write (see
	// handleFallThrough); this connection always closes after the
	// response either way (the fallback owned the read state).
	writeConn := conn
	if ctx.Response.IsBodyStream() {
		writeConn = &idleWriteConn{Conn: conn, budget: p.writeTime}
	}
	_ = conn.SetWriteDeadline(p.nowFunc().Add(p.writeTime))
	_, _ = ctx.Response.WriteTo(writeConn)
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
