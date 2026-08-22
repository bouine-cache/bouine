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
	"bytes"
	"errors"
	"io"
	"net"
	"time"
	"unsafe"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// readBufferSize is the size of the pooled read buffer. 16 KB covers
// 99.9%+ of real-world HTTP/1.1 request headers. Requests exceeding
// this fall through to the fallback handler.
const readBufferSize = 16 * 1024

// Parser parses HTTP/1.1 requests from a net.Conn and dispatches to
// the fast path or falls through to the fasthttp.RequestHandler.
type Parser struct {
	fastPath      api.FastPathHandler
	fallback      fasthttp.RequestHandler
	nowFunc       func() time.Time
	idleRead      time.Duration
	writeTime     time.Duration
	scheme        string
	metricsHook   func(method, route, cacheResult, source string, status, bytesOut int, duration time.Duration)
	smugglingHook func()
}

// New creates a Parser. fastPath may be nil — when nil, all requests
// fall through to the fallback handler. fallback must not be nil.
func New(fastPath api.FastPathHandler, fallback fasthttp.RequestHandler, opts ...Option) *Parser {
	p := &Parser{
		fastPath:  fastPath,
		fallback:  fallback,
		nowFunc:   time.Now,
		idleRead:  10 * time.Second,
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
func WithMetricsHook(fn func(method, route, cacheResult, source string, status, bytesOut int, duration time.Duration)) Option {
	return func(p *Parser) { p.metricsHook = fn }
}

// WithSmugglingHook sets a callback invoked when the parser detects an
// HTTP smuggling attempt.
func WithSmugglingHook(fn func()) Option {
	return func(p *Parser) { p.smugglingHook = fn }
}

// Serve handles a single connection: parse HTTP/1.1 requests in a
// keep-alive loop, dispatching hits to the fast path and misses to
// the fallback handler.
func (p *Parser) Serve(conn net.Conn) error {
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetNoDelay(true) // Critical: prevents Nagle's 40ms delay on small hit responses.
	}

	var readBuf [readBufferSize]byte

	for {
		if err := conn.SetReadDeadline(time.Now().Add(p.idleRead)); err != nil {
			return err
		}

		req, fallThrough, excess, err := p.parseRequest(conn, &readBuf)
		if err != nil {
			return err
		}
		if fallThrough {
			return p.handleFallThrough(conn, req, excess)
		}

		// Try the fast path.
		if p.fastPath != nil {
			now := p.nowFunc()
			resp, hit := p.fastPath.TryHit(req, now)
			if hit && resp != nil {
				if err := p.serveHit(conn, resp, now); err != nil {
					p.fastPath.Release(resp)
					return err
				}
				if p.metricsHook != nil {
					dur := p.nowFunc().Sub(now)
					p.metricsHook(req.Method, resp.Route, resp.CacheResult,
						resp.Source, resp.StatusCode, resp.BytesOut, dur)
				}
				p.fastPath.Release(resp)
				continue
			}
		}

		// Miss path: call the fallback handler with a fasthttp.RequestCtx.
		return p.handleFallThrough(conn, req, excess)
	}
}

// parseRequest reads and parses a single HTTP/1.1 request from conn.
func (p *Parser) parseRequest(conn net.Conn, readBuf *[readBufferSize]byte) (*api.RawRequest, bool, []byte, error) {
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
			return nil, true, nil, nil
		}
	}

	req := &api.RawRequest{Scheme: p.scheme}
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

// findHeaderEnd searches for \r\n\r\n in buf.
func findHeaderEnd(buf []byte) int {
	idx := bytes.Index(buf, []byte("\r\n\r\n"))
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
	req.Method = bytesToString(line[:sp1])

	sp2 := sp1 + 1
	for sp2 < len(line) && line[sp2] != ' ' {
		sp2++
	}
	if sp2 == len(line) {
		return errors.New("h1parser: missing version")
	}
	fullPath := bytesToString(line[sp1+1 : sp2])

	if q := indexByte(fullPath, '?'); q >= 0 {
		req.Path = fullPath[:q]
		req.Query = fullPath[q+1:]
	} else {
		req.Path = fullPath
	}

	req.HTTPVersion = bytesToString(line[sp2+1:])

	return nil
}

// parseHeaders parses header lines from buf, starting after the request line.
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

	for i := 0; i < req.NHeaders; i++ {
		if api.EqualFold(req.Headers[i].Key, "Host") {
			req.Host = req.Headers[i].Value
			break
		}
	}

	return nil
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
// RFC 9110 §6.6.2 and AGENTS.md §6.
func smugglingDetected(req *api.RawRequest) bool {
	var hasCL, hasTE bool
	var clCount int
	for i := 0; i < req.NHeaders; i++ {
		h := &req.Headers[i]
		if api.EqualFold(h.Key, header.ContentLength) {
			clCount++
			hasCL = true
		}
		if api.EqualFold(h.Key, header.TransferEncoding) {
			hasTE = true
		}
	}
	if hasCL && hasTE {
		return true
	}
	if clCount > 1 {
		return true
	}
	return false
}

// appendHeader parses a single header line and appends it to req.
func appendHeader(req *api.RawRequest, line []byte) {
	colon := 0
	for colon < len(line) && line[colon] != ':' {
		colon++
	}
	if colon == len(line) {
		return
	}

	key := bytesToString(line[:colon])
	valStart := colon + 1
	for valStart < len(line) && (line[valStart] == ' ' || line[valStart] == '\t') {
		valStart++
	}
	value := bytesToString(line[valStart:])

	req.Headers[req.NHeaders] = api.RawHeader{
		Key:   key,
		Value: value,
	}
	req.NHeaders++
}

// serveHit writes the fast path response to the connection via
// net.Buffers.WriteTo (single writev syscall). The caller is
// responsible for calling Release on resp after this returns.
func (p *Parser) serveHit(conn net.Conn, resp *api.FastPathResponse, now time.Time) error {
	if err := conn.SetWriteDeadline(now.Add(p.writeTime)); err != nil {
		return err
	}
	_, err := resp.Buffers.WriteTo(conn)
	return err
}

// handleFallThrough serves a miss-path request via the fallback
// fasthttp.RequestHandler. It constructs a *fasthttp.RequestCtx from
// the parsed RawRequest, calls the handler, and writes the response
// to the connection.
func (p *Parser) handleFallThrough(conn net.Conn, req *api.RawRequest, excess []byte) error {
	if req == nil {
		return errors.New("h1parser: nil request on fall-through")
	}

	// Reset deadlines so the fallback handler manages its own timeouts.
	_ = conn.SetReadDeadline(time.Time{})
	_ = conn.SetWriteDeadline(time.Time{})

	// Construct a fasthttp.RequestCtx from the parsed request.
	var ctx fasthttp.RequestCtx
	ctx.Init2(conn, nil, false)

	// Populate the request from the parsed RawRequest.
	r := &ctx.Request
	r.Header.SetMethod(req.Method)
	uri := req.Path
	if req.Query != "" {
		uri += "?" + req.Query
	}
	r.SetRequestURI(uri)
	r.Header.SetHost(req.Host)
	for i := 0; i < req.NHeaders; i++ {
		r.Header.SetBytesKV(
			[]byte(req.Headers[i].Key),
			[]byte(req.Headers[i].Value),
		)
	}
	if len(excess) > 0 {
		r.SetBodyRaw(excess)
	}

	// Call the fallback handler.
	p.fallback(&ctx)

	// Write the response to the connection.
	if err := conn.SetWriteDeadline(time.Now().Add(p.writeTime)); err != nil {
		return err
	}
	_, err := ctx.Response.WriteTo(conn)
	return err
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

// bytesToString converts a []byte to string without copying. The
// returned string is only valid as long as the underlying byte slice
// is not modified or garbage-collected.
//
//nolint:gosec // unsafe.String is safe: the string is valid only while
func bytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}
