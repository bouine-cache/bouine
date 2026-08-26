// Package h1parser implements a zero-allocation HTTP/1.1 request parser
// for the cache hit fast path. It parses request lines and headers from
// a net.Conn into a pooled RawRequest struct, avoiding the
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
	"strings"
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

// rawRequestPool pools *api.RawRequest to eliminate the per-hit heap
// allocation. RawRequest contains a fixed-size [100]RawHeader array
// (~1.6 KB); without pooling this escapes to the heap because
// parseRequest returns the pointer. Pooling reduces this to zero
// amortized allocations after warm-up.
var rawRequestPool = sync.Pool{
	New: func() any {
		return &api.RawRequest{}
	},
}

// getRawRequest returns a reset *RawRequest from the pool.
func (p *Parser) getRawRequest() *api.RawRequest {
	req := rawRequestPool.Get().(*api.RawRequest)
	req.NHeaders = 0
	req.Scheme = p.scheme
	req.Method = ""
	req.Path = ""
	req.Query = ""
	req.Host = ""
	req.HTTPVersion = ""
	return req
}

// putRawRequest resets and returns a *RawRequest to the pool.
// No-op if req is nil.
func putRawRequest(req *api.RawRequest) {
	if req == nil {
		return
	}
	req.NHeaders = 0
	req.Method = ""
	req.Path = ""
	req.Query = ""
	req.Host = ""
	req.Scheme = ""
	req.HTTPVersion = ""
	rawRequestPool.Put(req)
}

// Parser parses HTTP/1.1 requests from a net.Conn and dispatches to
// the fast path or falls through to the fasthttp.RequestHandler.
type Parser struct {
	fastPath      api.FastPathHandler
	fallback      fasthttp.RequestHandler
	nowFunc       func() time.Time
	metricsHook   func(method, route, cacheResult, source string, status, bytesOut int, duration time.Duration)
	smugglingHook func()
	scheme        string
	idleRead      time.Duration
	writeTime     time.Duration
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
// the fallback handler. The connection stays alive across both hits
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

	// Set the initial read deadline once. The deadline is refreshed
	// lazily: only when the remaining time drops below the refresh
	// threshold, avoiding a setsockopt syscall on every request.
	deadline := p.nowFunc().Add(p.idleRead)
	if err := conn.SetReadDeadline(deadline); err != nil {
		return err
	}
	const refreshThreshold = 2 * time.Second

	for {
		// Refresh the deadline only when the remaining window is too
		// small to read another request. This reduces setsockopt syscalls
		// from one-per-request to roughly one per refreshThreshold interval.
		now := p.nowFunc()
		if remaining := deadline.Sub(now); remaining < refreshThreshold {
			deadline = now.Add(p.idleRead)
			if err := conn.SetReadDeadline(deadline); err != nil {
				return err
			}
		}

		req, fallThrough, excess, err := p.parseRequest(conn, &readBuf)
		if err != nil {
			return err
		}
		if fallThrough {
			close, ftErr := p.handleFallThrough(conn, req, excess)
			putRawRequest(req)
			if ftErr != nil {
				return ftErr
			}
			if close {
				return nil
			}
			// handleFallThrough cleared the read deadline so the fallback
			// handler could manage its own timeouts. Re-arm it now for the
			// next keep-alive request, otherwise the connection is
			// unprotected against slowloris.
			deadline = p.nowFunc().Add(p.idleRead)
			if err := conn.SetReadDeadline(deadline); err != nil {
				return err
			}
			continue
		}

		// Try the fast path.
		if p.fastPath != nil {
			now := p.nowFunc()
			resp, hit := p.fastPath.TryHit(req, now)
			if hit && resp != nil {
				if err := p.serveHit(conn, resp, now); err != nil {
					p.fastPath.Release(resp)
					putRawRequest(req)
					return err
				}
				if p.metricsHook != nil {
					dur := p.nowFunc().Sub(now)
					p.metricsHook(req.Method, resp.Route, resp.CacheResult,
						resp.Source, resp.StatusCode, resp.BytesOut, dur)
				}
				p.fastPath.Release(resp)
				putRawRequest(req)
				continue
			}
		}

		// Miss path: call the fallback handler with a fasthttp.RequestCtx.
		close, ftErr := p.handleFallThrough(conn, req, excess)
		putRawRequest(req)
		if ftErr != nil {
			return ftErr
		}
		if close {
			return nil
		}
		// Re-arm the read deadline for the next keep-alive request.
		deadline = p.nowFunc().Add(p.idleRead)
		if err := conn.SetReadDeadline(deadline); err != nil {
			return err
		}
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

	req := p.getRawRequest()
	if err := parseRequestLine(buf, req); err != nil {
		putRawRequest(req)
		return nil, true, nil, err
	}
	if err := parseHeaders(buf, req); err != nil {
		putRawRequest(req)
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

// headerEndBytes is the HTTP/1.1 header terminator. Package-level
// to avoid allocating a 4-byte slice on every findHeaderEnd call.
var headerEndBytes = []byte("\r\n\r\n")

// findHeaderEnd searches for \r\n\r\n in buf.
func findHeaderEnd(buf []byte) int {
	idx := bytes.Index(buf, headerEndBytes)
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

	if q := strings.IndexByte(fullPath, '?'); q >= 0 {
		req.Path = fullPath[:q]
		req.Query = fullPath[q+1:]
	} else {
		req.Path = fullPath
	}

	req.HTTPVersion = header.BytesToString(line[sp2+1:])

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
//
// Returns (close, err). When close is true the caller should return
// from the keep-alive loop — the client requested Connection: close
// or the response indicates the connection should be closed.
func (p *Parser) handleFallThrough(conn net.Conn, req *api.RawRequest, excess []byte) (bool, error) {
	if req == nil {
		return false, errors.New("h1parser: nil request on fall-through")
	}

	// Check if the client requested Connection: close.
	clientClose := isConnectionClose(req)

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

	// Propagate Connection: close from the request to the response so
	// the client knows the connection will not be reused. The fasthttp
	// server's own serve loop does this automatically (server.go:2653),
	// but handleFallThrough bypasses that loop.
	if clientClose {
		ctx.Response.Header.SetConnectionClose()
	}

	// Write the response to the connection.
	if err := conn.SetWriteDeadline(p.nowFunc().Add(p.writeTime)); err != nil {
		return false, err
	}
	if _, err := ctx.Response.WriteTo(conn); err != nil {
		return false, err
	}

	// If the handler itself set Connection: close (e.g. via
	// ctx.SetConnectionClose), honour that too.
	return clientClose || ctx.Response.Header.ConnectionClose(), nil
}

// isConnectionClose reports whether the request contains a
// Connection: close token (RFC 9110 §7.6.1). The Connection header
// is a comma-separated list of tokens; "close" may appear alongside
// other tokens like "keep-alive". This determines whether the
// keep-alive loop should terminate after serving the response.
func isConnectionClose(req *api.RawRequest) bool {
	val := req.Header(header.Connection)
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

// splitHeaderTokens splits a comma-separated header value into trimmed
// tokens. It does not allocate — it returns subslices of the input.
func splitHeaderTokens(val string) []string {
	var tokens []string
	for len(val) > 0 {
		comma := strings.IndexByte(val, ',')
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
