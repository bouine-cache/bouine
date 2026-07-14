// Package h1parser implements a zero-allocation HTTP/1.1 request parser
// for the cache hit fast path. It parses request lines and headers from
// a net.Conn into a stack-allocated RawRequest struct, avoiding the
// *http.Request allocation that net/http imposes on every request.
//
// The parser handles keep-alive in a loop: parse → try fast path →
// serve or fall through. On fall-through (miss path), the connection
// is handed to net/http for the remainder of its lifetime.
//
// Unstable.
package h1parser

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"
	"unsafe"

	"github.com/bouine-cache/bouine/pkg/api"
)

// readBufferSize is the size of the pooled read buffer. 16 KB covers
// 99.9%+ of real-world HTTP/1.1 request headers. Requests exceeding
// this fall through to net/http.
const readBufferSize = 16 * 1024

// Parser parses HTTP/1.1 requests from a net.Conn and dispatches to
// the fast path or falls through to net/http.
type Parser struct {
	fastPath    api.FastPathHandler
	fallback    http.Handler
	nowFunc     func() time.Time
	idleRead    time.Duration
	writeTime   time.Duration
	scheme      string
	metricsHook func(method, route, cacheResult, source string, status, bytesOut int, duration time.Duration)
}

// New creates a Parser. fastPath may be nil — when nil, all requests
// fall through to the fallback handler. fallback must not be nil.
func New(fastPath api.FastPathHandler, fallback http.Handler, opts ...Option) *Parser {
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
// The callback receives the method, route, cache result, source,
// status code, bytes out, and request duration. Used by the engine
// to increment Prometheus counters and histograms without going
// through the middleware chain.
func WithMetricsHook(fn func(method, route, cacheResult, source string, status, bytesOut int, duration time.Duration)) Option {
	return func(p *Parser) { p.metricsHook = fn }
}

// ErrFallThrough signals that the parser cannot handle the connection
// and it should be handed to net/http.
var ErrFallThrough = errors.New("h1parser: fall through to net/http")

// Serve handles a single connection: parse HTTP/1.1 requests in a
// keep-alive loop, dispatching hits to the fast path and misses to
// the fallback handler. On any parse error or fall-through, the
// connection is yielded to net/http.
func (p *Parser) Serve(conn net.Conn) error {
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetKeepAlive(true)
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
			if req == nil {
				return ErrFallThrough
			}
			return p.handleFallThrough(conn, req, excess)
		}

		// Try the fast path.
		if p.fastPath != nil {
			now := p.nowFunc()
			start := now
			resp, hit := p.fastPath.TryHit(req, now)
			if hit && resp != nil {
				if err := p.serveHit(conn, resp); err != nil {
					return err
				}
				if p.metricsHook != nil {
					dur := p.nowFunc().Sub(start)
					p.metricsHook(req.Method, resp.Route, resp.CacheResult,
						resp.Source, resp.StatusCode, resp.BytesOut, dur)
				}
				continue
			}
		}

		// Miss path: construct *http.Request and fall through.
		return p.handleFallThrough(conn, req, excess)
	}
}

// parseRequest reads and parses a single HTTP/1.1 request from conn.
// Returns (req, fallThrough, err). When fallThrough is true, the
// caller should hand the connection to net/http.
func (p *Parser) parseRequest(conn net.Conn, readBuf *[readBufferSize]byte) (*api.RawRequest, bool, []byte, error) {
	buf := readBuf[:0]

	// Read until we find the end of headers (\r\n\r\n).
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
			// Headers exceed our buffer — fall through to net/http.
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

	// excess contains body bytes that were read past the header end.
	// These are returned to the caller so handleFallThrough can use
	// them as the start of the request body for non-GET/HEAD methods.
	var excess []byte
	if headerEnd >= 0 && headerEnd < len(buf) {
		excess = buf[headerEnd:]
	}
	return req, false, excess, nil
}

// findHeaderEnd searches for \r\n\r\n in buf.
func findHeaderEnd(buf []byte) int {
	for i := 0; i < len(buf)-3; i++ {
		if buf[i] == '\r' && buf[i+1] == '\n' && buf[i+2] == '\r' && buf[i+3] == '\n' {
			return i + 4
		}
	}
	return -1
}

// parseRequestLine parses the first line: "METHOD SP PATH SP VERSION\r\n".
func parseRequestLine(buf []byte, req *api.RawRequest) error {
	// Find end of request line.
	lineEnd := 0
	for lineEnd < len(buf) && buf[lineEnd] != '\r' {
		lineEnd++
	}
	if lineEnd >= len(buf)-1 || buf[lineEnd+1] != '\n' {
		return errors.New("h1parser: malformed request line")
	}
	line := buf[:lineEnd]

	// Parse method.
	sp1 := 0
	for sp1 < len(line) && line[sp1] != ' ' {
		sp1++
	}
	if sp1 == len(line) {
		return errors.New("h1parser: missing path")
	}
	req.Method = bytesToString(line[:sp1])

	// Parse path.
	sp2 := sp1 + 1
	for sp2 < len(line) && line[sp2] != ' ' {
		sp2++
	}
	if sp2 == len(line) {
		return errors.New("h1parser: missing version")
	}
	fullPath := bytesToString(line[sp1+1 : sp2])

	// Split path and query.
	if q := indexByte(fullPath, '?'); q >= 0 {
		req.Path = fullPath[:q]
		req.Query = fullPath[q+1:]
	} else {
		req.Path = fullPath
	}

	// Parse version.
	req.HTTPVersion = bytesToString(line[sp2+1:])

	return nil
}

// parseHeaders parses header lines from buf, starting after the request line.
func parseHeaders(buf []byte, req *api.RawRequest) error {
	// Skip past the request line.
	pos := skipRequestLine(buf)

	for pos < len(buf) {
		// Check for end of headers.
		if buf[pos] == '\r' && pos+1 < len(buf) && buf[pos+1] == '\n' {
			break
		}

		// Find end of this header line.
		lineEnd := pos
		for lineEnd < len(buf)-1 && (buf[lineEnd] != '\r' || buf[lineEnd+1] != '\n') {
			lineEnd++
		}
		if lineEnd >= len(buf)-1 {
			break
		}

		if req.NHeaders >= api.MaxRawHeaders {
			return errors.New("h1parser: too many headers")
		}

		appendHeader(req, buf[pos:lineEnd])

		pos = lineEnd + 2
	}

	// Extract Host header (case-insensitive, RFC 9110 §5.1).
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

// serveHit writes the fast path response to the connection and returns
// the pooled header buffer via the Return callback.
func (p *Parser) serveHit(conn net.Conn, resp *api.FastPathResponse) error {
	if err := conn.SetWriteDeadline(time.Now().Add(p.writeTime)); err != nil {
		return err
	}
	_, err := resp.Buffers.WriteTo(conn)
	if resp.Return != nil {
		resp.Return()
	}
	return err
}

// handleFallThrough serves a miss-path request via the fallback handler.
// It constructs an *http.Request from the parsed RawRequest and calls
// p.fallback.ServeHTTP with a connResponseWriter that writes directly
// to the connection. The parser does not loop back after a fall-through
// — the connection is closed after the response is written.
//
// excess contains body bytes that were already read from the connection
// by parseRequest (bytes past the \r\n\r\n header end). For non-GET/HEAD
// methods, these bytes plus the remaining connection data form the
// request body.
func (p *Parser) handleFallThrough(conn net.Conn, req *api.RawRequest, excess []byte) error {
	if req == nil {
		return ErrFallThrough
	}

	url := req.Path
	if req.Query != "" {
		url += "?" + req.Query
	}

	r, err := http.NewRequestWithContext( //nolint:noctx // context not available in fast-path fall-through
		context.Background(), req.Method, "http://"+req.Host+url, nil)
	if err != nil {
		return ErrFallThrough
	}

	// Set proto from the parsed version.
	if req.HTTPVersion == "HTTP/1.0" {
		r.Proto = "HTTP/1.0"
		r.ProtoMajor = 1
		r.ProtoMinor = 0
	} else {
		r.Proto = "HTTP/1.1"
		r.ProtoMajor = 1
		r.ProtoMinor = 1
	}

	for i := 0; i < req.NHeaders; i++ {
		r.Header.Add(req.Headers[i].Key, req.Headers[i].Value)
	}
	r.Host = req.Host
	r.RemoteAddr = conn.RemoteAddr().String()

	// Construct the request body. For GET/HEAD there is no body.
	// For other methods, the body is the excess bytes already read
	// by the parser, followed by any remaining bytes on the connection.
	// We use Content-Length to bound the reader; without it, we read
	// until EOF (which only works for Connection: close).
	contentLength := -1
	for i := 0; i < req.NHeaders; i++ {
		if api.EqualFold(req.Headers[i].Key, "Content-Length") {
			if cl, perr := strconv.Atoi(req.Headers[i].Value); perr == nil {
				contentLength = cl
			}
			break
		}
	}

	switch {
	case req.Method == http.MethodGet || req.Method == http.MethodHead:
		r.Body = http.NoBody
		r.ContentLength = 0
	case contentLength > 0:
		bodyReader := io.MultiReader(bytes.NewReader(excess), conn)
		r.Body = io.NopCloser(io.LimitReader(bodyReader, int64(contentLength)))
		r.ContentLength = int64(contentLength)
	case contentLength == 0:
		r.Body = http.NoBody
		r.ContentLength = 0
	default:
		// No Content-Length: read excess + connection until EOF.
		// This only works because we close the connection after
		// the response (Connection: close semantics).
		if len(excess) > 0 {
			r.Body = io.NopCloser(io.MultiReader(bytes.NewReader(excess), conn))
		} else {
			r.Body = io.NopCloser(conn)
		}
		r.ContentLength = -1
	}

	w := newConnResponseWriter(conn)
	p.fallback.ServeHTTP(w, r)
	w.flushAndClose()
	return nil
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
// is not modified or garbage-collected. Used in the h1parser where
// all string fields are slices of the read buffer and have a well-
// understood lifetime (valid until the next request on the same
// connection).
// the read buffer is not reused, enforced by the parser lifecycle.
//
//nolint:gosec // unsafe.String is safe: the string is valid only while
func bytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}
