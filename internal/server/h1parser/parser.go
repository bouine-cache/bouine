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
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
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
	fastPath       api.FastPathHandler
	fallback       http.Handler
	fallbackServer *http.Server
	nowFunc        func() time.Time
	idleRead       time.Duration
	writeTime      time.Duration
	scheme         string
	metricsHook    func(method, route, cacheResult, source string, status, bytesOut int, duration time.Duration)
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
	// Use the provided fallback server, or create a minimal one.
	if p.fallbackServer == nil {
		p.fallbackServer = &http.Server{
			Handler:           p.fallback,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      5 * time.Minute,
			IdleTimeout:       120 * time.Second,
		}
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

// WithFallbackServer sets the *http.Server used for fall-through
// connections. When provided, the parser uses this server's Serve
// method and inherits all its timeout and connection configuration.
// When nil (default), the parser creates a minimal server with
// ReadHeaderTimeout=10s.
func WithFallbackServer(srv *http.Server) Option {
	return func(p *Parser) { p.fallbackServer = srv }
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

// findHeaderEnd searches for \r\n\r\n in buf using bytes.Index for the
// optimized SIMD/Boyer-Moore implementation in the standard library.
func findHeaderEnd(buf []byte) int {
	idx := bytes.Index(buf, []byte("\r\n\r\n"))
	if idx < 0 {
		return -1
	}
	return idx + 4
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

// serveHit writes the fast path response to the connection. The caller
// is responsible for calling Release on resp after this returns.
func (p *Parser) serveHit(conn net.Conn, resp *api.FastPathResponse, now time.Time) error {
	if err := conn.SetWriteDeadline(now.Add(p.writeTime)); err != nil {
		return err
	}
	_, err := resp.Buffers.WriteTo(conn)
	return err
}

// handleFallThrough serves a miss-path request via the fallback handler.
// It reconstructs the raw request bytes and hands the connection to net/http
// via a singleConnListener, which uses http.Server's built-in response
// writer. This ensures correct HTTP framing (chunked encoding, content-length,
// flushing) that the custom connResponseWriter failed to handle, causing
// unexpected EOF errors under load.
//
// excess contains body bytes that were already read from the connection
// by parseRequest (bytes past the header end). These are prepended
// to the connection via a prefixedConn so net/http sees the complete request.
func (p *Parser) handleFallThrough(conn net.Conn, req *api.RawRequest, excess []byte) error {
	if req == nil {
		return ErrFallThrough
	}

	// Reconstruct the raw request bytes that net/http will parse.
	rawReq := reconstructRawRequest(req)

	// Prepend the raw request + any excess body bytes to the connection.
	prefix := make([]byte, 0, len(rawReq)+len(excess))
	prefix = append(prefix, rawReq...)
	prefix = append(prefix, excess...)
	// Reset deadlines so http.Server manages its own timeouts.
	_ = conn.SetReadDeadline(time.Time{})
	_ = conn.SetWriteDeadline(time.Time{})
	pc := &prefixedConn{Conn: conn, prefix: prefix}

	// Hand the connection to net/http via a one-shot listener.
	// This uses http.Server's proper response writer which correctly
	// handles chunked encoding, content-length, and connection lifecycle.
	//
	// closeNotifyConn signals when http.Server closes the connection so
	// that singleConnListener.Accept can block until the handler goroutine
	// is done. Without this, Serve returns immediately after spawning the
	// handler goroutine, and the caller closes the connection out from
	// under the handler — truncating the response.
	notifyConn := newCloseNotifyConn(pc)
	cl := &singleConnListener{conn: notifyConn, ready: notifyConn.done}
	if err := p.fallbackServer.Serve(cl); err != nil &&
		!errors.Is(err, http.ErrServerClosed) &&
		!errors.Is(err, net.ErrClosed) {
		return err
	}
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

// reconstructRawRequest rebuilds the raw HTTP/1.1 request bytes from a
// parsed RawRequest so net/http can re-parse them. This is needed because
// the h1parser already consumed the request bytes from the connection,
// and net/http's Server.Serve expects to read the request from the wire.
func reconstructRawRequest(req *api.RawRequest) []byte {
	var buf bytes.Buffer
	buf.Grow(256 + req.NHeaders*64)

	// Request line.
	buf.WriteString(req.Method)
	buf.WriteByte(' ')
	buf.WriteString(req.Path)
	if req.Query != "" {
		buf.WriteByte('?')
		buf.WriteString(req.Query)
	}
	buf.WriteByte(' ')
	buf.WriteString(req.HTTPVersion)
	buf.WriteString("\r\n")

	// Headers.
	for i := 0; i < req.NHeaders; i++ {
		buf.WriteString(req.Headers[i].Key)
		buf.WriteString(": ")
		buf.WriteString(req.Headers[i].Value)
		buf.WriteString("\r\n")
	}
	buf.WriteString("\r\n")

	return buf.Bytes()
}

// prefixedConn wraps a net.Conn and serves a prefix buffer before reading
// from the underlying connection. Used to prepend already-parsed request
// bytes when handing a connection to net/http.
type prefixedConn struct {
	net.Conn
	prefix []byte
}

func (p *prefixedConn) Read(b []byte) (int, error) {
	if len(p.prefix) > 0 {
		n := copy(b, p.prefix)
		p.prefix = p.prefix[n:]
		return n, nil
	}
	return p.Conn.Read(b)
}

// closeNotifyConn wraps a net.Conn and closes a channel when Close is
// called, so the singleConnListener can block until the handler goroutine
// is done with the connection.
type closeNotifyConn struct {
	net.Conn
	done chan struct{}
	once sync.Once
}

func newCloseNotifyConn(c net.Conn) *closeNotifyConn {
	return &closeNotifyConn{Conn: c, done: make(chan struct{})}
}

func (c *closeNotifyConn) Close() error {
	c.once.Do(func() { close(c.done) })
	return c.Conn.Close()
}

// singleConnListener returns one pre-accepted connection, then blocks
// until that connection is closed before returning ErrClosed on the
// next Accept. This ensures http.Server.Serve does not return until the
// handler goroutine has finished writing the response, preventing the
// caller from closing the connection mid-response.
//
// Duplicated from server.fp_conn.go to avoid a circular dependency;
// keep both copies in sync.
type singleConnListener struct {
	conn  net.Conn
	ready <-chan struct{}
	done  bool
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	if l.done {
		<-l.ready
		return nil, net.ErrClosed
	}
	l.done = true
	return l.conn, nil
}

func (l *singleConnListener) Close() error   { return nil }
func (l *singleConnListener) Addr() net.Addr { return l.conn.LocalAddr() }
