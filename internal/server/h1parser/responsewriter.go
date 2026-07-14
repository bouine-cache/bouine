package h1parser

import (
	"bufio"
	"net"
	"net/http"
	"strconv"
)

// connResponseWriter implements http.ResponseWriter by writing directly
// to a net.Conn. Used by the fall-through path to serve miss-path
// requests without handing the connection back to net/http.
//
// The writer buffers headers in an http.Header map until WriteHeader
// or the first Write call, then serializes the status line + headers
// to the connection in a single Write.
//
// The fall-through path closes the connection after the response, so
// Connection: close is always sent and Transfer-Encoding is stripped
// (the body is delimited by connection close if no Content-Length).
type connResponseWriter struct {
	conn      net.Conn
	header    http.Header
	status    int
	wroteHead bool
}

func newConnResponseWriter(conn net.Conn) *connResponseWriter {
	return &connResponseWriter{
		conn:   conn,
		header: make(http.Header, 8),
		status: 200,
	}
}

func (w *connResponseWriter) Header() http.Header {
	return w.header
}

func (w *connResponseWriter) WriteHeader(code int) {
	if w.wroteHead {
		return
	}
	w.wroteHead = true
	w.status = code

	// Strip Transfer-Encoding — we don't implement chunked framing.
	// The body is delimited by Content-Length (if set by the handler)
	// or by connection close.
	w.header.Del("Transfer-Encoding")

	// Force Connection: close — we close the connection after the
	// response and don't support keep-alive on the fall-through path.
	w.header.Set("Connection", "close")

	buf := make([]byte, 0, 4096)
	buf = append(buf, "HTTP/1.1 "...)
	buf = strconv.AppendInt(buf, int64(code), 10)
	buf = append(buf, ' ')
	buf = append(buf, http.StatusText(code)...)
	buf = append(buf, '\r', '\n')

	for k, vals := range w.header {
		for _, v := range vals {
			buf = append(buf, k...)
			buf = append(buf, ": "...)
			buf = append(buf, v...)
			buf = append(buf, '\r', '\n')
		}
	}
	buf = append(buf, '\r', '\n')
	_, _ = w.conn.Write(buf)
}

func (w *connResponseWriter) Write(b []byte) (int, error) {
	if !w.wroteHead {
		w.WriteHeader(w.status)
	}
	return w.conn.Write(b)
}

// Flush is a no-op — data is written directly to the connection with
// no buffering. Satisfies http.Flusher for handlers that call Flush.
func (w *connResponseWriter) Flush() {}

// flushAndClose ensures the response is fully written and then closes
// the write side of the connection. Called after ServeHTTP returns.
func (w *connResponseWriter) flushAndClose() {
	if !w.wroteHead {
		w.WriteHeader(w.status)
	}
	if tcp, ok := w.conn.(*net.TCPConn); ok {
		_ = tcp.CloseWrite()
	}
}

func (w *connResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	rw := bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(w.conn))
	return w.conn, rw, nil
}
