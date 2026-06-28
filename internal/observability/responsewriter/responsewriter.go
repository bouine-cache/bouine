// Package responsewriter provides a shared http.ResponseWriter wrapper
// used by both the access-log and data-plane metrics middlewares.
// It captures status code and bytes written without allocating
// extra structures on the hot path.
package responsewriter

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
)

var (
	// Compile-time guards: removing any method below breaks the build
	// instead of silently regressing interface satisfaction (see issue #73).
	_ http.ResponseWriter = (*ResponseWriter)(nil)
	_ http.Flusher        = (*ResponseWriter)(nil)
	_ http.Hijacker       = (*ResponseWriter)(nil)
	_ io.ReaderFrom       = (*ResponseWriter)(nil)
)

// ErrNotSupported is returned when the underlying http.ResponseWriter
// does not implement the requested optional interface.
var ErrNotSupported = errors.New("responsewriter: underlying ResponseWriter does not support this operation")

// ResponseWriter wraps an http.ResponseWriter and records the status
// code and total bytes written. Both fields default to zero-values;
// callers should treat status 0 as 200 unless WriteHeader was called.
//
// Stable.
type ResponseWriter struct {
	http.ResponseWriter
	Status int
	Bytes  int64
	// headerWritten records whether WriteHeader has been called, mirroring
	// net/http which ignores the second and subsequent calls.
	headerWritten bool
}

// pool reuses ResponseWriter instances to avoid a heap allocation on
// every request. The wrapper is reset in Acquire; callers must call
// Release after the underlying handler has returned.
var pool = sync.Pool{
	New: func() any { return new(ResponseWriter) },
}

// Acquire returns a ResponseWriter wrapping w from the pool.
// The caller MUST call Release when done (after next.ServeHTTP returns).
func Acquire(w http.ResponseWriter) *ResponseWriter {
	rw := pool.Get().(*ResponseWriter)
	rw.ResponseWriter = w
	rw.Status = 200
	rw.Bytes = 0
	rw.headerWritten = false
	return rw
}

// Release returns rw to the pool. rw must not be used after this call.
func Release(rw *ResponseWriter) {
	rw.ResponseWriter = nil // drop reference to underlying writer
	pool.Put(rw)
}

// New wraps w and pre-sets Status to 200 (the implicit default).
//
// Deprecated: prefer Acquire + Release to avoid a per-request allocation.
func New(w http.ResponseWriter) *ResponseWriter {
	return &ResponseWriter{ResponseWriter: w, Status: 200}
}

// WriteHeader records the status and delegates to the underlying writer.
// Only the first call records the status, mirroring net/http which
// ignores subsequent WriteHeader calls.
func (w *ResponseWriter) WriteHeader(code int) {
	if w.headerWritten {
		return
	}
	w.headerWritten = true
	w.Status = code
	w.ResponseWriter.WriteHeader(code)
}

// Write counts bytes and delegates to the underlying writer. The first
// Write triggers an implicit WriteHeader(200) in net/http, so we mark
// the header as written to keep subsequent explicit WriteHeader calls
// from overwriting the recorded status.
func (w *ResponseWriter) Write(b []byte) (int, error) {
	if !w.headerWritten {
		w.headerWritten = true
	}
	n, err := w.ResponseWriter.Write(b)
	w.Bytes += int64(n)
	return n, err
}

// Flush delegates to the underlying writer when it implements http.Flusher.
func (w *ResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack delegates to the underlying writer when it implements http.Hijacker.
func (w *ResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, ErrNotSupported
	}
	return h.Hijack()
}

// ReadFrom delegates to the underlying writer when it implements
// io.ReaderFrom so that zero-copy fast paths (e.g. sendfile) are preserved.
// Bytes written are counted after the copy completes.
func (w *ResponseWriter) ReadFrom(src io.Reader) (int64, error) {
	if rf, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		n, err := rf.ReadFrom(src)
		w.Bytes += n
		return n, err
	}
	return io.Copy(struct{ io.Writer }{w}, src)
}
