// Package responsewriter provides a shared http.ResponseWriter wrapper
// used by both the access-log and data-plane metrics middlewares.
// It captures status code and bytes written without allocating
// extra structures on the hot path.
package responsewriter

import (
	"net/http"
	"sync"
)

// ResponseWriter wraps an http.ResponseWriter and records the status
// code and total bytes written. Both fields default to zero-values;
// callers should treat status 0 as 200 unless WriteHeader was called.
//
// Stable.
type ResponseWriter struct {
	http.ResponseWriter
	Status int
	Bytes  int64
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
func (w *ResponseWriter) WriteHeader(code int) {
	w.Status = code
	w.ResponseWriter.WriteHeader(code)
}

// Write counts bytes and delegates to the underlying writer.
func (w *ResponseWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.Bytes += int64(n)
	return n, err
}
