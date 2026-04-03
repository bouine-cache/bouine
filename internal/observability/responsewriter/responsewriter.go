// Package responsewriter provides a shared http.ResponseWriter wrapper
// used by both the access-log and data-plane metrics middlewares.
// It captures status code and bytes written without allocating
// extra structures on the hot path.
package responsewriter

import "net/http"

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

// New wraps w and pre-sets Status to 200 (the implicit default).
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
