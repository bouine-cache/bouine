// Package accesslog provides a structured access logger for the data
// plane. Each request passing through the pipeline is logged as a
// JSON slog record with the fields listed in PLAN.md §10.
package accesslog

import (
	"log/slog"
	"net/http"
	"time"
)

// Middleware wraps an http.Handler and emits a structured access log
// line for every request. It captures method, host, path, status,
// duration, and bytes written.
//
// Stable.
func Middleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: 200}

		next.ServeHTTP(sw, r)

		logger.Info("access",
			"method", r.Method,
			"host", r.Host,
			"path", r.URL.Path,
			"proto", r.Proto,
			"status", sw.status,
			"bytes_out", sw.bytes,
			"dur_ms", time.Since(start).Milliseconds(),
			"remote", r.RemoteAddr,
		)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, err
}
