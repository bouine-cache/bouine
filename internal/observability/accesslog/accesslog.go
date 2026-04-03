// Package accesslog provides a structured access logger for the data
// plane. Each request passing through the pipeline is logged as a
// JSON slog record with the fields listed in PLAN.md §10.
package accesslog

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/thylong/bouine/internal/observability/responsewriter"
)

// Middleware wraps an http.Handler and emits a structured access log
// line for every request. It captures method, host, path, status,
// duration, and bytes written.
//
// Stable.
func Middleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := responsewriter.New(w)

		next.ServeHTTP(sw, r)

		logger.Info("access",
			"method", r.Method,
			"host", r.Host,
			"path", r.URL.Path,
			"proto", r.Proto,
			"status", sw.Status,
			"bytes_out", sw.Bytes,
			"dur_ms", time.Since(start).Milliseconds(),
			"remote", r.RemoteAddr,
			"cache_status", sw.Header().Get("X-Cache"),
		)
	})
}
