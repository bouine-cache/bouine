// Package accesslog provides a structured access logger for the data
// plane. Each request is logged as a JSON slog record with cache result,
// duration, status code, upstream pool, and route label.
package accesslog

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/internal/observability/responsewriter"
)

// Middleware wraps an http.Handler and emits a structured access log
// line for sampled requests. Only 200-OK responses are sampled at
// 1:100; every other status code (redirects, errors, and non-200 2xx
// such as 204) is always logged, keeping the log write off the
// critical path at high cache-hit RPS.
//
// Stable.
func Middleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := responsewriter.Acquire(w)
		defer responsewriter.Release(sw)

		next.ServeHTTP(sw, r)

		if observability.ShouldLogAccess(sw.Status) {
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
		}
	})
}
