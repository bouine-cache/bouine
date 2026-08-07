// Package accesslog provides a structured access logger for the data
// plane. Each request is logged as a JSON slog record with cache result,
// duration, status code, upstream pool, and route label.
package accesslog

import (
	"net/http"
	"time"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/observability/responsewriter"
	"github.com/bouine-cache/bouine/pkg/header"
)

// Middleware wraps an http.Handler and emits a structured access log
// line for sampled requests. Non-200 responses are always logged at
// Warn level (never sampled). 200-OK responses with a cache key are
// sampled deterministically by key via the SampledLogger; 200-OK
// responses without a key are sampled by counter fallback.
//
// Stable.
func Middleware(logger observability.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := responsewriter.Acquire(w)
		defer responsewriter.Release(sw)

		next.ServeHTTP(sw, r)

		cacheResult := observability.HeaderVal(sw.Header(), header.XCache)
		msg := requestMessage(cacheResult, sw.Status)
		attrs := []any{
			"method", r.Method,
			"host", r.Host,
			"path", r.URL.Path,
			"proto", r.Proto,
			"status", sw.Status,
			"bytes_out", sw.Bytes,
			"dur_ms", time.Since(start).Milliseconds(),
			"remote", r.RemoteAddr,
			"cache_status", cacheResult,
		}
		if !sw.Key.IsZero() {
			attrs = append(attrs, "key", sw.Key)
		}

		if sw.Status != http.StatusOK {
			logger.Warn(msg, attrs...)
		} else {
			logger.Info(msg, attrs...)
		}
	})
}

// requestMessage returns a human-readable log message based on the
// cache result and HTTP status code.
func requestMessage(cacheResult string, status int) string {
	if status != http.StatusOK {
		return "request completed with error"
	}
	switch cacheResult {
	case "HIT":
		return "served cache hit"
	case "MISS":
		return "served cache miss"
	case "BYPASS":
		return "bypassed cache"
	case "STALE":
		return "served stale response"
	case "REVALIDATED":
		return "served revalidated response"
	case "":
		return "served uncached response"
	default:
		return "served response (unknown cache status)"
	}
}
