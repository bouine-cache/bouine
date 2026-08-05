package accesslog

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/observability/responsewriter"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestMiddleware_LogsAccess(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(201)
		_, _ = io.WriteString(w, "ok")
	})

	h := Middleware(logger, inner)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/test", nil)
	req.Host = "example.com"
	h.ServeHTTP(rr, req)

	require.Equal(t, 201, rr.Code)

	var rec map[string]any
	err := json.Unmarshal(buf.Bytes(), &rec)
	require.NoError(t, err, "unmarshal")
	assert.Equal(t, "POST", rec["method"])
	assert.Equal(t, float64(201), rec["status"])
	assert.Equal(t, "/api/test", rec["path"])
	assert.Equal(t, "example.com", rec["host"])
	assert.Equal(t, float64(2), rec["bytes_out"])
	assert.Equal(t, "request completed with error", rec["msg"])
}

func TestMiddleware_200WithKeyLogsInfo(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := observability.NewSampledLogger(
		slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
		0, // always log
	)

	// The inner handler receives the middleware's ResponseWriter (sw).
	// It sets the cache key on sw, which is the outer wrapper.
	// The middleware reads sw.Key after the handler returns.
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.XCache, "HIT")
		w.WriteHeader(200)
		if rw, ok := w.(*responsewriter.ResponseWriter); ok {
			rw.SetCacheKey(api.Key{Hash: 42})
		}
	})

	h := Middleware(logger, inner)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/cached", nil)
	h.ServeHTTP(rr, req)

	var rec map[string]any
	err := json.Unmarshal(buf.Bytes(), &rec)
	require.NoError(t, err, "unmarshal")
	assert.Equal(t, "served cache hit", rec["msg"])
	assert.Equal(t, "2a", rec["key"])
}

func TestMiddleware_200WithoutKeyLogsInfo(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := observability.NewSampledLogger(
		slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
		0,
	)

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.XCache, "MISS")
		w.WriteHeader(200)
	})

	h := Middleware(logger, inner)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/miss", nil)
	h.ServeHTTP(rr, req)

	var rec map[string]any
	err := json.Unmarshal(buf.Bytes(), &rec)
	require.NoError(t, err, "unmarshal")
	assert.Equal(t, "served cache miss", rec["msg"])
}

func TestMiddleware_Non200LogsWarn(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	// Use rate 100 to verify Warn is never sampled.
	logger := observability.NewSampledLogger(
		slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
		100,
	)

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
	})

	h := Middleware(logger, inner)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/error", nil)
	h.ServeHTTP(rr, req)

	require.NotEqual(t, 0, buf.Len())
}

func TestRequestMessage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		cacheResult string
		status      int
		want        string
	}{
		{"HIT", 200, "served cache hit"},
		{"MISS", 200, "served cache miss"},
		{"BYPASS", 200, "bypassed cache"},
		{"STALE", 200, "served stale response"},
		{"REVALIDATED", 200, "served revalidated response"},
		{"", 200, "served uncached response"},
		{"UNKNOWN", 200, "served response (unknown cache status)"},
		{"HIT", 500, "request completed with error"},
		{"", 404, "request completed with error"},
	}
	for _, tc := range tests {
		got := requestMessage(tc.cacheResult, tc.status)
		assert.Equal(t, tc.want, got)
	}
}
