package accesslog

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/internal/observability/responsewriter"
	"github.com/thylong/bouine/pkg/api"
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

	if rr.Code != 201 {
		t.Fatalf("status = %d", rr.Code)
	}

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rec["method"] != "POST" {
		t.Errorf("method = %v", rec["method"])
	}
	if rec["status"] != float64(201) {
		t.Errorf("status = %v", rec["status"])
	}
	if rec["path"] != "/api/test" {
		t.Errorf("path = %v", rec["path"])
	}
	if rec["host"] != "example.com" {
		t.Errorf("host = %v", rec["host"])
	}
	if rec["bytes_out"] != float64(2) {
		t.Errorf("bytes_out = %v", rec["bytes_out"])
	}
	if rec["msg"] != "request completed with error" {
		t.Errorf("msg = %v, want 'request completed with error'", rec["msg"])
	}
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
		w.Header().Set("X-Cache", "HIT")
		w.WriteHeader(200)
		if rw, ok := w.(*responsewriter.ResponseWriter); ok {
			rw.SetCacheKey(api.Key(42))
		}
	})

	h := Middleware(logger, inner)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/cached", nil)
	h.ServeHTTP(rr, req)

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rec["msg"] != "served cache hit" {
		t.Errorf("msg = %v, want 'served cache hit'", rec["msg"])
	}
	if rec["key"] != "2a" {
		t.Errorf("key = %v, want 2a", rec["key"])
	}
}

func TestMiddleware_200WithoutKeyLogsInfo(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := observability.NewSampledLogger(
		slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
		0,
	)

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Cache", "MISS")
		w.WriteHeader(200)
	})

	h := Middleware(logger, inner)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/miss", nil)
	h.ServeHTTP(rr, req)

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rec["msg"] != "served cache miss" {
		t.Errorf("msg = %v, want 'served cache miss'", rec["msg"])
	}
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

	if buf.Len() == 0 {
		t.Fatal("non-200 should always log (Warn is never sampled)")
	}
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
		if got != tc.want {
			t.Errorf("requestMessage(%q, %d) = %q, want %q",
				tc.cacheResult, tc.status, got, tc.want)
		}
	}
}
