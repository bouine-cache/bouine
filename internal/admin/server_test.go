package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/thylong/bouine/internal/observability"
)

func newTestServer(t *testing.T, ready func() bool) *Server {
	t.Helper()
	return New(Config{
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		ReadyFn: ready,
	})
}

func get(t *testing.T, s *Server, path string) (int, []byte) {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), "GET", path, nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	return rr.Code, rr.Body.Bytes()
}

func TestHealthz(t *testing.T) {
	s := newTestServer(t, nil)
	status, body := get(t, s, "/healthz")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	var got map[string]string
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["status"] != "ok" {
		t.Fatalf("status field = %q, want ok", got["status"])
	}
}

func TestReadyz_Ready(t *testing.T) {
	s := newTestServer(t, func() bool { return true })
	status, _ := get(t, s, "/readyz")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
}

func TestReadyz_NotReady(t *testing.T) {
	s := newTestServer(t, func() bool { return false })
	status, _ := get(t, s, "/readyz")
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", status)
	}
}

func TestVersion(t *testing.T) {
	s := newTestServer(t, nil)
	status, body := get(t, s, "/version")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if !bytes.Contains(body, []byte("version")) {
		t.Fatalf("missing version field: %s", body)
	}
}

func TestMetrics_Mounted(t *testing.T) {
	m := observability.NewMetrics()
	s := New(Config{
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Metrics: m,
	})
	status, body := get(t, s, "/metrics")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if !bytes.Contains(body, []byte("go_info")) {
		t.Fatalf("expected go_info metric, got: %s", body[:min(len(body), 200)])
	}
}
