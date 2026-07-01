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
	"github.com/thylong/bouine/pkg/api"
	"github.com/thylong/bouine/pkg/header"
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
	t.Parallel()
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
	t.Parallel()
	s := newTestServer(t, func() bool { return true })
	status, _ := get(t, s, "/readyz")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
}

func TestReadyz_NotReady(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, func() bool { return false })
	status, _ := get(t, s, "/readyz")
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", status)
	}
}

func TestVersion(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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

func TestAuth_WriteRequiresToken(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
	})

	// Write without token → 401.
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/purge",
		bytes.NewBufferString(`{"url":"https://example.com/"}`))
	req.Header.Set(header.ContentType, "application/json")
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("no token: got %d, want 401", rr.Code)
	}

	// Write with wrong token → 401.
	rr = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), "POST", "/v1/purge",
		bytes.NewBufferString(`{"url":"https://example.com/"}`))
	req.Header.Set(header.ContentType, "application/json")
	req.Header.Set(header.Authorization, "Bearer wrong")
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: got %d, want 401", rr.Code)
	}

	// Write with correct token → 200.
	rr = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), "POST", "/v1/purge",
		bytes.NewBufferString(`{"url":"https://example.com/"}`))
	req.Header.Set(header.ContentType, "application/json")
	req.Header.Set(header.Authorization, "Bearer secret")
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("correct token: got %d, want 200", rr.Code)
	}
}

func TestAuth_ReadEndpointsExempt(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	for _, path := range []string{"/healthz", "/readyz", "/version"} {
		rr := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), "GET", path, nil)
		s.Handler().ServeHTTP(rr, req)
		if rr.Code == http.StatusUnauthorized {
			t.Errorf("%s returned 401 (should be exempt)", path)
		}
	}
}

func postWithToken(t *testing.T, s *Server, path, body string) (int, []byte) {
	t.Helper()
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", path,
		bytes.NewBufferString(body))
	req.Header.Set(header.ContentType, "application/json")
	req.Header.Set(header.Authorization, "Bearer secret")
	s.Handler().ServeHTTP(rr, req)
	return rr.Code, rr.Body.Bytes()
}

func TestPurge_EmptyURL(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
	})
	code, body := postWithToken(t, s, "/v1/purge", `{}`)
	if code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400: %s", code, body)
	}
	if !bytes.Contains(body, []byte("url field is required")) {
		t.Fatalf("expected url error, got: %s", body)
	}
}

func TestPurge_UnknownField(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
	})
	code, body := postWithToken(t, s, "/v1/purge", `{"patdddh_regex":"^/reviews/"}`)
	if code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400: %s", code, body)
	}
}

func TestPurge_MalformedJSON(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
	})
	code, _ := postWithToken(t, s, "/v1/purge", `{not json`)
	if code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", code)
	}
}

func TestPurge_Valid(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
	})
	code, body := postWithToken(t, s, "/v1/purge", `{"url":"https://example.com/reviews/1"}`)
	if code != http.StatusOK {
		t.Fatalf("got %d, want 200: %s", code, body)
	}
}

func TestRefresh_EmptyURL(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:     "secret",
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		RefreshFn: func(_ api.Key) error { return nil },
	})
	code, body := postWithToken(t, s, "/v1/refresh", `{}`)
	if code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400: %s", code, body)
	}
	if !bytes.Contains(body, []byte("url field is required")) {
		t.Fatalf("expected url error, got: %s", body)
	}
}

func TestRefresh_UnknownField(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:     "secret",
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		RefreshFn: func(_ api.Key) error { return nil },
	})
	code, body := postWithToken(t, s, "/v1/refresh", `{"patdddh_regex":"^/reviews/"}`)
	if code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400: %s", code, body)
	}
}

func TestRefresh_Valid(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:     "secret",
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		RefreshFn: func(_ api.Key) error { return nil },
	})
	code, body := postWithToken(t, s, "/v1/refresh", `{"url":"https://example.com/reviews/1"}`)
	if code != http.StatusOK {
		t.Fatalf("got %d, want 200: %s", code, body)
	}
}

func TestBan_EmptyExpr(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		BanFn:  func(_ api.BanExpr) (int, error) { return 0, nil },
	})
	code, body := postWithToken(t, s, "/v1/ban", `{}`)
	if code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400: %s", code, body)
	}
	if !bytes.Contains(body, []byte("at least one of")) {
		t.Fatalf("expected predicate error, got: %s", body)
	}
}

func TestBan_UnknownField(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		BanFn:  func(_ api.BanExpr) (int, error) { return 0, nil },
	})
	code, body := postWithToken(t, s, "/v1/ban", `{"patdddh_regex":"^/reviews/"}`)
	if code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400: %s", code, body)
	}
}

func TestBan_Valid(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		BanFn:  func(_ api.BanExpr) (int, error) { return 42, nil },
	})
	code, body := postWithToken(t, s, "/v1/ban", `{"path_regex":"^/reviews/"}`)
	if code != http.StatusOK {
		t.Fatalf("got %d, want 200: %s", code, body)
	}
	if !bytes.Contains(body, []byte("42")) {
		t.Fatalf("expected count 42 in response, got: %s", body)
	}
}
