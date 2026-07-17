package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bouine-cache/bouine/internal/cache"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
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

func TestReadyz_Detail_NotReady(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		ReadyFn: func() bool { return false },
		ConditionsFn: func() []Condition {
			return []Condition{
				{Name: "store-loaded", Ready: true},
				{Name: "listeners-bound", Ready: false},
			}
		},
	})
	status, body := get(t, s, "/readyz?detail=1")
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", status)
	}
	var resp map[string]any
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp["status"] != "not-ready" {
		t.Fatalf("status = %v, want not-ready", resp["status"])
	}
	conds, ok := resp["conditions"].([]any)
	if !ok {
		t.Fatal("missing conditions array")
	}
	if len(conds) != 2 {
		t.Fatalf("expected 2 conditions, got %d", len(conds))
	}
}

func TestReadyz_Detail_Ready(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		ReadyFn: func() bool { return true },
		ConditionsFn: func() []Condition {
			return []Condition{
				{Name: "store-loaded", Ready: true},
				{Name: "listeners-bound", Ready: true},
			}
		},
	})
	status, body := get(t, s, "/readyz?detail=1")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	var resp map[string]any
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp["status"] != "ready" {
		t.Fatalf("status = %v, want ready", resp["status"])
	}
	conds, ok := resp["conditions"].([]any)
	if !ok {
		t.Fatal("missing conditions array")
	}
	if len(conds) != 2 {
		t.Fatalf("expected 2 conditions, got %d", len(conds))
	}
}

func TestReadyz_Detail_NoConditionsFn(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, func() bool { return true })
	status, body := get(t, s, "/readyz?detail=1")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	var resp map[string]any
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	conds, ok := resp["conditions"].([]any)
	if !ok {
		t.Fatal("missing conditions array")
	}
	if len(conds) != 0 {
		t.Fatalf("expected 0 conditions, got %d", len(conds))
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

	for _, path := range []string{"/healthz", "/readyz", "/version", "/drain"} {
		rr := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(context.Background(), "GET", path, nil)
		s.Handler().ServeHTTP(rr, req)
		if rr.Code == http.StatusUnauthorized {
			t.Errorf("%s returned 401 (should be exempt)", path)
		}
	}
}

func TestDrain_NoDrainFn(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, func() bool { return true })
	status, body := get(t, s, "/drain")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	var got map[string]string
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["status"] != "drained" {
		t.Fatalf("status field = %q, want drained", got["status"])
	}
}

func TestDrain_CallsDrainFn(t *testing.T) {
	t.Parallel()
	called := false
	s := New(Config{
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		ReadyFn: func() bool { return true },
		DrainFn: func() { called = true },
	})
	status, _ := get(t, s, "/drain")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if !called {
		t.Fatal("expected DrainFn to be called")
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

func TestPurgeBatch_Success(t *testing.T) {
	t.Parallel()
	var purgedKeys []api.Key
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(key api.Key) error {
			purgedKeys = append(purgedKeys, key)
			return nil
		},
	})
	body := `{"urls":["https://a.com/","https://b.com/","https://c.com/"]}`
	code, respBody := postWithToken(t, s, "/v1/purge/batch", body)
	if code != http.StatusOK {
		t.Fatalf("got %d, want 200: %s", code, respBody)
	}
	if len(purgedKeys) != 3 {
		t.Fatalf("expected 3 purged keys, got %d", len(purgedKeys))
	}
	if !bytes.Contains(respBody, []byte(`"count":3`)) {
		t.Fatalf("expected count 3, got: %s", respBody)
	}
	if !bytes.Contains(respBody, []byte(`"failed":0`)) {
		t.Fatalf("expected failed 0, got: %s", respBody)
	}
}

func TestPurgeBatch_PartialFailure(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(key api.Key) error {
			if key == cache.BuildKeyFromURL("https://b.com/", nil) {
				return errors.New("storage error")
			}
			return nil
		},
	})
	body := `{"urls":["https://a.com/","https://b.com/","https://c.com/"]}`
	code, respBody := postWithToken(t, s, "/v1/purge/batch", body)
	if code != http.StatusOK {
		t.Fatalf("got %d, want 200: %s", code, respBody)
	}
	if !bytes.Contains(respBody, []byte(`"count":2`)) {
		t.Fatalf("expected count 2 (one failed), got: %s", respBody)
	}
	if !bytes.Contains(respBody, []byte(`"failed":1`)) {
		t.Fatalf("expected failed 1, got: %s", respBody)
	}
}

func TestPurgeBatch_Empty(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
	})
	code, body := postWithToken(t, s, "/v1/purge/batch", `{"urls":[]}`)
	if code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400: %s", code, body)
	}
}

func TestPurgeBatch_ExceedsMax(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:        "secret",
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn:      func(_ api.Key) error { return nil },
		MaxBatchSize: 2,
	})
	body := `{"urls":["https://a.com/","https://b.com/","https://c.com/"]}`
	code, respBody := postWithToken(t, s, "/v1/purge/batch", body)
	if code != http.StatusRequestEntityTooLarge {
		t.Fatalf("got %d, want 413: %s", code, respBody)
	}
}

func TestAuthCheck_WithToken(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	// Without token → 401.
	status, _ := get(t, s, "/v1/auth/check")
	if status != http.StatusUnauthorized {
		t.Fatalf("no token: got %d, want 401", status)
	}
	// With token → 200.
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/v1/auth/check", nil)
	req.Header.Set(header.Authorization, "Bearer secret")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("with token: got %d, want 200", rr.Code)
	}
}

func TestRateLimit_PostRejected(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:              "secret",
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn:            func(_ api.Key) error { return nil },
		RateLimitPerSecond: 1,
	})
	// First POST succeeds (consumes the single token).
	code, _ := postWithToken(t, s, "/v1/purge", `{"url":"https://a.com/"}`)
	if code != http.StatusOK {
		t.Fatalf("first POST: got %d, want 200", code)
	}
	// Second POST immediately → 429 (bucket empty, refill not yet).
	code, body := postWithToken(t, s, "/v1/purge", `{"url":"https://b.com/"}`)
	if code != http.StatusTooManyRequests {
		t.Fatalf("second POST: got %d, want 429: %s", code, body)
	}
	// GET still works (rate limiter skips GET).
	status, _ := get(t, s, "/healthz")
	if status != http.StatusOK {
		t.Fatalf("GET during rate limit: got %d, want 200", status)
	}
}

// --- pprof tests ---

func TestPprof_DisabledByDefault(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	// With auth, the route is not registered → mux returns 404.
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/debug/pprof/heap", nil)
	req.Header.Set(header.Authorization, "Bearer secret")
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("pprof disabled: got %d, want 404", rr.Code)
	}
}

func TestPprof_Enabled_HeapDebug(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PprofEnabled: true,
	})
	code, body := get(t, s, "/debug/pprof/heap?debug=1")
	if code != http.StatusOK {
		t.Fatalf("heap pprof: got %d, want 200", code)
	}
	if !bytes.Contains(body, []byte("heap")) {
		t.Fatalf("heap pprof: body does not contain 'heap': %s", body[:min(len(body), 200)])
	}
}

func TestPprof_NoAuthRequired(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Token:        "secret",
		PprofEnabled: true,
	})
	// No Authorization header — pprof must still be reachable.
	code, _ := get(t, s, "/debug/pprof/heap?debug=1")
	if code != http.StatusOK {
		t.Fatalf("pprof without auth: got %d, want 200", code)
	}
}

func TestPprof_GoroutineDebug(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PprofEnabled: true,
	})
	code, body := get(t, s, "/debug/pprof/goroutine?debug=1")
	if code != http.StatusOK {
		t.Fatalf("goroutine pprof: got %d, want 200", code)
	}
	if !bytes.Contains(body, []byte("goroutine")) {
		t.Fatalf("goroutine pprof: body does not contain 'goroutine': %s", body[:min(len(body), 200)])
	}
}

func TestPprof_Index(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PprofEnabled: true,
	})
	code, body := get(t, s, "/debug/pprof/")
	if code != http.StatusOK {
		t.Fatalf("pprof index: got %d, want 200", code)
	}
	if !bytes.Contains(body, []byte("pprof")) {
		t.Fatalf("pprof index: body does not contain 'pprof': %s", body[:min(len(body), 200)])
	}
}

// TestPprof_AuthStillEnforcedOnOtherEndpoints proves the auth exemption
// is scoped to /debug/pprof/* only — non-pprof write endpoints still
// require the bearer token when pprof is enabled.
func TestPprof_AuthStillEnforcedOnOtherEndpoints(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:        "secret",
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn:      func(_ api.Key) error { return nil },
		PprofEnabled: true,
	})
	// POST /v1/purge without token → must still get 401, not 200.
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/purge",
		bytes.NewBufferString(`{"url":"https://a.com/"}`))
	req.Header.Set(header.ContentType, "application/json")
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("purge without auth (pprof enabled): got %d, want 401", rr.Code)
	}
}
