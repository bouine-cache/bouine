package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.Equal(t, http.StatusOK, status)
	var got map[string]string
	err := json.Unmarshal(body, &got)
	require.NoError(t, err, "unmarshal")
	require.Equal(t, "ok", got["status"])
}

func TestReadyz_Ready(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, func() bool { return true })
	status, _ := get(t, s, "/readyz")
	require.Equal(t, http.StatusOK, status)
}

func TestReadyz_NotReady(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, func() bool { return false })
	status, _ := get(t, s, "/readyz")
	require.Equal(t, http.StatusServiceUnavailable, status)
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
	require.Equal(t, http.StatusServiceUnavailable, status)
	var resp map[string]any
	err := json.Unmarshal(body, &resp)
	require.NoError(t, err, "unmarshal")
	require.Equal(t, "not-ready", resp["status"])
	conds, ok := resp["conditions"].([]any)
	require.True(t, ok)
	require.Len(t, conds, 2)
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
	require.Equal(t, http.StatusOK, status)
	var resp map[string]any
	err := json.Unmarshal(body, &resp)
	require.NoError(t, err, "unmarshal")
	require.Equal(t, "ready", resp["status"])
	conds, ok := resp["conditions"].([]any)
	require.True(t, ok)
	require.Len(t, conds, 2)
}

func TestReadyz_Detail_NoConditionsFn(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, func() bool { return true })
	status, body := get(t, s, "/readyz?detail=1")
	require.Equal(t, http.StatusOK, status)
	var resp map[string]any
	err := json.Unmarshal(body, &resp)
	require.NoError(t, err, "unmarshal")
	conds, ok := resp["conditions"].([]any)
	require.True(t, ok)
	require.Len(t, conds, 0)
}

func TestVersion(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)
	status, body := get(t, s, "/version")
	require.Equal(t, http.StatusOK, status)
	require.True(t, bytes.Contains(body, []byte("version")))
}

func TestMetrics_Mounted(t *testing.T) {
	t.Parallel()
	m := observability.NewMetrics()
	s := New(Config{
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Metrics: m,
	})
	status, body := get(t, s, "/metrics")
	require.Equal(t, http.StatusOK, status)
	require.True(t, bytes.Contains(body, []byte("go_info")))
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
	require.Equal(t, http.StatusUnauthorized, rr.Code)

	// Write with wrong token → 401.
	rr = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), "POST", "/v1/purge",
		bytes.NewBufferString(`{"url":"https://example.com/"}`))
	req.Header.Set(header.ContentType, "application/json")
	req.Header.Set(header.Authorization, "Bearer wrong")
	s.Handler().ServeHTTP(rr, req)
	require.Equal(t, http.StatusUnauthorized, rr.Code)

	// Write with correct token → 200.
	rr = httptest.NewRecorder()
	req = httptest.NewRequestWithContext(context.Background(), "POST", "/v1/purge",
		bytes.NewBufferString(`{"url":"https://example.com/"}`))
	req.Header.Set(header.ContentType, "application/json")
	req.Header.Set(header.Authorization, "Bearer secret")
	s.Handler().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
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
		assert.NotEqual(t, http.StatusUnauthorized, rr.Code)
	}
}

func TestAuth_PeerMetricsExempt(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PeerMetricsHandler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("{}"))
		}),
	})

	// GET without token should succeed because /v1/peer/metrics is in
	// the auth-exempt map (same as peer fetch/purge/ban RPCs).
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/v1/peer/metrics", nil)
	s.Handler().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	require.Equal(t, "{}", body)
}

func TestDrain_NoDrainFn(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, func() bool { return true })
	status, body := get(t, s, "/drain")
	require.Equal(t, http.StatusOK, status)
	var got map[string]string
	err := json.Unmarshal(body, &got)
	require.NoError(t, err, "unmarshal")
	require.Equal(t, "drained", got["status"])
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
	require.Equal(t, http.StatusOK, status)
	require.True(t, called)
}

// TestDrain_LongDrainFnSurvivesWriteTimeout verifies that the /drain
// endpoint returns 200 even when DrainFn blocks longer than the admin
// server's WriteTimeout. This reproduces the K8s preStop hook failure
// where a 10s drain sleep exceeds the 5s WriteTimeout and net/http kills
// the connection before the response is written.
func TestDrain_LongDrainFnSurvivesWriteTimeout(t *testing.T) {
	t.Parallel()

	drainStarted := make(chan struct{})
	s := New(Config{
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		ReadyFn: func() bool { return true },
		DrainFn: func() {
			close(drainStarted)
			time.Sleep(200 * time.Millisecond)
		},
	})

	// Shrink the write timeout below the drain sleep so the test
	// reproduces the production failure (5s WriteTimeout vs 10s drain).
	s.inner.WriteTimeout = 50 * time.Millisecond

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")
	defer ln.Close()

	go s.inner.Serve(ln)
	defer s.inner.Close()

	resp, err := http.Get("http://" + ln.Addr().String() + "/drain")
	require.NoError(t, err, "drain request failed (write timeout killed connection)")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read body")
	var got map[string]string
	err = json.Unmarshal(body, &got)
	require.NoError(t, err, "unmarshal")
	require.Equal(t, "drained", got["status"])

	// DrainFn must have been called synchronously — the response is
	// only written after it returns, so the channel must already be
	// closed.
	select {
	case <-drainStarted:
	default:
		t.Fatal("expected DrainFn to be called before response is written")
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
	require.Equal(t, http.StatusBadRequest, code)
	require.True(t, bytes.Contains(body, []byte("url field is required")))
}

func TestPurge_UnknownField(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
	})
	code, _ := postWithToken(t, s, "/v1/purge", `{"patdddh_regex":"^/reviews/"}`)
	require.Equal(t, http.StatusBadRequest, code)
}

func TestPurge_MalformedJSON(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
	})
	code, _ := postWithToken(t, s, "/v1/purge", `{not json`)
	require.Equal(t, http.StatusBadRequest, code)
}

func TestPurge_Valid(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
	})
	code, _ := postWithToken(t, s, "/v1/purge", `{"url":"https://example.com/reviews/1"}`)
	require.Equal(t, http.StatusOK, code)
}

func TestRefresh_EmptyURL(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:     "secret",
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		RefreshFn: func(_ api.Key) error { return nil },
	})
	code, body := postWithToken(t, s, "/v1/refresh", `{}`)
	require.Equal(t, http.StatusBadRequest, code)
	require.True(t, bytes.Contains(body, []byte("url field is required")))
}

func TestRefresh_UnknownField(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:     "secret",
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		RefreshFn: func(_ api.Key) error { return nil },
	})
	code, _ := postWithToken(t, s, "/v1/refresh", `{"patdddh_regex":"^/reviews/"}`)
	require.Equal(t, http.StatusBadRequest, code)
}

func TestRefresh_Valid(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:     "secret",
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		RefreshFn: func(_ api.Key) error { return nil },
	})
	code, _ := postWithToken(t, s, "/v1/refresh", `{"url":"https://example.com/reviews/1"}`)
	require.Equal(t, http.StatusOK, code)
}

func TestBan_EmptyExpr(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		BanFn:  func(_ api.BanExpr) (int, error) { return 0, nil },
	})
	code, body := postWithToken(t, s, "/v1/ban", `{}`)
	require.Equal(t, http.StatusBadRequest, code)
	require.True(t, bytes.Contains(body, []byte("at least one of")))
}

func TestBan_UnknownField(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		BanFn:  func(_ api.BanExpr) (int, error) { return 0, nil },
	})
	code, _ := postWithToken(t, s, "/v1/ban", `{"patdddh_regex":"^/reviews/"}`)
	require.Equal(t, http.StatusBadRequest, code)
}

func TestBan_Valid(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		BanFn:  func(_ api.BanExpr) (int, error) { return 42, nil },
	})
	code, body := postWithToken(t, s, "/v1/ban", `{"path_regex":"^/reviews/"}`)
	require.Equal(t, http.StatusOK, code)
	require.True(t, bytes.Contains(body, []byte("42")))
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
	require.Equal(t, http.StatusOK, code)
	require.Len(t, purgedKeys, 3)
	require.True(t, bytes.Contains(respBody, []byte(`"count":3`)))
	require.True(t, bytes.Contains(respBody, []byte(`"failed":0`)))
}

func TestPurgeBatch_PartialFailure(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(key api.Key) error {
			if key == func() api.Key { k, _ := cache.BuildKeyFromURL("https://b.com/", nil); return k }() {
				return errors.New("storage error")
			}
			return nil
		},
	})
	body := `{"urls":["https://a.com/","https://b.com/","https://c.com/"]}`
	code, respBody := postWithToken(t, s, "/v1/purge/batch", body)
	require.Equal(t, http.StatusOK, code)
	require.True(t, bytes.Contains(respBody, []byte(`"count":2`)))
	require.True(t, bytes.Contains(respBody, []byte(`"failed":1`)))
}

func TestPurgeBatch_Empty(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
	})
	code, _ := postWithToken(t, s, "/v1/purge/batch", `{"urls":[]}`)
	require.Equal(t, http.StatusBadRequest, code)
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
	code, _ := postWithToken(t, s, "/v1/purge/batch", body)
	require.Equal(t, http.StatusRequestEntityTooLarge, code)
}

func TestAuthCheck_WithToken(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	// Without token → 401.
	status, _ := get(t, s, "/v1/auth/check")
	require.Equal(t, http.StatusUnauthorized, status)
	// With token → 200.
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/v1/auth/check", nil)
	req.Header.Set(header.Authorization, "Bearer secret")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
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
	require.Equal(t, http.StatusOK, code)
	// Second POST immediately → 429 (bucket empty, refill not yet).
	code, _ = postWithToken(t, s, "/v1/purge", `{"url":"https://b.com/"}`)
	require.Equal(t, http.StatusTooManyRequests, code)
	// GET still works (rate limiter skips GET).
	status, _ := get(t, s, "/healthz")
	require.Equal(t, http.StatusOK, status)
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
	require.Equal(t, http.StatusNotFound, rr.Code)
}

func TestPprof_Enabled_HeapDebug(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PprofEnabled: true,
	})
	code, body := get(t, s, "/debug/pprof/heap?debug=1")
	require.Equal(t, http.StatusOK, code)
	require.True(t, bytes.Contains(body, []byte("heap")))
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
	require.Equal(t, http.StatusOK, code)
}

func TestPprof_GoroutineDebug(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PprofEnabled: true,
	})
	code, body := get(t, s, "/debug/pprof/goroutine?debug=1")
	require.Equal(t, http.StatusOK, code)
	require.True(t, bytes.Contains(body, []byte("goroutine")))
}

func TestPprof_Index(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PprofEnabled: true,
	})
	code, body := get(t, s, "/debug/pprof/")
	require.Equal(t, http.StatusOK, code)
	require.True(t, bytes.Contains(body, []byte("pprof")))
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
	require.Equal(t, http.StatusUnauthorized, rr.Code)
}
