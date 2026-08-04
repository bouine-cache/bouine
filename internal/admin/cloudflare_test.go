package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func getAuth(t *testing.T, s *Server, path string) (int, []byte) {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), "GET", path, nil)
	req.Header.Set(header.Authorization, "Bearer test")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	return rr.Code, rr.Body.Bytes()
}

// TestPurge_CallsOnPurged verifies that a successful purge calls the
// OnPurged callback with the raw URL from the request body.
func TestPurge_CallsOnPurged(t *testing.T) {
	t.Parallel()
	var gotURL string
	s := New(Config{
		Token:   "test",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
		OnPurged: func(_ context.Context, url string) {
			gotURL = url
		},
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/purge",
		bytes.NewBufferString(`{"url":"https://example.com/products/123"}`))
	req.Header.Set(header.ContentType, "application/json")
	req.Header.Set(header.Authorization, "Bearer test")
	s.Handler().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "https://example.com/products/123", gotURL)
}

// TestRefresh_CallsOnRefreshed verifies that a successful refresh calls the
// OnRefreshed callback.
func TestRefresh_CallsOnRefreshed(t *testing.T) {
	t.Parallel()
	var gotURL string
	s := New(Config{
		Token:     "test",
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		RefreshFn: func(_ api.Key) error { return nil },
		OnRefreshed: func(_ context.Context, url string) {
			gotURL = url
		},
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/refresh",
		bytes.NewBufferString(`{"url":"https://example.com/img/logo.png"}`))
	req.Header.Set(header.ContentType, "application/json")
	req.Header.Set(header.Authorization, "Bearer test")
	s.Handler().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "https://example.com/img/logo.png", gotURL)
}

// TestBan_CallsOnBanned verifies that a successful ban calls the OnBanned
// callback with the decoded BanExpr.
func TestBan_CallsOnBanned(t *testing.T) {
	t.Parallel()
	var gotExpr api.BanExpr
	s := New(Config{
		Token:  "test",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		BanFn:  func(expr api.BanExpr) (int, error) { return 5, nil },
		OnBanned: func(_ context.Context, expr api.BanExpr) {
			gotExpr = expr
		},
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/ban",
		bytes.NewBufferString(`{"host_regex":"cdn.example.com","path_regex":"/api/v2/.*"}`))
	req.Header.Set(header.ContentType, "application/json")
	req.Header.Set(header.Authorization, "Bearer test")
	s.Handler().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "cdn.example.com", gotExpr.HostRegex)
	require.Equal(t, "/api/v2/.*", gotExpr.PathRegex)
}

// TestCloudflareStatus_Endpoint verifies that GET /v1/cloudflare/status returns
// the status from CFStatusFn.
func TestCloudflareStatus_Endpoint(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "test",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		CFStatusFn: func() CloudflareStatus {
			return CloudflareStatus{
				Enabled:       true,
				ZoneID:        "abc123",
				Async:         true,
				LastError:     nil,
				LastSuccessAt: nil,
			}
		},
	})
	status, body := getAuth(t, s, "/v1/cloudflare/status")
	require.Equal(t, http.StatusOK, status)
	var got map[string]any
	err := json.Unmarshal(body, &got)
	require.NoError(t, err, "unmarshal")
	require.Equal(t, true, got["enabled"])
	require.Equal(t, "abc123", got["zone_id"])
	require.Equal(t, true, got["async"])
}

// TestCloudflareStatus_Disabled verifies that the endpoint returns 404 when
// CF is not configured (CFStatusFn is nil).
func TestCloudflareStatus_Disabled(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "test",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		// CFStatusFn is nil — CF not configured.
	})
	status, _ := getAuth(t, s, "/v1/cloudflare/status")
	require.Equal(t, http.StatusNotFound, status)
}

// TestPurge_OnPurgedNotCalledOnError verifies that OnPurged is NOT called
// when the purge fails.
func TestPurge_OnPurgedNotCalledOnError(t *testing.T) {
	t.Parallel()
	var called atomic.Bool
	s := New(Config{
		Token:   "test",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return io.EOF },
		OnPurged: func(_ context.Context, _ string) {
			called.Store(true)
		},
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/purge",
		bytes.NewBufferString(`{"url":"https://example.com/"}`))
	req.Header.Set(header.ContentType, "application/json")
	req.Header.Set(header.Authorization, "Bearer test")
	s.Handler().ServeHTTP(rr, req)
	// Purge failed, so the handler still returns 500 and OnPurged should not fire.
	_ = rr.Code // status is 500 but we only care that OnPurged was not called
	time.Sleep(10 * time.Millisecond)
	require.False(t, called.Load())
}
