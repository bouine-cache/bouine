package cache

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/thylong/bouine/internal/storage"
)

func newMaxResponseBytesHandler(t *testing.T, upstream http.Handler, maxBytes int64) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	return NewHandler(HandlerConfig{
		Upstream:         upstream,
		Store:            store,
		MaxResponseBytes: maxBytes,
	})
}

func TestMaxResponseBytes_OverLimitReturns502(t *testing.T) {
	t.Parallel()
	calls := 0
	body := strings.Repeat("x", 2048)
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("Cache-Control", "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, body)
	})
	h := newMaxResponseBytesHandler(t, upstream, 1024)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/too-big", nil))

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("oversized response should return 502, got %d", rr.Code)
	}
	if calls != 1 {
		t.Errorf("origin called %d times, want 1", calls)
	}

	// Second request should also miss (nothing cached).
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/too-big", nil))
	if rr2.Header().Get("X-Cache") == "HIT" {
		t.Error("truncated response must not be cached")
	}
	if calls != 2 {
		t.Errorf("origin called %d times, want 2 (nothing cached)", calls)
	}
}

func TestMaxResponseBytes_UnderLimitSucceeds(t *testing.T) {
	t.Parallel()
	calls := 0
	body := strings.Repeat("y", 512)
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("Cache-Control", "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, body)
	})
	h := newMaxResponseBytesHandler(t, upstream, 1024)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/ok", nil))
	if rr.Code != 200 || rr.Body.String() != body {
		t.Fatalf("response under limit should pass through: status=%d body=%q", rr.Code, rr.Body.String())
	}

	// Should be cached.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/ok", nil))
	if rr2.Header().Get("X-Cache") != "HIT" {
		t.Errorf("response under limit should be cached, got X-Cache=%q", rr2.Header().Get("X-Cache"))
	}
	if calls != 1 {
		t.Errorf("origin called %d times, want 1 (cached)", calls)
	}
}

func TestMaxResponseBytes_ExactBoundarySucceeds(t *testing.T) {
	t.Parallel()
	body := strings.Repeat("a", 512)
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, body)
	})
	h := newMaxResponseBytesHandler(t, upstream, 512)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/exact", nil))
	if rr.Code != 200 || rr.Body.String() != body {
		t.Fatalf("response at exact boundary should pass through: status=%d body=%q", rr.Code, rr.Body.String())
	}
}

func TestMaxResponseBytes_DefaultAppliedWhenZero(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Cache-Control", "max-age=60")
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "small")
		}),
		Store:            store,
		MaxResponseBytes: 0, // should default to 64 MiB
	})
	if h.maxResponseBytes != defaultMaxResponseBytes {
		t.Fatalf("zero MaxResponseBytes should default to %d, got %d", defaultMaxResponseBytes, h.maxResponseBytes)
	}
}

func TestMaxResponseBytes_InvalidateAndProxyOverLimit(t *testing.T) {
	t.Parallel()
	calls := 0
	body := strings.Repeat("z", 2048)
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("Cache-Control", "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, body)
	})
	h := newMaxResponseBytesHandler(t, upstream, 1024)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("POST", "http://example.com/post", strings.NewReader("")))

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("oversized POST response should return 502, got %d", rr.Code)
	}
}
