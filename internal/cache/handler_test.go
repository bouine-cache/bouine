package cache

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/thylong/bouine/internal/storage"
)

func origin200(body, cc string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if cc != "" {
			w.Header().Set("Cache-Control", cc)
		}
		w.Header().Set("ETag", `"v1"`)
		w.WriteHeader(200)
		_, _ = io.WriteString(w, body)
	})
}

func testHandler(t *testing.T, upstream http.Handler) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  1 << 20,
		NumShards: 2,
	})
	return NewHandler(HandlerConfig{
		Upstream: upstream,
		Store:    store,
	})
}

func TestHandler_MissThenHit(t *testing.T) {
	var originCalls int
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		originCalls++
		w.Header().Set("Cache-Control", "max-age=60")
		w.Header().Set("ETag", `"v1"`)
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "cached-body")
	})
	h := testHandler(t, upstream)

	// First request — MISS, fetches from origin.
	rr1 := httptest.NewRecorder()
	h.ServeHTTP(rr1, httptest.NewRequest("GET", "http://example.com/foo", nil))
	if rr1.Code != 200 {
		t.Fatalf("req1 status = %d", rr1.Code)
	}
	if rr1.Header().Get("X-Cache") != "MISS" {
		t.Fatalf("req1 X-Cache = %q", rr1.Header().Get("X-Cache"))
	}
	if rr1.Body.String() != "cached-body" {
		t.Fatalf("req1 body = %q", rr1.Body.String())
	}

	// Second request — HIT, served from cache.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/foo", nil))
	if rr2.Code != 200 {
		t.Fatalf("req2 status = %d", rr2.Code)
	}
	if rr2.Header().Get("X-Cache") != "HIT" {
		t.Fatalf("req2 X-Cache = %q", rr2.Header().Get("X-Cache"))
	}
	if rr2.Body.String() != "cached-body" {
		t.Fatalf("req2 body = %q", rr2.Body.String())
	}

	// Age header should be present.
	if rr2.Header().Get("Age") == "" {
		t.Fatal("req2 missing Age header")
	}

	// Origin should have been called only once.
	if originCalls != 1 {
		t.Fatalf("origin called %d times, want 1", originCalls)
	}
}

func TestHandler_NoStoreNotCached(t *testing.T) {
	var calls int
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "private")
	}))

	for range 3 {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/x", nil))
		if rr.Code != 200 {
			t.Fatalf("status = %d", rr.Code)
		}
	}
	if calls != 3 {
		t.Fatalf("origin called %d times, want 3 (not cached)", calls)
	}
}

func TestHandler_PostInvalidates(t *testing.T) {
	h := testHandler(t, origin200("body", "max-age=60"))

	// Populate cache.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/res", nil))
	if rr.Header().Get("X-Cache") != "MISS" {
		t.Fatal("expected MISS")
	}

	// POST invalidates.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("POST", "http://example.com/res", strings.NewReader("data")))

	// GET again — should be MISS (invalidated).
	rr3 := httptest.NewRecorder()
	h.ServeHTTP(rr3, httptest.NewRequest("GET", "http://example.com/res", nil))
	if rr3.Header().Get("X-Cache") != "MISS" {
		t.Fatalf("expected MISS after POST invalidation, got %q", rr3.Header().Get("X-Cache"))
	}
}

func TestHandler_BypassOnRequestNoStore(t *testing.T) {
	h := testHandler(t, origin200("body", "max-age=60"))

	// Populate cache.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/bp", nil))

	// Request with no-store bypasses cache — goes directly to upstream.
	req := httptest.NewRequest("GET", "http://example.com/bp", nil)
	req.Header.Set("Cache-Control", "no-store")
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, req)
	if rr2.Code != 200 {
		t.Fatalf("bypass status = %d", rr2.Code)
	}
}

func TestHandler_HeadServedFromCache(t *testing.T) {
	h := testHandler(t, origin200("full-body", "max-age=60"))

	// Populate with GET.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/hd", nil))

	// HEAD should hit cache but not return body.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("HEAD", "http://example.com/hd", nil))
	if rr2.Code != 200 {
		t.Fatalf("HEAD status = %d", rr2.Code)
	}
	if rr2.Header().Get("X-Cache") != "HIT" {
		t.Fatalf("HEAD X-Cache = %q", rr2.Header().Get("X-Cache"))
	}
	if rr2.Body.Len() != 0 {
		t.Fatalf("HEAD should have empty body, got %d bytes", rr2.Body.Len())
	}
}
