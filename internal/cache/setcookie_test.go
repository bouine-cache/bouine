package cache

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/thylong/bouine/internal/storage"
	"github.com/thylong/bouine/pkg/header"
)

func newSetCookieHandler(t *testing.T, upstream http.Handler, allow bool) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	return NewHandler(HandlerConfig{
		Upstream:       upstream,
		Store:          store,
		AllowSetCookie: allow,
	})
}

func originWithSetCookie(body, cc, cookie string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if cc != "" {
			w.Header().Set(header.CacheControl, cc)
		}
		if cookie != "" {
			w.Header().Set(header.SetCookie, cookie)
		}
		w.WriteHeader(200)
		_, _ = io.WriteString(w, body)
	})
}

// TestSetCookie_DefaultBlocksCaching verifies the safe default: when
// AllowSetCookie is false (default), a response with Set-Cookie + max-age
// is proxied to the first client but NOT stored in the cache.
func TestSetCookie_DefaultBlocksCaching(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.SetCookie, "session=abc123; Path=/")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	h := newSetCookieHandler(t, upstream, false)

	url := "http://example.com/login"
	rr1 := httptest.NewRecorder()
	h.ServeHTTP(rr1, httptest.NewRequest("GET", url, nil))

	if rr1.Code != 200 {
		t.Fatalf("req1 status = %d", rr1.Code)
	}
	if rr1.Header().Get(header.SetCookie) != "session=abc123; Path=/" {
		t.Errorf("first client should receive Set-Cookie, got %q", rr1.Header().Get(header.SetCookie))
	}

	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", url, nil))

	if calls != 2 {
		t.Errorf("origin called %d times, want 2 (response must not be cached)", calls)
	}
	if rr2.Header().Get(header.XCache) == "HIT" {
		t.Error("second request must not be a HIT when allow_set_cookie is false")
	}
}

// TestSetCookie_AllowTrueStoresWithoutCookie verifies that when
// AllowSetCookie is true, the response IS cached, the first client gets
// Set-Cookie, but the cached copy served to the second client does NOT
// contain Set-Cookie.
func TestSetCookie_AllowTrueStoresWithoutCookie(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.SetCookie, "session=xyz789; Path=/; HttpOnly")
		w.Header().Set(header.ETag, `"v1"`)
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "public-body")
	})
	h := newSetCookieHandler(t, upstream, true)

	url := "http://example.com/page"

	// First request: MISS — client gets Set-Cookie from the origin.
	rr1 := httptest.NewRecorder()
	h.ServeHTTP(rr1, httptest.NewRequest("GET", url, nil))
	if rr1.Code != 200 {
		t.Fatalf("req1 status = %d", rr1.Code)
	}
	if rr1.Header().Get(header.SetCookie) == "" {
		t.Error("first client (MISS) should receive Set-Cookie from origin")
	}

	// Second request: HIT — must NOT have Set-Cookie.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", url, nil))
	if rr2.Header().Get(header.XCache) != "HIT" {
		t.Errorf("second request should be HIT, got X-Cache=%q", rr2.Header().Get(header.XCache))
	}
	if rr2.Header().Get(header.SetCookie) != "" {
		t.Errorf("cached response must NOT contain Set-Cookie, got %q", rr2.Header().Get(header.SetCookie))
	}
	if calls != 1 {
		t.Errorf("origin called %d times, want 1 (response should be cached)", calls)
	}
}

// TestSetCookie_NoStoreStillBlocks verifies that no-store is honoured
// regardless of AllowSetCookie setting.
func TestSetCookie_NoStoreStillBlocks(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set(header.CacheControl, "no-store")
		w.Header().Set(header.SetCookie, "session=nope")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "private")
	})
	h := newSetCookieHandler(t, upstream, true) // even with allow=true

	url := "http://example.com/auth"
	for range 3 {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))
	}
	if calls != 3 {
		t.Errorf("origin called %d times, want 3 (no-store must always bypass)", calls)
	}
}

// TestSetCookie_NoSetCookieHeaderUnaffected confirms that responses
// without Set-Cookie are cached normally regardless of AllowSetCookie.
func TestSetCookie_NoSetCookieHeaderUnaffected(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "public")
	})
	h := newSetCookieHandler(t, upstream, false) // default (false)

	url := "http://example.com/public"
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", url, nil))

	if rr2.Header().Get(header.XCache) != "HIT" {
		t.Errorf("response without Set-Cookie should be cached, got X-Cache=%q", rr2.Header().Get(header.XCache))
	}
	if calls != 1 {
		t.Errorf("origin called %d times, want 1", calls)
	}
}

// TestSetCookie_DefaultBlocksEvenWithExplicitFreshness is the security
// regression test: Set-Cookie + max-age=3600 must NOT be cached when
// AllowSetCookie defaults to false. This was the pre-fix vulnerability.
func TestSetCookie_DefaultBlocksEvenWithExplicitFreshness(t *testing.T) {
	t.Parallel()
	upstream := originWithSetCookie("body", "max-age=3600", "token=secret123")
	h := newSetCookieHandler(t, upstream, false)

	url := "http://example.com/important"
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))

	// Verify the object was NOT stored.
	key := BuildKey(httptest.NewRequest("GET", url, nil))
	obj, _ := h.store.Get(httptest.NewRequest("GET", url, nil).Context(), key)
	if obj != nil {
		t.Fatal("response with Set-Cookie must NOT be stored when allow_set_cookie is false")
	}
}
