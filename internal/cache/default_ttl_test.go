package cache

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/thylong/bouine/internal/storage"
)

// newDefaultTTLHandler builds a Handler with DefaultTTL set, backed by an
// in-process hot store.
func newDefaultTTLHandler(t *testing.T, upstream http.Handler, def time.Duration) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	return NewHandler(HandlerConfig{
		Upstream:   upstream,
		Store:      store,
		DefaultTTL: def,
	})
}

// TestIsCacheableWithDefault_NoFreshness verifies that a bare 200 with no
// freshness headers becomes eligible once DefaultTTL > 0, while remaining
// uncacheable under the strict RFC decision.
func TestIsCacheableWithDefault_NoFreshness(t *testing.T) {
	t.Parallel()
	req := http.Header{}
	resp := http.Header{} // no Cache-Control, Expires, or Last-Modified

	if IsCacheable(200, req, resp) {
		t.Fatal("strict IsCacheable should reject a header-less 200")
	}
	if !IsCacheableWithDefault(200, req, resp, 0, 5*time.Second) {
		t.Error("DefaultTTL should make a header-less 200 eligible")
	}
	if IsCacheableWithDefault(200, req, resp, 0, 0) {
		t.Error("DefaultTTL=0 must not change the strict decision")
	}
}

// TestIsCacheableWithDefault_HonoursBlocks verifies blocking directives still
// prevent storage even when DefaultTTL is configured.
func TestIsCacheableWithDefault_HonoursBlocks(t *testing.T) {
	t.Parallel()
	const def = 5 * time.Second
	cases := []struct {
		name   string
		status int
		req    http.Header
		resp   http.Header
		want   bool
	}{
		{"no-store", 200, http.Header{}, http.Header{"Cache-Control": {"no-store"}}, false},
		{"private", 200, http.Header{}, http.Header{"Cache-Control": {"private"}}, false},
		{"set-cookie", 200, http.Header{}, http.Header{"Set-Cookie": {"sid=abc"}}, false},
		{"vary-star", 200, http.Header{}, http.Header{"Vary": {"*"}}, false},
		{"pragma-no-cache", 200, http.Header{}, http.Header{"Pragma": {"no-cache"}}, false},
		{"authorization", 200, http.Header{"Authorization": {"Bearer x"}}, http.Header{}, false},
		{"5xx-excluded", 500, http.Header{}, http.Header{}, false},
		{"plain-200-ok", 200, http.Header{}, http.Header{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := IsCacheableWithDefault(tc.status, tc.req, tc.resp, 0, def)
			if got != tc.want {
				t.Errorf("IsCacheableWithDefault(%d) = %v, want %v", tc.status, got, tc.want)
			}
		})
	}
}

// TestDefaultTTL_CachesHeaderlessResponse is the end-to-end regression test
// for the reported bug: an origin that sends a bare 200 (no Cache-Control,
// Expires, or Last-Modified) must be cached for DefaultTTL, so the second
// request is a HIT rather than a perpetual MISS.
func TestDefaultTTL_CachesHeaderlessResponse(t *testing.T) {
	t.Parallel()
	var hits int
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	h := newDefaultTTLHandler(t, upstream, 5*time.Second)

	req := httptest.NewRequest("GET", "http://example.com/r", nil)
	rr1 := httptest.NewRecorder()
	h.ServeHTTP(rr1, req)
	if got := rr1.Header().Get("X-Cache"); got != "MISS" {
		t.Fatalf("first request X-Cache = %q, want MISS", got)
	}

	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/r", nil))
	if got := rr2.Header().Get("X-Cache"); got != "HIT" {
		t.Fatalf("second request X-Cache = %q, want HIT", got)
	}
	if hits != 1 {
		t.Errorf("origin hit %d times, want 1 (second served from cache)", hits)
	}
}

// TestDefaultTTL_DisabledKeepsMISS guards the strict default: with no
// DefaultTTL configured, a header-less response is never cached.
func TestDefaultTTL_DisabledKeepsMISS(t *testing.T) {
	t.Parallel()
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	h := newDefaultTTLHandler(t, upstream, 0)

	for i := range 2 {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/r", nil))
		if got := rr.Header().Get("X-Cache"); got != "MISS" {
			t.Fatalf("request %d X-Cache = %q, want MISS (DefaultTTL disabled)", i+1, got)
		}
	}
}

// TestDefaultTTL_NoStoreStillBypasses verifies an explicit no-store response
// is never cached even with DefaultTTL set.
func TestDefaultTTL_NoStoreStillBypasses(t *testing.T) {
	t.Parallel()
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	h := newDefaultTTLHandler(t, upstream, 5*time.Second)

	for i := range 2 {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/r", nil))
		if got := rr.Header().Get("X-Cache"); got != "MISS" {
			t.Fatalf("request %d X-Cache = %q, want MISS (no-store)", i+1, got)
		}
	}
}
