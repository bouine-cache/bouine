package cache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func origin200(body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.ETag, `"v1"`)
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
	t.Parallel()
	var originCalls int
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		originCalls++
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.ETag, `"v1"`)
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
	if rr1.Header().Get(header.XCache) != "MISS" {
		t.Fatalf("req1 X-Cache = %q", rr1.Header().Get(header.XCache))
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
	if rr2.Header().Get(header.XCache) != "HIT" {
		t.Fatalf("req2 X-Cache = %q", rr2.Header().Get(header.XCache))
	}
	if rr2.Body.String() != "cached-body" {
		t.Fatalf("req2 body = %q", rr2.Body.String())
	}

	// Age header should be present.
	if rr2.Header().Get(header.Age) == "" {
		t.Fatal("req2 missing Age header")
	}

	// Origin should have been called only once.
	if originCalls != 1 {
		t.Fatalf("origin called %d times, want 1", originCalls)
	}
}

func TestHandler_NoStoreNotCached(t *testing.T) {
	t.Parallel()
	var calls int
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set(header.CacheControl, "no-store")
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

func TestHandler_PostInvalidatesAndStores(t *testing.T) {
	t.Parallel()
	// RFC 9111 §4.4: POST invalidates cached GET response.
	// RFC 9111 §4.3.1: cacheable POST response stored under GET key.
	h := testHandler(t, origin200("body"))

	// Populate cache with GET.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/res", nil))
	if rr.Header().Get(header.XCache) != "MISS" {
		t.Fatal("expected MISS for initial GET")
	}

	// POST invalidates cache AND stores the cacheable response under GET key.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("POST", "http://example.com/res", strings.NewReader("data")))

	// GET — should be HIT (POST response stored after invalidation).
	rr3 := httptest.NewRecorder()
	h.ServeHTTP(rr3, httptest.NewRequest("GET", "http://example.com/res", nil))
	if rr3.Header().Get(header.XCache) != "HIT" {
		t.Fatalf("expected HIT for GET after POST (cacheable), got %q", rr3.Header().Get(header.XCache))
	}
}

func TestHandler_InvalidateLocation_BarePath(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.ContentLocation, "/other")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	}))

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("GET", "http://example.com/other", nil))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/other", nil))
	if rr.Header().Get(header.XCache) != "HIT" {
		t.Fatalf("precondition: expected HIT for /other, got %q", rr.Header().Get(header.XCache))
	}

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("POST", "http://example.com/submit", strings.NewReader("data")))

	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/other", nil))
	if rr2.Header().Get(header.XCache) == "HIT" {
		t.Fatalf("Content-Location /other was not invalidated by POST /submit")
	}
}

func TestHandler_InvalidateLocation_BarePathWithQueryOnPost(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.ContentLocation, "/other")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	}))

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("GET", "http://example.com/other", nil))

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("POST", "http://example.com/submit?ref=1", strings.NewReader("data")))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/other", nil))
	if rr.Header().Get(header.XCache) == "HIT" {
		t.Fatalf("POST query string leaked into Content-Location key; /other not invalidated")
	}
}

func TestHandler_InvalidateLocation_AbsoluteURL(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.ContentLocation, "http://example.com:80/cdn/v2.json?x=1")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	}))

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("GET", "http://example.com/cdn/v2.json?x=1", nil))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/cdn/v2.json?x=1", nil))
	if rr.Header().Get(header.XCache) != "HIT" {
		t.Fatalf("precondition: expected HIT, got %q", rr.Header().Get(header.XCache))
	}

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("POST", "http://example.com/submit", strings.NewReader("data")))

	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/cdn/v2.json?x=1", nil))
	if rr2.Header().Get(header.XCache) == "HIT" {
		t.Fatalf("absolute Content-Location was not invalidated")
	}
}

func TestHandler_InvalidateLocation_RelativePath(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.ContentLocation, "../v2.json")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	}))

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("GET", "http://example.com/api/v2.json", nil))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/api/v2.json", nil))
	if rr.Header().Get(header.XCache) != "HIT" {
		t.Fatalf("precondition: expected HIT, got %q", rr.Header().Get(header.XCache))
	}

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("POST", "http://example.com/api/sub/submit", strings.NewReader("data")))

	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/api/v2.json", nil))
	if rr2.Header().Get(header.XCache) == "HIT" {
		t.Fatalf("relative Content-Location ../v2.json was not invalidated")
	}
}

func TestHandler_InvalidateLocation_DifferentHost(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.ContentLocation, "http://other.example.com/resource")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	}))

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("GET", "http://other.example.com/resource", nil))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://other.example.com/resource", nil))
	if rr.Header().Get(header.XCache) != "HIT" {
		t.Fatalf("precondition: expected HIT, got %q", rr.Header().Get(header.XCache))
	}

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("POST", "http://example.com/submit", strings.NewReader("data")))

	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://other.example.com/resource", nil))
	if rr2.Header().Get(header.XCache) == "HIT" {
		t.Fatalf("cross-host Content-Location was not invalidated")
	}
}

func TestHandler_InvalidateLocation_LocationHeader(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.Location, "/redirect-target")
		w.WriteHeader(201)
		_, _ = io.WriteString(w, "created")
	}))

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("GET", "http://example.com/redirect-target", nil))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/redirect-target", nil))
	if rr.Header().Get(header.XCache) != "HIT" {
		t.Fatalf("precondition: expected HIT, got %q", rr.Header().Get(header.XCache))
	}

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("POST", "http://example.com/create", strings.NewReader("data")))

	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/redirect-target", nil))
	if rr2.Header().Get(header.XCache) == "HIT" {
		t.Fatalf("Location header was not invalidated")
	}
}

func TestHandler_BypassOnRequestNoStore(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))

	// Populate cache.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/bp", nil))

	// Request with no-store bypasses cache — goes directly to upstream.
	req := httptest.NewRequest("GET", "http://example.com/bp", nil)
	req.Header.Set(header.CacheControl, "no-store")
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, req)
	if rr2.Code != 200 {
		t.Fatalf("bypass status = %d", rr2.Code)
	}
}

func TestHandler_BypassOnRequestNoStoreWithOtherDirectives(t *testing.T) {
	t.Parallel()
	cases := []string{
		"no-store, max-age=60",
		"max-age=60, no-store",
		" no-store ",
		"NO-STORE",
	}
	for _, cc := range cases {
		t.Run(cc, func(t *testing.T) {
			t.Parallel()
			var originCalls atomic.Int32
			upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				originCalls.Add(1)
				origin200("body").ServeHTTP(w, r)
			})
			h := testHandler(t, upstream)

			h.ServeHTTP(httptest.NewRecorder(),
				httptest.NewRequest("GET", "http://example.com/ns", nil))

			req := httptest.NewRequest("GET", "http://example.com/ns", nil)
			req.Header.Set(header.CacheControl, cc)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != 200 {
				t.Fatalf("bypass status = %d", rr.Code)
			}
			if got := originCalls.Load(); got != 2 {
				t.Fatalf("origin called %d times, want 2 (no-store must bypass)", got)
			}
		})
	}
}

func TestHandler_HeadServedFromCache(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("full-body"))

	// Populate with GET.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/hd", nil))

	// HEAD should hit cache but not return body.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("HEAD", "http://example.com/hd", nil))
	if rr2.Code != 200 {
		t.Fatalf("HEAD status = %d", rr2.Code)
	}
	if rr2.Header().Get(header.XCache) != "HIT" {
		t.Fatalf("HEAD X-Cache = %q", rr2.Header().Get(header.XCache))
	}
	if rr2.Body.Len() != 0 {
		t.Fatalf("HEAD should have empty body, got %d bytes", rr2.Body.Len())
	}
}

func testHandlerStayinAlive(t *testing.T, upstream http.Handler) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  1 << 20,
		NumShards: 2,
	})
	return NewHandler(HandlerConfig{
		Upstream:    upstream,
		Store:       store,
		StayinAlive: true,
	})
}

func TestHandler_StayinAlive_ServesStaleon5xx(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		errorStatus int
		cachedBody  string
	}{
		{"503", 503, "healthy-body"},
		{"500", 500, "alive-body"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				if calls == 1 {
					w.Header().Set(header.CacheControl, "max-age=1")
					w.WriteHeader(200)
					_, _ = io.WriteString(w, tc.cachedBody)
					return
				}
				w.WriteHeader(tc.errorStatus)
			})

			h := testHandlerStayinAlive(t, upstream)

			rr1 := httptest.NewRecorder()
			h.ServeHTTP(rr1, httptest.NewRequest("GET", "http://example.com/sa", nil))
			if rr1.Code != 200 {
				t.Fatalf("populate: status = %d", rr1.Code)
			}

			rr2 := httptest.NewRecorder()
			req2 := httptest.NewRequest("GET", "http://example.com/sa", nil)
			req2.Header.Set(header.CacheControl, "no-cache")
			h.ServeHTTP(rr2, req2)
			if rr2.Code != 200 {
				t.Fatalf("stayin-alive: status = %d, want 200 (stale served)", rr2.Code)
			}
			if !strings.Contains(rr2.Body.String(), tc.cachedBody) {
				t.Fatalf("stayin-alive: body = %q, want cached body", rr2.Body.String())
			}
		})
	}
}

// TestHandler_Revalidate_5xx_StaleFallbackGateConsistency verifies that the
// revalidate 5xx stale-fallback path uses the same gate as the miss path
// (staleFallbackAllowed). A stored response carrying no-cache or s-maxage
// must NOT be served stale on a 5xx — those directives require a successful
// revalidation. This is a regression test for the divergent inline gate that
// only checked must-revalidate/proxy-revalidate.
func TestHandler_Revalidate_5xx_StaleFallbackGateConsistency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cacheCtrl string
	}{
		{
			name:      "no-cache stored response",
			cacheCtrl: "no-cache", // respCC.NoCache triggers revalidate via evalNoCache
		},
		{
			name:      "s-maxage=0 stored response",
			cacheCtrl: "s-maxage=0", // SMaxAgeSet + stale triggers revalidate via evalStale
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				if calls == 1 {
					w.Header().Set(header.CacheControl, tc.cacheCtrl)
					w.Header().Set(header.ETag, `"v1"`)
					w.WriteHeader(200)
					_, _ = io.WriteString(w, "fresh-body")
					return
				}
				// Revalidation: upstream returns 5xx.
				w.WriteHeader(503)
			})

			h := testHandler(t, upstream)

			// Seed cache.
			seed := httptest.NewRecorder()
			h.ServeHTTP(seed, httptest.NewRequest("GET", "http://example.com/gate", nil))
			if seed.Code != 200 {
				t.Fatalf("seed: status = %d", seed.Code)
			}

			// Second request triggers revalidation; upstream returns 5xx.
			rr := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "http://example.com/gate", nil)
			h.ServeHTTP(rr, req)

			// staleFallbackAllowed returns false for no-cache / s-maxage,
			// so the 5xx must be forwarded to the client — not served stale.
			if rr.Code != 503 {
				t.Fatalf("revalidate 5xx: status = %d, want 503 (stale must NOT be served for %s)",
					rr.Code, tc.cacheCtrl)
			}
			if rr.Header().Get(header.XCache) != "MISS" {
				t.Fatalf("revalidate 5xx: X-Cache = %q, want MISS", rr.Header().Get(header.XCache))
			}
			if strings.Contains(rr.Body.String(), "fresh-body") {
				t.Fatalf("revalidate 5xx: stale body served for %s, should be forwarded 5xx",
					tc.cacheCtrl)
			}
		})
	}
}

// TestHandler_StayinAlive_AgeNotInflatedByUpstreamLatency verifies that when
// the upstream is slow and returns 5xx, the stale object's Age header is
// computed from the request-start timestamp, not from a second time.Now()
// taken after the slow upstream returns. This is a regression test for the
// double time.Now() bug in fetchAndStoreStayinAlive and revalidate.
func TestHandler_StayinAlive_AgeNotInflatedByUpstreamLatency(t *testing.T) {
	t.Parallel()

	const upstreamDelay = 500 * time.Millisecond
	const staleAge = 700 * time.Millisecond
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set(header.CacheControl, "max-age=1")
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "age-body")
			return
		}
		time.Sleep(upstreamDelay)
		w.WriteHeader(503)
	})

	h := testHandlerStayinAlive(t, upstream)
	url := "http://example.com/age-test"

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))

	key := BuildKey(httptest.NewRequest("GET", url, nil))
	obj, _, _ := h.store.Get(context.Background(), key)
	if obj == nil {
		t.Fatal("object not stored after seed")
	}
	stale := *obj
	stale.StoredAt = time.Now().Add(-staleAge)
	_ = h.store.Put(context.Background(), key, &stale)

	reqStart := time.Now()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", url, nil)
	req.Header.Set(header.CacheControl, "no-cache")
	h.ServeHTTP(rr, req)

	if rr.Code != 200 {
		t.Fatalf("status = %d, want 200 (stale served)", rr.Code)
	}

	ageStr := rr.Header().Get(header.Age)
	ageSecs, err := strconv.Atoi(ageStr)
	if err != nil {
		t.Fatalf("Age header = %q, not an integer: %v", ageStr, err)
	}

	expectedMin := int(reqStart.Sub(stale.StoredAt).Seconds())
	expectedMax := int(reqStart.Add(50 * time.Millisecond).Sub(stale.StoredAt).Seconds())

	if ageSecs < expectedMin || ageSecs > expectedMax {
		t.Fatalf("Age = %d (stored %v before request, upstream slept %v); "+
			"expected %d-%d — a second time.Now() would inflate it to %d",
			ageSecs, staleAge, upstreamDelay, expectedMin, expectedMax,
			int(reqStart.Add(upstreamDelay).Sub(stale.StoredAt).Seconds()))
	}
}

// TestMaxVariants_CapIsEnforced verifies that once MaxVariants distinct
// Vary variants are stored for a primary key, subsequent variants are
// silently dropped and VaryCapHits is incremented.
func TestMaxVariants_CapIsEnforced(t *testing.T) {
	t.Parallel()
	hitCount := 0

	orig := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=3600")
		w.Header().Set(header.Vary, "X-Test-Variant")
		_, _ = w.Write([]byte("body"))
	})

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20})
	h := NewHandler(HandlerConfig{
		Upstream:    orig,
		Store:       store,
		Logger:      slog.Default(),
		VaryCapHits: counterFunc(func() { hitCount++ }),
	})

	// Fill exactly MaxVariants distinct variants.
	for i := range MaxVariants {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "http://example.com/vary", nil)
		req.Header.Set("X-Test-Variant", strconv.Itoa(i))
		h.ServeHTTP(rr, req)
	}
	if hitCount != 0 {
		t.Fatalf("expected 0 cap hits before limit, got %d", hitCount)
	}

	// One more should trip the cap.
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://example.com/vary", nil)
	req.Header.Set("X-Test-Variant", "overflow")
	h.ServeHTTP(rr, req)
	if hitCount != 1 {
		t.Fatalf("expected 1 cap hit, got %d", hitCount)
	}
}

type counterFunc func()

func (f counterFunc) Inc() { f() }

func TestMaxVariants_OverwriteDoesNotDoubleCount(t *testing.T) {
	t.Parallel()
	hitCount := 0

	orig := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=3600")
		w.Header().Set(header.Vary, "X-Test-Variant")
		_, _ = w.Write([]byte("body"))
	})

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20})
	h := NewHandler(HandlerConfig{
		Upstream:    orig,
		Store:       store,
		Logger:      slog.Default(),
		VaryCapHits: counterFunc(func() { hitCount++ }),
	})

	for range 100 {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "http://example.com/vary", nil)
		req.Header.Set("X-Test-Variant", "same")
		h.ServeHTTP(rr, req)
	}
	if hitCount != 0 {
		t.Fatalf("expected 0 cap hits for repeated overwrite, got %d", hitCount)
	}
}

func TestMaxVariants_CapRecoversAfterEviction(t *testing.T) {
	t.Parallel()
	hitCount := 0

	orig := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=3600")
		w.Header().Set(header.Vary, "X-Test-Variant")
		_, _ = w.Write([]byte("body"))
	})

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 2 << 10})
	h := NewHandler(HandlerConfig{
		Upstream:    orig,
		Store:       store,
		Logger:      slog.Default(),
		VaryCapHits: counterFunc(func() { hitCount++ }),
	})

	for i := range MaxVariants + 10 {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "http://example.com/vary", nil)
		req.Header.Set("X-Test-Variant", strconv.Itoa(i))
		h.ServeHTTP(rr, req)
	}
	if hitCount > 0 {
		t.Fatalf("expected 0 cap hits after eviction reconcile, got %d", hitCount)
	}
}

func TestMaxVariants_PrimaryKeyEvictionResetsSet(t *testing.T) {
	t.Parallel()

	orig := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=3600")
		w.Header().Set(header.Vary, "X-Test-Variant")
		_, _ = w.Write([]byte("body"))
	})

	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:       4 << 20,
		ReaperInterval: -1,
	})
	h := NewHandler(HandlerConfig{
		Upstream: orig,
		Store:    store,
		Logger:   slog.Default(),
	})

	req1 := httptest.NewRequest("GET", "http://example.com/vary", nil)
	req1.Header.Set("X-Test-Variant", "a")
	h.ServeHTTP(httptest.NewRecorder(), req1)

	primaryKey := h.buildKey(req1)
	storeKey := VariantKey(primaryKey, "X-Test-Variant", req1.Header, nil)

	h.variantMu.Lock()
	set := h.variantSets[primaryKey]
	if set == nil || len(set) != 1 {
		h.variantMu.Unlock()
		t.Fatalf("expected 1 variant in set, got %v", set)
	}
	if _, ok := set[storeKey]; !ok {
		h.variantMu.Unlock()
		t.Fatalf("expected storeKey in set")
	}
	h.variantMu.Unlock()

	// Simulate silent SIEVE eviction: the primary key and its variant
	// are removed from the store without notifying variantSets (unlike
	// the explicit Delete path in invalidateAndProxy).
	_ = store.Delete(context.Background(), primaryKey)
	_ = store.Delete(context.Background(), storeKey)

	pkObj, _, _ := store.Get(context.Background(), primaryKey)
	if pkObj != nil {
		t.Fatal("primary key should be gone from store")
	}

	// Request a new variant. The handler should detect the evicted
	// primary key and reset the stale variant set.
	req2 := httptest.NewRequest("GET", "http://example.com/vary", nil)
	req2.Header.Set("X-Test-Variant", "b")
	h.ServeHTTP(httptest.NewRecorder(), req2)

	newStoreKey := VariantKey(primaryKey, "X-Test-Variant", req2.Header, nil)

	h.variantMu.Lock()
	set = h.variantSets[primaryKey]
	if set == nil {
		h.variantMu.Unlock()
		t.Fatal("expected variant set to exist after re-store")
	}
	if len(set) != 1 {
		h.variantMu.Unlock()
		t.Fatalf("expected 1 variant after reset, got %d (stale entries not cleaned up)", len(set))
	}
	if _, ok := set[newStoreKey]; !ok {
		h.variantMu.Unlock()
		t.Fatalf("expected new storeKey in set after reset")
	}
	h.variantMu.Unlock()
}

func TestHandler_EventualNoPeerFetch(t *testing.T) {
	t.Parallel()
	// In eventual mode, ownerFn and peerFetch are nil. A miss goes
	// straight to origin without attempting peer fetch.
	originCalls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		originCalls++
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "eventual-body")
	})
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream: upstream,
		Store:    store,
		// ownerFn and peerFetch are nil (default zero-value) —
		// simulates eventual mode where no peer fetch occurs.
	})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/e", nil))
	if rr.Code != 200 {
		t.Fatalf("status = %d", rr.Code)
	}
	if originCalls != 1 {
		t.Fatalf("expected 1 origin call, got %d", originCalls)
	}
	if rr.Header().Get(header.XCache) != "MISS" {
		t.Fatalf("X-Cache = %q, want MISS", rr.Header().Get(header.XCache))
	}

	// Second request should be a HIT — served from local cache.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/e", nil))
	if rr2.Header().Get(header.XCache) != "HIT" {
		t.Fatalf("X-Cache = %q, want HIT", rr2.Header().Get(header.XCache))
	}
}
func TestHandler_BanByPathRegex(t *testing.T) {
	t.Parallel()
	var originCalls atomic.Int64
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		originCalls.Add(1)
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "cached-body")
	})
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  1 << 20,
		NumShards: 2,
	})
	h := NewHandler(HandlerConfig{Upstream: upstream, Store: store})

	// Warm the cache.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/ban-me", nil))
	if rr.Header().Get(header.XCache) != "MISS" {
		t.Fatalf("warmup should be MISS, got %q", rr.Header().Get(header.XCache))
	}

	// Second request — HIT.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/ban-me", nil))
	if rr2.Header().Get(header.XCache) != "HIT" {
		t.Fatalf("expected HIT, got %q", rr2.Header().Get(header.XCache))
	}

	// Ban by path regex.
	count, err := store.Ban(context.Background(), api.BanExpr{
		PathRegex: "^/ban-me",
	})
	if err != nil {
		t.Fatalf("ban failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("ban count = %d, want 1", count)
	}

	// After ban — should be MISS (re-fetch from origin).
	rr3 := httptest.NewRecorder()
	h.ServeHTTP(rr3, httptest.NewRequest("GET", "http://example.com/ban-me", nil))
	if rr3.Header().Get(header.XCache) != "MISS" {
		t.Fatalf("after ban expected MISS, got %q", rr3.Header().Get(header.XCache))
	}
}

func TestHandler_BanByHostRegex(t *testing.T) {
	t.Parallel()
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "cached-body")
	})
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  1 << 20,
		NumShards: 2,
	})
	h := NewHandler(HandlerConfig{Upstream: upstream, Store: store})

	// Warm the cache.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/foo", nil))
	if rr.Header().Get(header.XCache) != "MISS" {
		t.Fatalf("warmup should be MISS")
	}

	// Ban by host regex.
	count, err := store.Ban(context.Background(), api.BanExpr{
		HostRegex: "example.com",
	})
	if err != nil {
		t.Fatalf("ban failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("ban count = %d, want 1", count)
	}

	// After ban — should be MISS.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/foo", nil))
	if rr2.Header().Get(header.XCache) != "MISS" {
		t.Fatalf("after ban expected MISS, got %q", rr2.Header().Get(header.XCache))
	}
}

func TestHandler_ServeObjectStripsInternalHeaders(t *testing.T) {
	t.Parallel()
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "cached-body")
	})
	h := testHandler(t, upstream)

	// Warm and serve from cache.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/foo", nil))

	// Internal headers must not leak to the client.
	if v := rr.Header().Get(header.XBouinePath); v != "" {
		t.Fatalf("X-Bouine-Path leaked to client: %q", v)
	}
	if v := rr.Header().Get(header.XBouineHost); v != "" {
		t.Fatalf("X-Bouine-Host leaked to client: %q", v)
	}
}
func TestReleaseRecorder_DiscardsOversizedBuffer(t *testing.T) {
	// sync.Pool is per-P and may be cleared by GC, both of which mask the
	// regression. Pin to a single P and disable GC so Put→Get on the same
	// P reliably returns the just-put recorder when it was not discarded.
	prevGC := debug.SetGCPercent(-1)
	t.Cleanup(func() { debug.SetGCPercent(prevGC) })
	prevProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(prevProcs) })

	rec := acquireRecorder(0)
	if _, err := rec.body.Write(make([]byte, maxRecorderCap+1)); err != nil {
		t.Fatalf("write: %v", err)
	}
	releaseRecorder(rec)

	fresh := acquireRecorder(0)
	if fresh.body.Cap() > maxRecorderCap {
		t.Fatalf("reacquired recorder retained oversized buffer: cap=%d > %d",
			fresh.body.Cap(), maxRecorderCap)
	}
	releaseRecorder(fresh)
}

func TestHandler_FetchSemaphoreBoundsConcurrentFetches(t *testing.T) {
	t.Parallel()
	const maxConc = 2
	var inFlight, maxInFlight atomic.Int32
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := inFlight.Add(1)
		for {
			old := maxInFlight.Load()
			if cur <= old || maxInFlight.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inFlight.Add(-1)
		w.Header().Set("Cache-Control", "max-age=0")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream:            upstream,
		Store:               store,
		MaxFetchConcurrency: maxConc,
	})

	var wg sync.WaitGroup
	for i := 0; i < maxConc*3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			url := "http://example.com/sem-test-" + strconv.Itoa(i)
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))
		}(i)
	}
	wg.Wait()

	if got := maxInFlight.Load(); got > int32(maxConc) {
		t.Fatalf("max concurrent origin fetches = %d, want <= %d", got, maxConc)
	}
}

// testRefreshHandler creates a Handler with refresh-before-expiry enabled
// and the given min-hits threshold. The upstream returns 304 for
// conditional requests (If-None-Match), so background refreshes succeed
// without generating full 200 responses.
func testRefreshHandler(t *testing.T, minHits int) *Handler {
	return testRefreshHandlerWithPersist(t, minHits, 0)
}

// testRefreshHandlerWithPersist creates a Handler with refresh-before-expiry
// enabled, the given min-hits threshold and persist cycles.
func testRefreshHandlerWithPersist(t *testing.T, minHits, persistCycles int) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  1 << 20,
		NumShards: 2,
	})
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if etag := r.Header.Get("If-None-Match"); etag == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.ETag, `"v1"`)
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	h := NewHandler(HandlerConfig{
		Upstream:             upstream,
		Store:                store,
		Logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		DefaultTTL:           60 * time.Second,
		RefreshBeforeExpiry:  true,
		RefreshMargin:        6 * time.Second,
		RefreshTimeout:       5 * time.Second,
		RefreshConcurrency:   4,
		RefreshMinHits:       minHits,
		RefreshPersistCycles: persistCycles,
		RouteName:            "test",
	})
	return h
}

func TestRefreshMinHits_UnpopularObjectNotRescheduled(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 2)
	defer h.Close(context.Background())

	// Store an object via foreground path (isRefresh=false → always
	// schedules regardless of min-hits).
	req := httptest.NewRequest("GET", "http://example.com/page", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	// The initial store scheduled 1 entry. Stop the scheduler to clear
	// it, then create a fresh one so we can detect whether the refresh
	// re-schedules.
	h.scheduler.Stop()
	h.scheduler = NewRefreshScheduler(h.triggerBgRefresh, h.lookupForRefresh)
	h.scheduler.Start()

	ctx := context.Background()
	key := h.buildKey(httptest.NewRequest("GET", "http://example.com/page", nil))
	obj, _, err := h.store.Get(ctx, key)
	if err != nil || obj == nil {
		t.Fatalf("store.Get: obj=%v err=%v", obj, err)
	}
	// After the test's Get, Hits=1 (first access, slow path). With
	// minHits=2, the gate should block re-scheduling.
	if obj.Hits != 1 {
		t.Fatalf("expected 1 hit after Get, got %d", obj.Hits)
	}
	h.doBackgroundRefresh(ctx, key, obj, 0)

	// After refresh with Hits=1 < minHits=2, the object should NOT be
	// re-scheduled. The scheduler should still be empty.
	scheduled := h.scheduler.Len()
	if scheduled != 0 {
		t.Fatalf("expected 0 scheduled entries after unpopular refresh, got %d", scheduled)
	}
}

func TestRefreshMinHits_PopularObjectRescheduled(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	defer h.Close(context.Background())

	// Store (MISS) then access (HIT → Hits=1 on slow path).
	// The test's store.Get will use the fast path (visited=true),
	// so Hits stays at 1.
	url := "http://example.com/popular"
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))

	// Clear the initial scheduler entry to detect re-scheduling.
	h.scheduler.Stop()
	h.scheduler = NewRefreshScheduler(h.triggerBgRefresh, h.lookupForRefresh)
	h.scheduler.Start()

	key := h.buildKey(httptest.NewRequest("GET", url, nil))
	obj, _, err := h.store.Get(context.Background(), key)
	if err != nil || obj == nil {
		t.Fatalf("store.Get: obj=%v err=%v", obj, err)
	}
	if obj.Hits < 1 {
		t.Fatalf("expected >= 1 hits, got %d", obj.Hits)
	}

	// Simulate a background refresh. Since the object was accessed
	// (windowHits >= refreshMinHits), it should be re-scheduled.
	staleHits := h.store.WindowHits(key)
	h.doBackgroundRefresh(context.Background(), key, obj, staleHits)

	scheduled := h.scheduler.Len()
	if scheduled != 1 {
		t.Fatalf("expected 1 scheduled entry after popular refresh, got %d", scheduled)
	}
}

func TestRefreshMinHits_InitialStoreAlwaysSchedules(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 10) // high threshold
	defer h.Close(context.Background())

	// A foreground store (isRefresh=false) must schedule even though
	// Hits == 0 < refreshMinHits (10). Every object gets one chance.
	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("GET", "http://example.com/first-chance", nil))

	scheduled := h.scheduler.Len()
	if scheduled != 1 {
		t.Fatalf("initial store should schedule 1 entry, got %d", scheduled)
	}
}

func TestRefresh_HitCountResetOn200Refresh(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  1 << 20,
		NumShards: 2,
	})
	// Upstream always returns 200 (never 304), so the 200 refresh
	// path is exercised.
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.ETag, `"v1"`)
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	h := NewHandler(HandlerConfig{
		Upstream:            upstream,
		Store:               store,
		Logger:              slog.New(slog.NewTextHandler(io.Discard, nil)),
		DefaultTTL:          60 * time.Second,
		RefreshBeforeExpiry: true,
		RefreshMargin:       6 * time.Second,
		RefreshTimeout:      5 * time.Second,
		RefreshConcurrency:  4,
		RefreshMinHits:      0,
		RouteName:           "test",
	})
	defer h.Close(context.Background())

	// Store (MISS) then access (HIT → Hits=1).
	url := "http://example.com/hits"
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))

	key := h.buildKey(httptest.NewRequest("GET", url, nil))
	obj, _, err := h.store.Get(context.Background(), key)
	if err != nil || obj == nil {
		t.Fatalf("store.Get: obj=%v err=%v", obj, err)
	}
	if obj.Hits < 1 {
		t.Fatalf("expected >= 1 hit before refresh, got %d", obj.Hits)
	}

	// doBackgroundRefresh triggers a 200 (upstream never returns 304).
	// The refreshed object should have Hits reset to 0 (SIEVE signal).
	// windowHits from the previous window is used for the popularity gate.
	h.doBackgroundRefresh(context.Background(), key, obj, 0)

	// Verify the stored object has Hits reset (may be 1 from SIEVE slow path
	// on the test's store.Get, but should not carry over the previous window's count).
	refreshed, _, err := h.store.Get(context.Background(), key)
	if err != nil || refreshed == nil {
		t.Fatalf("store.Get after refresh: obj=%v err=%v", refreshed, err)
	}
	if refreshed.Hits > 1 {
		t.Fatalf("hit count not reset after 200 refresh: got %d, want <= 1", refreshed.Hits)
	}
}

func TestRefreshPersistCycles_UnpopularObjectPersistsThenExpires(t *testing.T) {
	t.Parallel()
	h := testRefreshHandlerWithPersist(t, 2, 3)
	defer h.Close(context.Background())

	url := "http://example.com/persist"
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))

	// Clear initial scheduler entry.
	h.scheduler.Stop()
	h.scheduler = NewRefreshScheduler(h.triggerBgRefresh, h.lookupForRefresh)
	h.scheduler.Start()

	key := h.buildKey(httptest.NewRequest("GET", url, nil))
	obj, _, err := h.store.Get(context.Background(), key)
	if err != nil || obj == nil {
		t.Fatalf("store.Get: obj=%v err=%v", obj, err)
	}
	// After Get, Hits=1 (visited bit flip). With minHits=2, the gate
	// would block. But persist=3 should keep it alive for 3 more cycles.
	// We pass staleHits=0 to each doBackgroundRefresh — the 304 path
	// resets Hits to 0 and the gate checks staleHits (0 < minHits=2).

	// Refresh 1: Hits=1 < minHits=2, persist=3 → decrement to 2, re-schedule.
	h.doBackgroundRefresh(context.Background(), key, obj, 0)
	if scheduled := h.scheduler.Len(); scheduled != 1 {
		t.Fatalf("after 1st persist refresh: expected 1 scheduled, got %d", scheduled)
	}

	// Clear scheduler to detect next re-schedule.
	h.scheduler.Stop()
	h.scheduler = NewRefreshScheduler(h.triggerBgRefresh, h.lookupForRefresh)
	h.scheduler.Start()

	// Refresh 2: persist=2 → decrement to 1, re-schedule.
	h.doBackgroundRefresh(context.Background(), key, obj, 0)
	if scheduled := h.scheduler.Len(); scheduled != 1 {
		t.Fatalf("after 2nd persist refresh: expected 1 scheduled, got %d", scheduled)
	}

	h.scheduler.Stop()
	h.scheduler = NewRefreshScheduler(h.triggerBgRefresh, h.lookupForRefresh)
	h.scheduler.Start()

	// Refresh 3: persist=1 → decrement to 0, re-schedule.
	h.doBackgroundRefresh(context.Background(), key, obj, 0)
	if scheduled := h.scheduler.Len(); scheduled != 1 {
		t.Fatalf("after 3rd persist refresh: expected 1 scheduled, got %d", scheduled)
	}

	h.scheduler.Stop()
	h.scheduler = NewRefreshScheduler(h.triggerBgRefresh, h.lookupForRefresh)
	h.scheduler.Start()

	// Refresh 4: persist=0 → gate blocks, no re-schedule.
	h.doBackgroundRefresh(context.Background(), key, obj, 0)
	if scheduled := h.scheduler.Len(); scheduled != 0 {
		t.Fatalf("after persist exhausted: expected 0 scheduled, got %d", scheduled)
	}
}

func TestRefreshPersistCycles_PopularRefreshResetsCounter(t *testing.T) {
	t.Parallel()
	// Use minHits=2, persist=2 so we can have an unpopular phase (Hits=1)
	// and then a popular phase (Hits=2 after a client HIT).
	h := testRefreshHandlerWithPersist(t, 2, 2)
	defer h.Close(context.Background())

	url := "http://example.com/reset"
	// MISS → Hits=1 (visited bit flip). Registered with persist=2.
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))

	// Clear scheduler.
	h.scheduler.Stop()
	h.scheduler = NewRefreshScheduler(h.triggerBgRefresh, h.lookupForRefresh)
	h.scheduler.Start()

	key := h.buildKey(httptest.NewRequest("GET", url, nil))
	obj, _, err := h.store.Get(context.Background(), key)
	if err != nil || obj == nil {
		t.Fatalf("store.Get: obj=%v err=%v", obj, err)
	}
	if obj.Hits != 1 {
		t.Fatalf("expected 1 hit after MISS, got %d", obj.Hits)
	}

	// Unpopular refresh: windowHits=0 < minHits=2 → persist 2→1, re-scheduled.
	staleHits := h.store.WindowHits(key)
	h.doBackgroundRefresh(context.Background(), key, obj, staleHits)
	if scheduled := h.scheduler.Len(); scheduled != 1 {
		t.Fatalf("persist should re-schedule, got %d", scheduled)
	}
	entry := h.refreshRegistry.Lookup(key)
	if entry == nil || entry.persistCycles != 1 {
		t.Fatalf("after unpopular refresh: persist should be 1, got %v", entry)
	}

	// Client HITs → windowHits incremented. After refresh reset,
	// two HITs are needed to pass the minHits=2 gate.
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))

	// Re-read obj to get updated Hits.
	obj, _, err = h.store.Get(context.Background(), key)
	if err != nil || obj == nil {
		t.Fatalf("store.Get after HIT: obj=%v err=%v", obj, err)
	}
	wh := h.store.WindowHits(key)
	if wh < 2 {
		t.Fatalf("expected >= 2 windowHits after HITs, got %d", wh)
	}

	// Clear scheduler.
	h.scheduler.Stop()
	h.scheduler = NewRefreshScheduler(h.triggerBgRefresh, h.lookupForRefresh)
	h.scheduler.Start()

	// Popular refresh: windowHits >= minHits → Register called → persist RESET to 2.
	staleHits = h.store.WindowHits(key)
	h.doBackgroundRefresh(context.Background(), key, obj, staleHits)
	if scheduled := h.scheduler.Len(); scheduled != 1 {
		t.Fatalf("popular refresh should re-schedule, got %d", scheduled)
	}
	entry = h.refreshRegistry.Lookup(key)
	if entry == nil {
		t.Fatal("registry entry should exist after popular refresh")
	}
	if entry.persistCycles != 2 {
		t.Fatalf("persist should be reset to 2 after popular refresh, got %d", entry.persistCycles)
	}
}

func TestRefreshPersistCycles_ZeroPersistBlocksImmediately(t *testing.T) {
	t.Parallel()
	// persist=0 means the gate works as before (no persistence).
	h := testRefreshHandlerWithPersist(t, 2, 0)
	defer h.Close(context.Background())

	url := "http://example.com/no-persist"
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))

	h.scheduler.Stop()
	h.scheduler = NewRefreshScheduler(h.triggerBgRefresh, h.lookupForRefresh)
	h.scheduler.Start()

	key := h.buildKey(httptest.NewRequest("GET", url, nil))
	obj, _, err := h.store.Get(context.Background(), key)
	if err != nil || obj == nil {
		t.Fatalf("store.Get: obj=%v err=%v", obj, err)
	}
	if obj.Hits != 1 {
		t.Fatalf("expected 1 hit, got %d", obj.Hits)
	}

	// Hits=1 < minHits=2, persist=0 → gate blocks immediately.
	h.doBackgroundRefresh(context.Background(), key, obj, 0)
	if scheduled := h.scheduler.Len(); scheduled != 0 {
		t.Fatalf("with persist=0, gate should block immediately, got %d scheduled", scheduled)
	}
}

func TestRefreshPersistCycles_DecrementPersistOnMissingKey(t *testing.T) {
	t.Parallel()
	r := newRefreshRegistry()
	key := api.Key(42)

	// Key not registered → DecrementPersist returns false.
	if r.DecrementPersist(key) {
		t.Fatal("DecrementPersist should return false for unregistered key")
	}

	req := httptest.NewRequest("GET", "http://example.com/test", nil)
	r.Register(key, req, "", 2)

	// persist=2 → decrement to 1.
	if !r.DecrementPersist(key) {
		t.Fatal("DecrementPersist should return true with persist=2")
	}
	// persist=1 → decrement to 0.
	if !r.DecrementPersist(key) {
		t.Fatal("DecrementPersist should return true with persist=1")
	}
	// persist=0 → returns false.
	if r.DecrementPersist(key) {
		t.Fatal("DecrementPersist should return false when persist exhausted")
	}
}

func TestDoFetchErrAbortHandler(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	req := httptest.NewRequest("GET", "/", nil)
	res := h.doFetch(req)
	if res.Err == nil {
		t.Fatal("expected error from ErrAbortHandler, got nil")
	}
	if !errors.Is(res.Err, http.ErrAbortHandler) {
		t.Fatalf("expected error wrapping http.ErrAbortHandler, got %v", res.Err)
	}
}

func TestDoFetchRealPanicPropagates(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		panic("real bug")
	}))
	req := httptest.NewRequest("GET", "/", nil)
	defer func() {
		if rv := recover(); rv == nil {
			t.Fatal("expected real panic to propagate, got nothing")
		}
	}()
	h.doFetch(req)
}

func TestDoFetchSemaphoreReleasedAfterAbort(t *testing.T) {
	t.Parallel()
	// Use a cap-1 semaphore so doFetch's acquire is the only slot.
	// After doFetch returns, the channel must be empty — if the defer
	// failed to release, the slot would still be held.
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	h.fetchSem = make(chan struct{}, 1)
	req := httptest.NewRequest("GET", "/", nil)
	_ = h.doFetch(req)
	select {
	case <-h.fetchSem:
		t.Fatal("fetch semaphore leaked — slot still held after ErrAbortHandler")
	default:
		// good — channel is empty, doFetch released its slot
	}
}

func TestCollapsedFetchErrAbortHandler(t *testing.T) {
	t.Parallel()
	// Verify that ErrAbortHandler is converted to a clean fetchResult
	// error through the singleflight path. singleflight wraps panics in
	// *panicError and re-panics — if doFetch didn't recover, this test
	// would panic instead of returning an error.
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	req := httptest.NewRequest("GET", "/", nil)
	res := h.collapsedFetch(req, 0)
	if res.Err == nil {
		t.Fatal("expected error from collapsedFetch after ErrAbortHandler, got nil")
	}
	if !errors.Is(res.Err, http.ErrAbortHandler) {
		t.Fatalf("expected error wrapping http.ErrAbortHandler, got %v", res.Err)
	}
}

func TestDoFetchTimeoutAbortsSlowOrigin(t *testing.T) {
	t.Parallel()
	// Origin that sleeps longer than the fetch timeout. doFetch must
	// abort the fetch and return a fetchResult.Err, not hang forever.
	// The handler respects the request context so the timeout actually
	// interrupts the upstream call (a real ReverseProxy would do this
	// automatically via its Transport).
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
			w.WriteHeader(200)
		case <-r.Context().Done():
			return
		}
	}))
	h.fetchTimeout = 100 * time.Millisecond
	req := httptest.NewRequest("GET", "/", nil)
	res := h.doFetch(req)
	if res.Err == nil {
		t.Fatal("expected error from fetch timeout, got nil")
	}
}

func TestDoFetchTimeoutStartsAfterSemaphore(t *testing.T) {
	t.Parallel()
	// If the timeout started before semaphore acquire, the queueing
	// delay would eat into the fetch budget. This test fills the
	// semaphore so doFetch blocks on acquire for longer than the fetch
	// timeout, then releases it. If the timeout started before acquire,
	// the fetch would fail immediately with a deadline error. Since it
	// starts after, the fetch proceeds normally once the slot is freed.
	done := make(chan struct{})
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(done)
		w.WriteHeader(200)
	}))
	h.fetchSem = make(chan struct{}, 1)
	h.fetchSem <- struct{}{} // pre-fill the only slot
	h.fetchTimeout = 50 * time.Millisecond
	req := httptest.NewRequest("GET", "/", nil)
	go func() {
		time.Sleep(100 * time.Millisecond)
		<-h.fetchSem // release the slot after 100ms > fetchTimeout
	}()
	res := h.doFetch(req)
	select {
	case <-done:
		// good — the origin was reached, proving the timeout didn't
		// fire during the semaphore wait
	case <-time.After(5 * time.Second):
		t.Fatalf("origin never called — fetch timeout fired during semaphore wait: %v", res.Err)
	}
	if res.Err != nil {
		t.Fatalf("expected no error, got %v", res.Err)
	}
}

func TestDoFetchCanceledContextKeepsValidResponse(t *testing.T) {
	t.Parallel()
	// If the client disconnects (context cancelled) after the origin
	// returned a complete response, doFetch must still return the
	// response for caching — not discard it. Only timeout
	// (DeadlineExceeded) or an empty recorder should produce an error.
	ctx, cancel := context.WithCancel(context.Background())
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "cached-body")
		cancel() // simulate client disconnect after origin responded
	}))
	h.fetchTimeout = 5 * time.Second
	req := httptest.NewRequest("GET", "/", nil)
	req = req.WithContext(ctx)
	res := h.doFetch(req)
	if res.Err != nil {
		t.Fatalf("expected no error for completed response with cancelled context, got %v", res.Err)
	}
	if res.StatusCode != 200 {
		t.Fatalf("expected status 200, got %d", res.StatusCode)
	}
	if string(res.Body) != "cached-body" {
		t.Fatalf("expected body %q, got %q", "cached-body", res.Body)
	}
}

func TestRefreshFrom304_RecomputesSerializedHead(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))

	stale := &api.Object{
		Key:        BuildKeyFromURL("http://example.com/test"),
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=60"}, header.ETag: {`"v1"`}, "X-Sensitive": {"secret"}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now().Add(-time.Minute),
		TTL:        time.Minute,
		ETag:       `"v1"`,
	}
	stale.CacheControl = stale.Header.Get(header.CacheControl)
	stale.SerializedHead = serializeHead(stale)

	// 304 adds no-cache="X-Sensitive" — serializeHead must now skip X-Sensitive.
	res := fetchResult{
		StatusCode: 304,
		Header:     http.Header{header.CacheControl: {"max-age=3600, no-cache=\"X-Sensitive\""}, header.ETag: {`"v2"`}},
	}

	refreshed := h.refreshFrom304(stale, res)

	expected := serializeHead(refreshed)
	if !bytes.Equal(refreshed.SerializedHead, expected) {
		t.Fatalf("SerializedHead mismatch after 304 refresh:\n  got:  %q\n  want: %q", refreshed.SerializedHead, expected)
	}
	if !bytes.Contains(refreshed.SerializedHead, []byte("max-age=3600")) {
		t.Fatalf("SerializedHead does not contain updated Cache-Control: %q", refreshed.SerializedHead)
	}
	if bytes.Contains(refreshed.SerializedHead, []byte("max-age=60")) {
		t.Fatalf("SerializedHead still contains old Cache-Control value")
	}
	if bytes.Contains(refreshed.SerializedHead, []byte("X-Sensitive: secret")) {
		t.Fatalf("SerializedHead contains X-Sensitive header, which should be skipped by no-cache directive: %q", refreshed.SerializedHead)
	}
}
