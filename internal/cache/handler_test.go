package cache

import (
	"context"
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

	"github.com/thylong/bouine/internal/storage"
	"github.com/thylong/bouine/pkg/api"
	"github.com/thylong/bouine/pkg/header"
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
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			// First call: serve a cacheable response.
			w.Header().Set(header.CacheControl, "max-age=1")
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "healthy-body")
			return
		}
		// Subsequent calls: upstream is unhealthy.
		w.WriteHeader(503)
	})

	h := testHandlerStayinAlive(t, upstream)

	// Populate cache.
	rr1 := httptest.NewRecorder()
	h.ServeHTTP(rr1, httptest.NewRequest("GET", "http://example.com/sa", nil))
	if rr1.Code != 200 {
		t.Fatalf("populate: status = %d", rr1.Code)
	}

	// Expire the entry by manipulating nothing — just request again
	// after expiry would require time travel; instead confirm that
	// revalidation with 5xx triggers stayin-alive serving.
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest("GET", "http://example.com/sa", nil)
	req2.Header.Set(header.CacheControl, "no-cache") // force revalidation
	h.ServeHTTP(rr2, req2)
	if rr2.Code != 200 {
		t.Fatalf("stayin-alive: status = %d, want 200 (stale served)", rr2.Code)
	}
	body := rr2.Body.String()
	if !strings.Contains(body, "healthy-body") {
		t.Fatalf("stayin-alive: body = %q, want cached body", body)
	}
}

func TestHandler_StayinAlive_ServesStaleonError(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set(header.CacheControl, "max-age=1")
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "alive-body")
			return
		}
		// Simulate connection error by hijacking; simplest: panic recover.
		// Instead just return 500 to test the 5xx branch.
		w.WriteHeader(500)
	})

	h := testHandlerStayinAlive(t, upstream)

	// Seed cache.
	seed := httptest.NewRecorder()
	h.ServeHTTP(seed, httptest.NewRequest("GET", "http://example.com/err", nil))

	// Force revalidation.
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://example.com/err", nil)
	req.Header.Set(header.CacheControl, "no-cache")
	h.ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("stayin-alive on 500: status = %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "alive-body") {
		t.Fatalf("body = %q, want cached", rr.Body.String())
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
	obj, _ := h.store.Get(context.Background(), key)
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

	pkObj, _ := store.Get(context.Background(), primaryKey)
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

func TestHandler_FullReplicationHook(t *testing.T) {
	t.Parallel()
	// In full mode, ReplicateFn is called after a cacheable response
	// is stored. This verifies the hook fires and receives the object.
	var replicated atomic.Int32
	replicateFn := func(_ context.Context, obj *api.Object) {
		if obj == nil {
			t.Error("replicateFn received nil object")
			return
		}
		replicated.Add(1)
	}
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "full-mode-body")
	})
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream:    upstream,
		Store:       store,
		ReplicateFn: replicateFn,
	})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/f", nil))
	if rr.Code != 200 {
		t.Fatalf("status = %d", rr.Code)
	}
	if replicated.Load() != 1 {
		t.Fatalf("replicateFn called %d times, want 1", replicated.Load())
	}
}

func TestHandler_FullReplicationHookNotCalledOnBypass(t *testing.T) {
	t.Parallel()
	// Non-cacheable responses (e.g. 200 with no Cache-Control) should
	// NOT trigger the replication hook.
	var replicated atomic.Int32
	replicateFn := func(_ context.Context, _ *api.Object) {
		replicated.Add(1)
	}
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// No Cache-Control → not cacheable.
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "no-cache-body")
	})
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream:    upstream,
		Store:       store,
		ReplicateFn: replicateFn,
	})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/nocache", nil))
	if rr.Code != 200 {
		t.Fatalf("status = %d", rr.Code)
	}
	if replicated.Load() != 0 {
		t.Fatalf("replicateFn called %d times, want 0 (non-cacheable)", replicated.Load())
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

func TestHandler_ReplicateOnForegroundRevalidate(t *testing.T) {
	t.Parallel()
	var replicated atomic.Int32
	replicateFn := func(_ context.Context, obj *api.Object) {
		if obj != nil {
			replicated.Add(1)
		}
	}
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(header.IfNoneMatch) != "" {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.ETag, `"v1"`)
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream:    upstream,
		Store:       store,
		ReplicateFn: replicateFn,
	})

	url := "http://example.com/r"
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))

	key := BuildKey(httptest.NewRequest("GET", url, nil))
	obj, _ := store.Get(context.Background(), key)
	if obj == nil {
		t.Fatal("object not stored after initial fill")
	}
	stale := *obj
	stale.TTL = 1 * time.Second
	stale.StoredAt = time.Now().Add(-2 * time.Second)
	_ = store.Put(context.Background(), key, &stale)

	replicated.Store(0)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", url, nil))
	if rr.Header().Get(header.XCache) != "REVALIDATED" {
		t.Fatalf("X-Cache = %q, want REVALIDATED", rr.Header().Get(header.XCache))
	}
	if replicated.Load() != 1 {
		t.Fatalf("replicateFn called %d times after 304 revalidation, want 1", replicated.Load())
	}
}

func TestHandler_ReplicateOnBackgroundSWR(t *testing.T) {
	t.Parallel()
	var replicated atomic.Int32
	replicateFn := func(_ context.Context, obj *api.Object) {
		if obj != nil {
			replicated.Add(1)
		}
	}
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(header.IfNoneMatch) != "" {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set(header.CacheControl, "max-age=60, stale-while-revalidate=60")
		w.Header().Set(header.ETag, `"v1"`)
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream:    upstream,
		Store:       store,
		ReplicateFn: replicateFn,
	})

	url := "http://example.com/swr"
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))

	key := BuildKey(httptest.NewRequest("GET", url, nil))
	obj, _ := store.Get(context.Background(), key)
	if obj == nil {
		t.Fatal("object not stored after initial fill")
	}
	stale := *obj
	stale.TTL = 1 * time.Second
	stale.StoredAt = time.Now().Add(-2 * time.Second)
	_ = store.Put(context.Background(), key, &stale)

	replicated.Store(0)

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if replicated.Load() > 0 {
			break
		}
		runtime.Gosched()
	}
	if replicated.Load() < 1 {
		t.Fatalf("replicateFn called %d times after background SWR, want >= 1", replicated.Load())
	}
}

func TestHandler_ReplicateOnPOSTCacheable(t *testing.T) {
	t.Parallel()
	var replicated atomic.Int32
	replicateFn := func(_ context.Context, obj *api.Object) {
		if obj != nil {
			replicated.Add(1)
		}
	}
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.Header().Set(header.CacheControl, "max-age=60")
			w.Header().Set(header.ContentLocation, "/post-resource")
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "post-body")
			return
		}
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "get-body")
	})
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream:    upstream,
		Store:       store,
		ReplicateFn: replicateFn,
	})

	postReq := httptest.NewRequest("POST", "http://example.com/post-resource", strings.NewReader("data"))
	postReq.Header.Set(header.ContentType, "application/x-www-form-urlencoded")
	h.ServeHTTP(httptest.NewRecorder(), postReq)

	if replicated.Load() != 1 {
		t.Fatalf("replicateFn called %d times after POST, want 1", replicated.Load())
	}
}

func TestHandler_ReplicateVaryPrimaryAndVariant(t *testing.T) {
	t.Parallel()
	var replicatedKeys sync.Map
	replicateFn := func(_ context.Context, obj *api.Object) {
		if obj != nil {
			replicatedKeys.Store(obj.Key, true)
		}
	}
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.Vary, "Accept-Encoding")
		w.Header().Set(header.ContentEncoding, r.Header.Get(header.AcceptEncoding))
		w.WriteHeader(200)
		_, _ = w.Write([]byte("body-" + r.Header.Get(header.AcceptEncoding)))
	})
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream:    upstream,
		Store:       store,
		ReplicateFn: replicateFn,
	})

	r := httptest.NewRequest("GET", "http://example.com/vary", nil)
	r.Header.Set(header.AcceptEncoding, "gzip")
	h.ServeHTTP(httptest.NewRecorder(), r)

	primaryKey := BuildKey(r)
	variantKey := VariantKey(primaryKey, "Accept-Encoding", r.Header)
	primaryReplicated := false
	variantReplicated := false
	replicatedKeys.Range(func(k, _ any) bool {
		if k.(api.Key) == primaryKey {
			primaryReplicated = true
		}
		if k.(api.Key) == variantKey {
			variantReplicated = true
		}
		return true
	})
	if !primaryReplicated {
		t.Fatal("primary marker was not replicated")
	}
	if !variantReplicated {
		t.Fatal("variant was not replicated")
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
