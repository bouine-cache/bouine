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

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
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
	require.Equal(t, 200, rr1.Code)
	require.Equal(t, "MISS", rr1.Header().Get(header.XCache))
	require.Equal(t, "cached-body", rr1.Body.String())

	// Second request — HIT, served from cache.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/foo", nil))
	require.Equal(t, 200, rr2.Code)
	require.Equal(t, "HIT", rr2.Header().Get(header.XCache))
	require.Equal(t, "cached-body", rr2.Body.String())

	// Age header should be present.
	require.NotEqual(t, "", rr2.Header().Get(header.Age))

	// Origin should have been called only once.
	require.Equal(t, 1, originCalls)
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
		require.Equal(t, 200, rr.Code)
	}
	require.Equal(t, 3, calls)
}

func TestHandler_PostInvalidatesAndStores(t *testing.T) {
	t.Parallel()
	// RFC 9111 §4.4: POST invalidates cached GET response.
	// RFC 9111 §4.3.1: cacheable POST response stored under GET key.
	h := testHandler(t, origin200("body"))

	// Populate cache with GET.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/res", nil))
	require.Equal(t, "MISS", rr.Header().Get(header.XCache))

	// POST invalidates cache AND stores the cacheable response under GET key.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("POST", "http://example.com/res", strings.NewReader("data")))

	// GET — should be HIT (POST response stored after invalidation).
	rr3 := httptest.NewRecorder()
	h.ServeHTTP(rr3, httptest.NewRequest("GET", "http://example.com/res", nil))
	require.Equal(t, "HIT", rr3.Header().Get(header.XCache))
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
	require.Equal(t, "HIT", rr.Header().Get(header.XCache))

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("POST", "http://example.com/submit", strings.NewReader("data")))

	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/other", nil))
	require.NotEqual(t, "HIT", rr2.Header().Get(header.XCache))
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
	require.NotEqual(t, "HIT", rr.Header().Get(header.XCache))
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
	require.Equal(t, "HIT", rr.Header().Get(header.XCache))

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("POST", "http://example.com/submit", strings.NewReader("data")))

	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/cdn/v2.json?x=1", nil))
	require.NotEqual(t, "HIT", rr2.Header().Get(header.XCache))
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
	require.Equal(t, "HIT", rr.Header().Get(header.XCache))

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("POST", "http://example.com/api/sub/submit", strings.NewReader("data")))

	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/api/v2.json", nil))
	require.NotEqual(t, "HIT", rr2.Header().Get(header.XCache))
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
	require.Equal(t, "HIT", rr.Header().Get(header.XCache))

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("POST", "http://example.com/submit", strings.NewReader("data")))

	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://other.example.com/resource", nil))
	require.NotEqual(t, "HIT", rr2.Header().Get(header.XCache))
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
	require.Equal(t, "HIT", rr.Header().Get(header.XCache))

	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("POST", "http://example.com/create", strings.NewReader("data")))

	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/redirect-target", nil))
	require.NotEqual(t, "HIT", rr2.Header().Get(header.XCache))
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
	require.Equal(t, 200, rr2.Code)
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
	require.Equal(t, 200, rr2.Code)
	require.Equal(t, "HIT", rr2.Header().Get(header.XCache))
	require.Equal(t, 0, rr2.Body.Len())
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

// TestHandler_Revalidate_5xx_NoSIEWindow verifies that an expired object
// without a stale-if-error window is NOT served stale on a 5xx response.
// RFC 5861 §4 bounds stale-on-error to the stale-if-error window.
// Regression test for #291.
func TestHandler_Revalidate_5xx_NoSIEWindow(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set(header.CacheControl, "max-age=1")
			w.Header().Set(header.ETag, `"v1"`)
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "fresh-body")
			return
		}
		w.WriteHeader(503)
	})

	h := testHandler(t, upstream)

	seed := httptest.NewRecorder()
	h.ServeHTTP(seed, httptest.NewRequest("GET", "http://example.com/no-sie", nil))
	require.Equal(t, 200, seed.Code)

	// Wait for the object to expire (max-age=1, no stale-if-error).
	time.Sleep(1500 * time.Millisecond)

	// Second request triggers revalidation; upstream returns 5xx.
	// Without a stale-if-error window, the 5xx must be forwarded.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/no-sie", nil))
	require.Equal(t, 503, rr.Code)
	require.Equal(t, "MISS", rr.Header().Get(header.XCache))
}

// TestHandler_Revalidate_5xx_MustRevalidateWithSIE verifies that an object
// with both must-revalidate and stale-if-error is NOT served stale on 5xx.
// must-revalidate requires a successful revalidation before serving stale
// (RFC 9111 §5.2.2.1), so the SIE window must not override it.
func TestHandler_Revalidate_5xx_MustRevalidateWithSIE(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set(header.CacheControl, "max-age=1, must-revalidate, stale-if-error=600")
			w.Header().Set(header.ETag, `"v1"`)
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "fresh-body")
			return
		}
		w.WriteHeader(503)
	})

	h := testHandler(t, upstream)

	seed := httptest.NewRecorder()
	h.ServeHTTP(seed, httptest.NewRequest("GET", "http://example.com/must-revalidate", nil))
	require.Equal(t, 200, seed.Code)

	time.Sleep(1500 * time.Millisecond)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/must-revalidate", nil))
	require.Equal(t, 503, rr.Code)
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

	key := BuildKey(httptest.NewRequest("GET", url, nil), nil)
	obj, _, _ := h.store.Get(context.Background(), key)
	require.NotNil(t, obj)
	stale := obj.CloneForRefresh()
	stale.StoredAt = time.Now().Add(-staleAge)
	_ = h.store.Put(context.Background(), key, stale)

	reqStart := time.Now()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", url, nil)
	req.Header.Set(header.CacheControl, "no-cache")
	h.ServeHTTP(rr, req)

	require.Equal(t, 200, rr.Code)

	ageStr := rr.Header().Get(header.Age)
	ageSecs, err := strconv.Atoi(ageStr)
	require.NoErrorf(t, err, "Age header = %q, not an integer", ageStr)

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
	require.Equal(t, 0, hitCount)

	// One more should trip the cap.
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://example.com/vary", nil)
	req.Header.Set("X-Test-Variant", "overflow")
	h.ServeHTTP(rr, req)
	require.Equal(t, 1, hitCount)
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
	require.Equal(t, 0, hitCount)
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
	require.Nil(t, pkObj)

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
	require.Equal(t, 200, rr.Code)
	require.Equal(t, 1, originCalls)
	require.Equal(t, "MISS", rr.Header().Get(header.XCache))

	// Second request should be a HIT — served from local cache.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/e", nil))
	require.Equal(t, "HIT", rr2.Header().Get(header.XCache))
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
	require.Equal(t, "MISS", rr.Header().Get(header.XCache))

	// Second request — HIT.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/ban-me", nil))
	require.Equal(t, "HIT", rr2.Header().Get(header.XCache))

	// Ban by path regex.
	count, err := store.Ban(context.Background(), api.BanExpr{
		PathRegex: "^/ban-me",
	})
	require.NoError(t, err, "ban failed")
	require.Equal(t, 1, count)

	// After ban — should be MISS (re-fetch from origin).
	rr3 := httptest.NewRecorder()
	h.ServeHTTP(rr3, httptest.NewRequest("GET", "http://example.com/ban-me", nil))
	require.Equal(t, "MISS", rr3.Header().Get(header.XCache))
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
	require.Equal(t, "MISS", rr.Header().Get(header.XCache))

	// Ban by host regex.
	count, err := store.Ban(context.Background(), api.BanExpr{
		HostRegex: "example.com",
	})
	require.NoError(t, err, "ban failed")
	require.Equal(t, 1, count)

	// After ban — should be MISS.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/foo", nil))
	require.Equal(t, "MISS", rr2.Header().Get(header.XCache))
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
	v := rr.Header().Get(header.XBouinePath)
	require.Equal(t, "", v)
	v = rr.Header().Get(header.XBouineHost)
	require.Equal(t, "", v)
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
	_, err := rec.body.Write(make([]byte, maxRecorderCap+1))
	require.NoError(t, err, "write")
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
	require.Equal(t, uint64(1), obj.Hits)
	h.doBackgroundRefresh(ctx, key, obj, 0)

	// After refresh with Hits=1 < minHits=2, the object should NOT be
	// re-scheduled. The scheduler should still be empty.
	scheduled := h.scheduler.Len()
	require.Equal(t, 0, scheduled)
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
	require.Equal(t, 1, scheduled)
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
	require.Equal(t, 1, scheduled)
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
	scheduled := h.scheduler.Len()
	require.Equal(t, 1, scheduled)

	// Clear scheduler to detect next re-schedule.
	h.scheduler.Stop()
	h.scheduler = NewRefreshScheduler(h.triggerBgRefresh, h.lookupForRefresh)
	h.scheduler.Start()

	// Refresh 2: persist=2 → decrement to 1, re-schedule.
	h.doBackgroundRefresh(context.Background(), key, obj, 0)
	scheduled = h.scheduler.Len()
	require.Equal(t, 1, scheduled)

	h.scheduler.Stop()
	h.scheduler = NewRefreshScheduler(h.triggerBgRefresh, h.lookupForRefresh)
	h.scheduler.Start()

	// Refresh 3: persist=1 → decrement to 0, re-schedule.
	h.doBackgroundRefresh(context.Background(), key, obj, 0)
	scheduled = h.scheduler.Len()
	require.Equal(t, 1, scheduled)

	h.scheduler.Stop()
	h.scheduler = NewRefreshScheduler(h.triggerBgRefresh, h.lookupForRefresh)
	h.scheduler.Start()

	// Refresh 4: persist=0 → gate blocks, no re-schedule.
	h.doBackgroundRefresh(context.Background(), key, obj, 0)
	scheduled = h.scheduler.Len()
	require.Equal(t, 0, scheduled)
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
	require.Equal(t, uint64(1), obj.Hits)

	// Unpopular refresh: windowHits=0 < minHits=2 → persist 2→1, re-scheduled.
	staleHits := h.store.WindowHits(key)
	h.doBackgroundRefresh(context.Background(), key, obj, staleHits)
	scheduled := h.scheduler.Len()
	require.Equal(t, 1, scheduled)
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
	scheduled = h.scheduler.Len()
	require.Equal(t, 1, scheduled)
	entry = h.refreshRegistry.Lookup(key)
	require.NotNil(t, entry)
	require.Equal(t, 2, entry.persistCycles)
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
	require.Equal(t, uint64(1), obj.Hits)

	// Hits=1 < minHits=2, persist=0 → gate blocks immediately.
	h.doBackgroundRefresh(context.Background(), key, obj, 0)
	scheduled := h.scheduler.Len()
	require.Equal(t, 0, scheduled)
}

func TestRefreshPersistCycles_DecrementPersistOnMissingKey(t *testing.T) {
	t.Parallel()
	r := newRefreshRegistry()
	key := testkey.Key(42)

	// Key not registered → DecrementPersist returns false.
	require.False(t, r.DecrementPersist(key))

	req := httptest.NewRequest("GET", "http://example.com/test", nil)
	r.Register(key, req, "", 2)

	// persist=2 → decrement to 1.
	require.True(t, r.DecrementPersist(key))
	// persist=1 → decrement to 0.
	require.True(t, r.DecrementPersist(key))
	// persist=0 → returns false.
	require.False(t, r.DecrementPersist(key))
}

func TestDoFetchErrAbortHandler(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	req := httptest.NewRequest("GET", "/", nil)
	res := h.doFetch(req)
	require.NotNil(t, res.Err)
	require.True(t, errors.Is(res.Err, http.ErrAbortHandler))
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
	res := h.collapsedFetch(req, api.Key{})
	require.NotNil(t, res.Err)
	require.True(t, errors.Is(res.Err, http.ErrAbortHandler))
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
	require.NotNil(t, res.Err)
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
	require.Nil(t, res.Err)
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
	require.Nil(t, res.Err)
	require.Equal(t, 200, res.StatusCode)
	require.Equal(t, "cached-body", string(res.Body))
}

func TestRefreshFrom304_HeadersUpdatedForLazySerialization(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))

	stale := &api.Object{
		Key:        BuildKeyFromURL("http://example.com/test", nil),
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=60"}, header.ETag: {`"v1"`}, "X-Sensitive": {"secret"}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now().Add(-time.Minute),
		TTL:        time.Minute,
		ETag:       `"v1"`,
	}
	stale.CacheControl = stale.Header.Get(header.CacheControl)

	// 304 adds no-cache="X-Sensitive" — serializeHead must now skip X-Sensitive.
	res := fetchResult{
		StatusCode: 304,
		Header:     http.Header{header.CacheControl: {"max-age=3600, no-cache=\"X-Sensitive\""}, header.ETag: {`"v2"`}},
	}

	refreshed := h.refreshFrom304(stale, res)

	// SerializedHead is lazy — must be nil after refresh (not eagerly computed).
	require.Nil(t, refreshed.LoadSerializedHead())

	// Verify that serializeHead(refreshed) produces the correct result.
	expected := serializeHead(refreshed)
	require.True(t, bytes.Equal(expected, serializeHead(refreshed)))
	require.True(t, bytes.Contains(expected, []byte("max-age=3600")))
	require.False(t, bytes.Contains(expected, []byte("max-age=60")))
	require.False(t, bytes.Contains(expected, []byte("X-Sensitive: secret")))
}

// blockingStore wraps a real HotStore but blocks Put after blockCh is
// signalled. This lets tests hold an SWR goroutine inside store.Put while
// Handler.Close is called, proving Close drains the goroutine before
// returning.
type blockingStore struct {
	storage.Store
	blockCh     chan struct{} // when closed, Put starts blocking
	putReached  chan struct{} // closed when a Put blocks on release
	releaseCh   chan struct{} // when closed, blocked Put proceeds
	putDone     atomic.Bool
	reachedOnce sync.Once
}

func newBlockingStore(inner storage.Store) *blockingStore {
	return &blockingStore{
		Store:      inner,
		blockCh:    make(chan struct{}),
		putReached: make(chan struct{}),
		releaseCh:  make(chan struct{}),
	}
}

func (b *blockingStore) startBlocking() {
	close(b.blockCh)
}

func (b *blockingStore) Put(ctx context.Context, key api.Key, obj *api.Object) error {
	// Only block if blocking has been enabled. Before startBlocking(),
	// Put passes through to the inner store immediately.
	select {
	case <-b.blockCh:
		b.reachedOnce.Do(func() { close(b.putReached) })
		// Intentionally ignore ctx so the goroutine stays blocked even
		// when Close cancels the background context. This simulates a
		// slow store.Put that does not honour context cancellation, which
		// is exactly the scenario where Close must wait on revalWg.
		<-b.releaseCh
	default:
		// Not yet in blocking mode — fall through.
	}
	err := b.Store.Put(ctx, key, obj)
	b.putDone.Store(true)
	return err
}

// TestHandler_SWR_Close_DrainsInFlightRevalidate verifies that Handler.Close
// waits for in-flight SWR background revalidation goroutines to complete
// before returning. This is the regression test for the use-after-close
// panic described in issue #283.
func TestHandler_SWR_Close_DrainsInFlightRevalidate(t *testing.T) {
	t.Parallel()

	innerStore := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  1 << 20,
		NumShards: 2,
	})
	bs := newBlockingStore(innerStore)

	// Origin returns a cacheable response with SWR.
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=1, stale-while-revalidate=60")
		w.Header().Set(header.ETag, `"v1"`)
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})

	h := NewHandler(HandlerConfig{
		Upstream: upstream,
		Store:    bs,
	})

	// First request: populates the cache.
	r1 := httptest.NewRequest("GET", "http://example.com/swr", nil)
	w1 := httptest.NewRecorder()
	h.ServeHTTP(w1, r1)
	require.Equal(t, 200, w1.Code)

	// Wait for TTL to expire so the next request is a StaleHit + SWR trigger.
	time.Sleep(2 * time.Second)

	// Enable blocking so the SWR goroutine's Put will block.
	bs.startBlocking()

	// Second request: StaleHit → serves stale, triggers background revalidation.
	// The blocking store will hold the SWR goroutine inside Put.
	r2 := httptest.NewRequest("GET", "http://example.com/swr", nil)
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, r2)
	require.Equal(t, 200, w2.Code)

	// Wait for the SWR goroutine to reach store.Put.
	select {
	case <-bs.putReached:
	case <-time.After(5 * time.Second):
		t.Fatal("SWR goroutine never reached store.Put")
	}

	// Close the handler while the SWR goroutine is blocked in Put.
	// Close must NOT return until the goroutine completes (or ctx times out).
	closeDone := make(chan error)
	go func() {
		closeDone <- h.Close(context.Background())
	}()

	// Give Close a moment to prove it's blocked.
	select {
	case <-closeDone:
		t.Fatal("Close returned before SWR goroutine completed — use-after-close risk")
	case <-time.After(100 * time.Millisecond):
		// Good: Close is still waiting.
	}

	// Release the SWR goroutine so it can complete.
	close(bs.releaseCh)

	// Now Close should return.
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after SWR goroutine completed")
	}

	// Verify the SWR goroutine actually finished Put (no use-after-close).
	require.True(t, bs.putDone.Load(), "SWR goroutine should have completed store.Put")

	_ = innerStore.Close(context.Background())
}

// TestHandler_SWR_Close_NoGoroutineLeakAfterShutdown verifies that after
// Close, no new SWR goroutines are spawned (the h.done check prevents it).
func TestHandler_SWR_Close_NoNewRevalidateAfterClose(t *testing.T) {
	t.Parallel()

	innerStore := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  1 << 20,
		NumShards: 2,
	})
	bs := newBlockingStore(innerStore)

	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=1, stale-while-revalidate=60")
		w.Header().Set(header.ETag, `"v1"`)
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})

	h := NewHandler(HandlerConfig{
		Upstream: upstream,
		Store:    bs,
	})

	// Populate cache.
	r1 := httptest.NewRequest("GET", "http://example.com/swr2", nil)
	w1 := httptest.NewRecorder()
	h.ServeHTTP(w1, r1)

	time.Sleep(2 * time.Second)

	// Close handler — h.done is now closed.
	require.NoError(t, h.Close(context.Background()))

	// Request after close: should not spawn a new SWR goroutine.
	r2 := httptest.NewRequest("GET", "http://example.com/swr2", nil)
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, r2)

	// The SWR goroutine should not have been spawned, so Put should not be reached.
	select {
	case <-bs.putReached:
		t.Fatal("SWR goroutine was spawned after Close — h.done check failed")
	case <-time.After(200 * time.Millisecond):
		// Good: no new goroutine.
	}

	_ = innerStore.Close(context.Background())
}
