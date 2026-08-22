package cache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
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
// without a stale-if-error window is still served stale on a 5xx response.
// The cache-tests conformance suite (stale-warning-stored, stale-close)
// expects stale serving on origin errors regardless of an explicit SIE
// window, matching the behaviour of Varnish and squid.
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
	// Without a stale-if-error window, stale is still served because
	// staleFallbackAllowed returns true (no must-revalidate etc.).
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/no-sie", nil))
	require.Equal(t, 200, rr.Code)
	require.Equal(t, "STALE", rr.Header().Get(header.XCache))
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

// TestHandler_Revalidate_ConnError_MustRevalidate verifies that a
// must-revalidate object is NOT served stale when the origin connection
// drops (res.Err path). The res.Err path uses the same staleFallbackAllowed
// gate as the 5xx path, so must-revalidate requires the error to be
// forwarded (RFC 9111 §5.2.2.1).
func TestHandler_Revalidate_ConnError_MustRevalidate(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set(header.CacheControl, "max-age=1, must-revalidate")
			w.Header().Set(header.ETag, `"v1"`)
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "fresh-body")
			return
		}
		// Simulate a connection drop: ErrAbortHandler is caught by doFetch
		// and converted to fetchResult{Err: ...}.
		panic(http.ErrAbortHandler)
	})

	h := testHandler(t, upstream)

	seed := httptest.NewRecorder()
	h.ServeHTTP(seed, httptest.NewRequest("GET", "http://example.com/must-revalidate-err", nil))
	require.Equal(t, 200, seed.Code)

	time.Sleep(1500 * time.Millisecond)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/must-revalidate-err", nil))
	require.Equal(t, 502, rr.Code)
	require.Equal(t, "MISS", rr.Header().Get(header.XCache))
}

// TestHandler_Revalidate_ConnError_NoDirective verifies that an object
// without blocking directives IS served stale when the origin connection
// drops (res.Err path), matching the 5xx path behaviour.
func TestHandler_Revalidate_ConnError_NoDirective(t *testing.T) {
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
		panic(http.ErrAbortHandler)
	})

	h := testHandler(t, upstream)

	seed := httptest.NewRecorder()
	h.ServeHTTP(seed, httptest.NewRequest("GET", "http://example.com/no-dir-err", nil))
	require.Equal(t, 200, seed.Code)

	time.Sleep(1500 * time.Millisecond)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/no-dir-err", nil))
	require.Equal(t, 200, rr.Code)
	require.Equal(t, "STALE", rr.Header().Get(header.XCache))
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

	key := BuildKey(requestInfoFromURL("GET", url), nil)
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

// TestHandler_StayinAlive_FetchAndStore_ServesStaleOn5xx exercises the
// fetchAndStoreStayinAlive path (miss → stale fallback) with a 5xx upstream
// response. Unlike the revalidate tests above, the second request does NOT
// send no-cache, so the dispatch is Miss (not Revalidate) and the request
// goes through handleCacheMiss → fetchAndStoreStayinAlive.
func TestHandler_StayinAlive_FetchAndStore_ServesStaleOn5xx(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set(header.CacheControl, "max-age=60")
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "fresh-body")
			return
		}
		w.WriteHeader(503)
	})

	h := testHandlerStayinAlive(t, upstream)
	url := "http://example.com/sa-miss-5xx"

	// Populate cache.
	rr1 := httptest.NewRecorder()
	h.ServeHTTP(rr1, httptest.NewRequest("GET", url, nil))
	require.Equal(t, 200, rr1.Code)

	// Manually expire the stored object past TTL + SWR + SIE so Evaluate
	// returns Miss (not StaleHit or Revalidate).
	key := BuildKey(requestInfoFromURL("GET", url), nil)
	obj, _, _ := h.store.Get(context.Background(), key)
	require.NotNil(t, obj)
	stale := obj.CloneForRefresh()
	stale.StoredAt = time.Now().Add(-2 * time.Minute) // past max-age=60
	stale.StaleWhileRevalidate = 0                    // no SWR window
	stale.StaleIfError = 0                            // no SIE window
	require.NoError(t, h.store.Put(context.Background(), key, stale))

	// Second request without no-cache → Miss → fetchAndStoreStayinAlive.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", url, nil))
	require.Equal(t, 200, rr2.Code, "stale served on 5xx")
	require.Contains(t, rr2.Body.String(), "fresh-body")
}

// TestHandler_StayinAlive_FetchAndStore_ServesStaleOnErr exercises the
// fetchAndStoreStayinAlive path with a connection error (upstream aborts
// via http.ErrAbortHandler). This covers the res.Err != nil branch.
func TestHandler_StayinAlive_FetchAndStore_ServesStaleOnErr(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set(header.CacheControl, "max-age=60")
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "err-body")
			return
		}
		panic(http.ErrAbortHandler)
	})

	h := testHandlerStayinAlive(t, upstream)
	url := "http://example.com/sa-miss-err"

	rr1 := httptest.NewRecorder()
	h.ServeHTTP(rr1, httptest.NewRequest("GET", url, nil))
	require.Equal(t, 200, rr1.Code)

	key := BuildKey(requestInfoFromURL("GET", url), nil)
	obj, _, _ := h.store.Get(context.Background(), key)
	require.NotNil(t, obj)
	stale := obj.CloneForRefresh()
	stale.StoredAt = time.Now().Add(-2 * time.Minute)
	stale.StaleWhileRevalidate = 0
	stale.StaleIfError = 0
	require.NoError(t, h.store.Put(context.Background(), key, stale))

	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", url, nil))
	require.Equal(t, 200, rr2.Code, "stale served on upstream error")
	require.Contains(t, rr2.Body.String(), "err-body")
}

// TestHandler_StayinAlive_Revalidate_ServesStaleOnErr exercises the
// revalidate path with a connection error. The existing 5xx revalidate
// test covers res.StatusCode >= 500; this covers res.Err != nil.
// Requires an ETag on the stored response so evalNoCache returns
// Revalidate (without a validator, no-cache dispatches as Miss).
func TestHandler_StayinAlive_Revalidate_ServesStaleOnErr(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set(header.CacheControl, "max-age=1")
			w.Header().Set(header.ETag, `"v1"`)
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "reval-err-body")
			return
		}
		panic(http.ErrAbortHandler)
	})

	h := testHandlerStayinAlive(t, upstream)
	url := "http://example.com/sa-reval-err"

	rr1 := httptest.NewRecorder()
	h.ServeHTTP(rr1, httptest.NewRequest("GET", url, nil))
	require.Equal(t, 200, rr1.Code)

	// no-cache + ETag forces the Revalidate dispatch → revalidate path.
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest("GET", url, nil)
	req2.Header.Set(header.CacheControl, "no-cache")
	h.ServeHTTP(rr2, req2)
	require.Equal(t, 200, rr2.Code, "stale served on revalidation error")
	require.Contains(t, rr2.Body.String(), "reval-err-body")
}

// TestHandler_StayinAlive_Revalidate_ServesStaleOn5xx exercises the
// revalidate path's 5xx branch with stayinAlive. The existing
// ServesStaleon5xx test lacks an ETag, so it goes through
// fetchAndStoreStayinAlive (Miss dispatch), not revalidate. This test
// adds an ETag so no-cache triggers Revalidate dispatch.
func TestHandler_StayinAlive_Revalidate_ServesStaleOn5xx(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set(header.CacheControl, "max-age=1")
			w.Header().Set(header.ETag, `"v1"`)
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "reval-5xx-body")
			return
		}
		w.WriteHeader(503)
	})

	h := testHandlerStayinAlive(t, upstream)
	url := "http://example.com/sa-reval-5xx"

	rr1 := httptest.NewRecorder()
	h.ServeHTTP(rr1, httptest.NewRequest("GET", url, nil))
	require.Equal(t, 200, rr1.Code)

	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest("GET", url, nil)
	req2.Header.Set(header.CacheControl, "no-cache")
	h.ServeHTTP(rr2, req2)
	require.Equal(t, 200, rr2.Code, "stale served on revalidation 5xx")
	require.Contains(t, rr2.Body.String(), "reval-5xx-body")
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
	storeKey := VariantKey(primaryKey, "X-Test-Variant", header.FromHTTP(req1.Header), nil)

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

	newStoreKey := VariantKey(primaryKey, "X-Test-Variant", header.FromHTTP(req2.Header), nil)

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

// TestHandler_PurgeDeletesVariants pins RFC 9111 §4.2.4: purging a primary
// key MUST invalidate every Vary variant stored under composite keys.
// Before the fix, Purge deleted only the primary key and left all variant
// entries live in the store, so subsequent requests for different variants
// were served stale hits.
func TestHandler_PurgeDeletesVariants(t *testing.T) {
	t.Parallel()

	orig := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=3600")
		w.Header().Set(header.Vary, "X-Test-Variant")
		_, _ = w.Write([]byte(r.Header.Get("X-Test-Variant")))
	})

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20})
	h := NewHandler(HandlerConfig{
		Upstream: orig,
		Store:    store,
		Logger:   slog.Default(),
	})

	// Populate two distinct variants under composite keys.
	reqA := httptest.NewRequest("GET", "http://example.com/vary", nil)
	reqA.Header.Set("X-Test-Variant", "a")
	h.ServeHTTP(httptest.NewRecorder(), reqA)

	reqB := httptest.NewRequest("GET", "http://example.com/vary", nil)
	reqB.Header.Set("X-Test-Variant", "b")
	h.ServeHTTP(httptest.NewRecorder(), reqB)

	primaryKey := h.buildKey(reqA)
	keyA := VariantKey(primaryKey, "X-Test-Variant", header.FromHTTP(reqA.Header), nil)
	keyB := VariantKey(primaryKey, "X-Test-Variant", header.FromHTTP(reqB.Header), nil)
	require.NotEqual(t, primaryKey, keyA)
	require.NotEqual(t, primaryKey, keyB)

	// Sanity: both variants and the primary are live in the store.
	objA, _, _ := store.Get(context.Background(), keyA)
	require.NotNil(t, objA)
	objB, _, _ := store.Get(context.Background(), keyB)
	require.NotNil(t, objB)
	objPK, _, _ := store.Get(context.Background(), primaryKey)
	require.NotNil(t, objPK)

	// Purge the primary key — must delete the primary AND every variant.
	owned, err := h.Purge(context.Background(), primaryKey)
	require.NoError(t, err)
	require.True(t, owned, "handler should own the key it cached")

	// All three keys must be gone from the store.
	if obj, _, _ := store.Get(context.Background(), primaryKey); obj != nil {
		t.Fatal("primary key still in store after Purge")
	}
	if obj, _, _ := store.Get(context.Background(), keyA); obj != nil {
		t.Fatal("variant A still in store after Purge")
	}
	if obj, _, _ := store.Get(context.Background(), keyB); obj != nil {
		t.Fatal("variant B still in store after Purge")
	}

	// variantSets entry for the primary key must be cleared.
	h.variantMu.Lock()
	set := h.variantSets[primaryKey]
	h.variantMu.Unlock()
	if len(set) != 0 {
		t.Fatalf("expected variantSets[primary] cleared, got %d entries", len(set))
	}

	// Subsequent requests must miss and re-fetch from origin (no stale hits).
	rrA := httptest.NewRecorder()
	h.ServeHTTP(rrA, reqA)
	require.Equal(t, "MISS", rrA.Header().Get(header.XCache))
	require.Equal(t, "a", rrA.Body.String())
}

// TestHandler_PurgeNoVariants verifies Purge works when the primary key has
// no Vary variants (the common case): it deletes the primary and clears a
// possibly-empty variantSets entry.
func TestHandler_PurgeNoVariants(t *testing.T) {
	t.Parallel()

	orig := origin200("body")
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	h := NewHandler(HandlerConfig{
		Upstream: orig,
		Store:    store,
		Logger:   slog.Default(),
	})

	req := httptest.NewRequest("GET", "http://example.com/plain", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	key := h.buildKey(req)
	obj, _, _ := store.Get(context.Background(), key)
	require.NotNil(t, obj)

	owned, err := h.Purge(context.Background(), key)
	require.NoError(t, err)
	require.True(t, owned, "handler should own the key it cached")
	if obj, _, _ := store.Get(context.Background(), key); obj != nil {
		t.Fatal("key still in store after Purge")
	}
}

// TestHandler_PurgeUnknownKey verifies Purge is a no-op (no error, no panic)
// for a primary key that was never stored.
func TestHandler_PurgeUnknownKey(t *testing.T) {
	t.Parallel()

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	h := NewHandler(HandlerConfig{
		Upstream: origin200("body"),
		Store:    store,
		Logger:   slog.Default(),
	})
	owned, err := h.Purge(context.Background(), testkey.Key(42))
	require.NoError(t, err)
	require.False(t, owned, "handler should not own an unknown key")
}

// TestHandler_PurgeVariantsUnregistersRefresh confirms that Purge also
// unregisters purged keys from the refresh registry so background
// revalidation does not resurrect purged objects.
func TestHandler_PurgeVariantsUnregistersRefresh(t *testing.T) {
	t.Parallel()

	orig := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=3600")
		w.Header().Set(header.Vary, "X-Test-Variant")
		_, _ = w.Write([]byte(r.Header.Get("X-Test-Variant")))
	})

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20})
	h := NewHandler(HandlerConfig{
		Upstream:            orig,
		Store:               store,
		Logger:              slog.Default(),
		RefreshBeforeExpiry: true,
		RefreshMinHits:      0,
	})

	reqA := httptest.NewRequest("GET", "http://example.com/vary", nil)
	reqA.Header.Set("X-Test-Variant", "a")
	h.ServeHTTP(httptest.NewRecorder(), reqA)

	primaryKey := h.buildKey(reqA)
	keyA := VariantKey(primaryKey, "X-Test-Variant", header.FromHTTP(reqA.Header), nil)

	// Both the primary and the variant should be in the refresh registry.
	require.NotNil(t, h.refreshRegistry.Lookup(primaryKey))
	require.NotNil(t, h.refreshRegistry.Lookup(keyA))

	owned, err := h.Purge(context.Background(), primaryKey)
	require.NoError(t, err)
	require.True(t, owned, "handler should own the key it cached")

	// After purge, neither key should be in the refresh registry.
	require.Nil(t, h.refreshRegistry.Lookup(primaryKey))
	require.Nil(t, h.refreshRegistry.Lookup(keyA))
}

// TestHandler_PurgeNonOwningHandlerSkipsStoreDelete verifies that after one
// handler purges a key, a second handler sharing the store does NOT call
// store.Delete (which would write a spurious warm-tier tombstone). This is
// the unit-level guarantee that purgeKey relies on: it stops after the
// first owning handler, and subsequent handlers see the key gone and
// return owned=false without any store mutation.
func TestHandler_PurgeNonOwningHandlerSkipsStoreDelete(t *testing.T) {
	t.Parallel()

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})

	// Handler A caches a key; handler B shares the store but never
	// caches anything.
	orig := origin200("body")
	hA := NewHandler(HandlerConfig{
		Upstream: orig,
		Store:    store,
		Logger:   slog.Default(),
	})
	hB := NewHandler(HandlerConfig{
		Upstream: orig,
		Store:    store,
		Logger:   slog.Default(),
	})

	req := httptest.NewRequest("GET", "http://example.com/owned-by-a", nil)
	hA.ServeHTTP(httptest.NewRecorder(), req)
	key := hA.buildKey(req)

	// Confirm the key is in the store.
	obj, _, _ := store.Get(context.Background(), key)
	require.NotNil(t, obj)

	// Purge through handler A (owning). Must return owned=true
	// and delete the key.
	owned, err := hA.Purge(context.Background(), key)
	require.NoError(t, err)
	require.True(t, owned, "handler A should own the key it cached")

	obj, _, _ = store.Get(context.Background(), key)
	require.Nil(t, obj, "key must be gone after owning Purge")

	// Purge through handler B (non-owning, key already gone). Must
	// return owned=false and must NOT error — the key is already
	// deleted, so there is nothing for handler B to do.
	owned, err = hB.Purge(context.Background(), key)
	require.NoError(t, err)
	require.False(t, owned, "handler B should not own an already-purged key")
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
	// After the test's Get, obj.Hits=0 (fast path: visited=true on
	// insert #484 skips the slow-path Hits increment). The per-window
	// WindowHits counter is 1. With minHits=2, the gate should block
	// re-scheduling (staleHits=0 passed below).
	require.GreaterOrEqual(t, h.store.WindowHits(key), int64(1))
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

	// Store (MISS) then access (HIT). With visited=true on insert
	// (#484), the fast path skips the Object.Hits increment, so
	// obj.Hits=0. The per-window WindowHits counter is incremented
	// on both fast and slow paths and is what the refresh gate uses.
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
	if h.store.WindowHits(key) < 1 {
		t.Fatalf("expected >= 1 windowHits, got %d", h.store.WindowHits(key))
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

	// Store (MISS) then access (HIT). With visited=true on insert
	// (#484), the fast path skips Object.Hits, so obj.Hits=0. The
	// per-window WindowHits counter is 1 and is what the refresh
	// gate uses.
	url := "http://example.com/hits"
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))

	key := h.buildKey(httptest.NewRequest("GET", url, nil))
	obj, _, err := h.store.Get(context.Background(), key)
	if err != nil || obj == nil {
		t.Fatalf("store.Get: obj=%v err=%v", obj, err)
	}
	if h.store.WindowHits(key) < 1 {
		t.Fatalf("expected >= 1 windowHit before refresh, got %d", h.store.WindowHits(key))
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
	// After Get, obj.Hits=0 (fast path: visited=true on insert #484).
	// With minHits=2, the gate would block. But persist=3 should keep
	// it alive for 3 more cycles. We pass staleHits=0 to each
	// doBackgroundRefresh — the 304 path resets Hits to 0 and the gate
	// checks staleHits (0 < minHits=2).

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
	// MISS → stores with visited=true (#484). Registered with persist=2.
	// Object.Hits stays 0 (fast path); WindowHits=1 after the test's Get.
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
	// obj.Hits=0 (fast path: visited=true on insert #484). WindowHits=1.
	require.GreaterOrEqual(t, h.store.WindowHits(key), int64(1))

	// Unpopular refresh: windowHits=1 < minHits=2 → persist 2→1, re-scheduled.
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
	// obj.Hits=0 (fast path: visited=true on insert #484). WindowHits=1.
	// With minHits=2, the gate blocks (staleHits=0 passed below).
	require.GreaterOrEqual(t, h.store.WindowHits(key), int64(1))

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
	r.Register(key, requestInfoFromHTTP(req.Method, req.URL.String(), req.Host, req.URL.Path, req.TLS != nil, header.FromHTTP(req.Header)), "", 2)

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
		Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=60"}, header.ETag: []string{`"v1"`}, "X-Sensitive": {"secret"}}),
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
		Header:     fromHeaderMap(header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=3600, no-cache=\"X-Sensitive\""}, header.ETag: []string{`"v2"`}})),
	}

	refreshed := h.refreshFrom304(stale, res, time.Now())

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

func TestErrorType(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "timeout", errorType(errors.New("context deadline exceeded")))
	assert.Equal(t, "connection", errorType(errors.New("connection refused")))
	assert.Equal(t, "connection", errorType(errors.New("dial tcp: EOF")))
	assert.Equal(t, "other", errorType(errors.New("something else")))
	assert.Equal(t, "unknown", errorType(nil))
}

func TestParseSurrogateKeys(t *testing.T) {
	t.Parallel()
	t.Run("surrogate_key", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set("Surrogate-Key", "key1 key2")
		keys := parseSurrogateKeys(header.FromHTTP(h))
		assert.Equal(t, []string{"key1", "key2"}, keys)
	})
	t.Run("cache_tag", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set("Cache-Tag", "tag1,tag2")
		keys := parseSurrogateKeys(header.FromHTTP(h))
		assert.Equal(t, []string{"tag1", "tag2"}, keys)
	})
	t.Run("x_cache_tags", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set("X-Cache-Tags", "t1 t2, t3")
		keys := parseSurrogateKeys(header.FromHTTP(h))
		assert.Equal(t, []string{"t1", "t2", "t3"}, keys)
	})
	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		assert.Nil(t, parseSurrogateKeys(header.FromHTTP(h)))
	})
	t.Run("dedup", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set("Surrogate-Key", "key1 key1 key2")
		keys := parseSurrogateKeys(header.FromHTTP(h))
		assert.Equal(t, []string{"key1", "key2"}, keys)
	})
}

func TestStripNoCacheFields(t *testing.T) {
	t.Parallel()
	dst := http.Header{
		header.CacheControl: []string{"max-age=60"},
		"Set-Cookie":        {"sid=abc"},
		"Content-Encoding":  {"gzip"},
		"ETag":              {`"v1"`},
	}
	stripNoCacheFields(dst, `no-cache="Set-Cookie, Content-Encoding"`)
	assert.NotContains(t, dst, "Set-Cookie")
	assert.NotContains(t, dst, "Content-Encoding")
	assert.Contains(t, dst, "ETag")
}

func TestStripNoCacheFields_Empty(t *testing.T) {
	t.Parallel()
	dst := http.Header{"ETag": {`"v1"`}}
	stripNoCacheFields(dst, "")
	stripNoCacheFields(dst, "max-age=60")
	assert.Contains(t, dst, "ETag")
}

func TestIsInvalidating(t *testing.T) {
	t.Parallel()
	assert.True(t, isInvalidating("POST"))
	assert.True(t, isInvalidating(http.MethodPut))
	assert.True(t, isInvalidating(http.MethodDelete))
	assert.True(t, isInvalidating(http.MethodPatch))
	assert.False(t, isInvalidating("GET"))
	assert.False(t, isInvalidating(http.MethodHead))
}

func TestStaleFallbackAllowed(t *testing.T) {
	t.Parallel()
	t.Run("no_restrictions", func(t *testing.T) {
		t.Parallel()
		obj := &api.Object{CacheControl: "max-age=60"}
		assert.True(t, staleFallbackAllowed(obj))
	})
	t.Run("must_revalidate_blocks", func(t *testing.T) {
		t.Parallel()
		obj := &api.Object{CacheControl: "max-age=60, must-revalidate"}
		assert.False(t, staleFallbackAllowed(obj))
	})
	t.Run("no_cache_blocks", func(t *testing.T) {
		t.Parallel()
		obj := &api.Object{CacheControl: "no-cache"}
		assert.False(t, staleFallbackAllowed(obj))
	})
}

func TestSourceSlice(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []string{"hot"}, sourceSlice(api.SourceHot))
	assert.Equal(t, []string{"warm"}, sourceSlice(api.SourceWarm))
	assert.Equal(t, []string{"peer"}, sourceSlice(api.SourcePeer))
	assert.Equal(t, []string{"origin"}, sourceSlice(api.SourceOrigin))
}

func TestComputeTTL_NegativeTTL(t *testing.T) {
	t.Parallel()
	h := http.Header{}
	ttl := computeTTL(header.FromHTTP(h), 404, Directives{}, 30*time.Second, 0, 0, 0, time.Now())
	assert.Equal(t, 30*time.Second, ttl)
}

func TestComputeTTL_HeuristicTTL(t *testing.T) {
	t.Parallel()
	h := http.Header{
		header.Date:         []string{"Mon, 01 Jan 2024 00:00:00 GMT"},
		header.LastModified: []string{"Mon, 01 Jan 2023 00:00:00 GMT"},
	}
	ttl := computeTTL(header.FromHTTP(h), 200, Directives{}, 0, 0, 0, 0, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, 876*time.Hour, ttl)
}

func TestComputeTTL_DefaultTTL(t *testing.T) {
	t.Parallel()
	h := http.Header{}
	ttl := computeTTL(header.FromHTTP(h), 200, Directives{}, 0, 60*time.Second, 0, 0, time.Now())
	assert.Equal(t, 60*time.Second, ttl)
}

func TestHandler_OnlyIfCachedBypass(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	r := httptest.NewRequest("GET", "http://example.com/nonexistent", nil)
	r.Header.Set(header.CacheControl, "only-if-cached")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	require.Equal(t, http.StatusGatewayTimeout, rr.Code)
	assert.Equal(t, "MISS", rr.Header().Get(header.XCache))
}

func TestHandler_RefreshStats(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	// No refresh configured → zeros.
	scheduled, registry := h.RefreshStats()
	assert.Equal(t, 0, scheduled)
	assert.Equal(t, 0, registry)
}

func TestHandler_RouteName(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	assert.Equal(t, "", h.RouteName())
}

func TestHandler_RefreshEnabled(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	assert.False(t, h.RefreshEnabled())
}

func TestBuildObject_CDNCacheControl(t *testing.T) {
	t.Parallel()
	resHeader := http.Header{}
	resHeader.Set("CDN-Cache-Control", "max-age=120")
	resHeader.Set(header.ContentType, "text/html")
	res := fetchResult{
		StatusCode: 200,
		Header:     fromHeaderMap(header.FromHTTP(resHeader)),
		Body:       []byte("hello"),
	}
	r := httptest.NewRequest("GET", "http://example.com/", nil)
	obj := buildObject(api.Key{}, r, res, 0, 0, 0, 0, 0, 0, nil, time.Now())
	require.NotNil(t, obj)
	assert.Equal(t, 120*time.Second, obj.TTL)
	assert.Contains(t, obj.CacheControl, "max-age=120")
}

func TestBuildObject_OverrideTTL(t *testing.T) {
	t.Parallel()
	res := fetchResult{
		StatusCode: 200,
		Header: fromHeaderMap(header.FromHTTP(http.Header{
			header.CacheControl: []string{"max-age=60"},
		})),
		Body: []byte("hello"),
	}
	r := httptest.NewRequest("GET", "http://example.com/", nil)
	obj := buildObject(api.Key{}, r, res, 0, 0, 300*time.Second, 0, 0, 0, nil, time.Now())
	require.NotNil(t, obj)
	assert.Equal(t, 300*time.Second, obj.TTL)
}

func TestBuildObject_ContentLengthSynthesis(t *testing.T) {
	t.Parallel()
	res := fetchResult{
		StatusCode: 200,
		Header:     fromHeaderMap(header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=60"}})),
		Body:       []byte("hello world"),
	}
	r := httptest.NewRequest("GET", "http://example.com/", nil)
	obj := buildObject(api.Key{}, r, res, 0, 0, 0, 0, 0, 0, nil, time.Now())
	require.NotNil(t, obj)
	assert.Equal(t, "11", obj.Header.Get(header.ContentLength))
}

func TestBuildObject_DateApparentAge(t *testing.T) {
	t.Parallel()
	now := time.Now()
	res := fetchResult{
		StatusCode: 200,
		Header: fromHeaderMap(header.FromHTTP(http.Header{
			header.CacheControl: []string{"max-age=60"},
			header.Date:         []string{now.Add(-10 * time.Second).Format(httpTimeFormat)},
			header.Age:          []string{"5"},
		})),
		Body: []byte("hello"),
	}
	r := httptest.NewRequest("GET", "http://example.com/", nil)
	obj := buildObject(api.Key{}, r, res, 0, 0, 0, 0, 0, 0, nil, now)
	require.NotNil(t, obj)
	// OriginAge should be max(5s from Age header, ~10s apparent age from Date).
	assert.GreaterOrEqual(t, obj.OriginAge, 5*time.Second)
}

func TestBuildObject_LastModifiedParsed(t *testing.T) {
	t.Parallel()
	res := fetchResult{
		StatusCode: 200,
		Header: fromHeaderMap(header.FromHTTP(http.Header{
			header.CacheControl: []string{"max-age=60"},
			header.LastModified: []string{"Mon, 01 Jan 2024 00:00:00 GMT"},
		})),
		Body: []byte("hello"),
	}
	r := httptest.NewRequest("GET", "http://example.com/", nil)
	obj := buildObject(api.Key{}, r, res, 0, 0, 0, 0, 0, 0, nil, time.Now())
	require.NotNil(t, obj)
	assert.False(t, obj.LastModified.IsZero())
}

func TestBuildObject_SWRDefault(t *testing.T) {
	t.Parallel()
	res := fetchResult{
		StatusCode: 200,
		Header:     fromHeaderMap(header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=60"}})),
		Body:       []byte("hello"),
	}
	r := httptest.NewRequest("GET", "http://example.com/", nil)
	obj := buildObject(api.Key{}, r, res, 0, 0, 0, 30*time.Second, 0, 0, nil, time.Now())
	require.NotNil(t, obj)
	assert.Equal(t, 30*time.Second, obj.StaleWhileRevalidate)
}

func TestBuildObject_SIEDefault(t *testing.T) {
	t.Parallel()
	res := fetchResult{
		StatusCode: 200,
		Header:     fromHeaderMap(header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=60"}})),
		Body:       []byte("hello"),
	}
	r := httptest.NewRequest("GET", "http://example.com/", nil)
	obj := buildObject(api.Key{}, r, res, 0, 0, 0, 0, 60*time.Second, 0, nil, time.Now())
	require.NotNil(t, obj)
	assert.Equal(t, 60*time.Second, obj.StaleIfError)
}

func TestBuildObject_VaryKeyComputed(t *testing.T) {
	t.Parallel()
	res := fetchResult{
		StatusCode: 200,
		Header: fromHeaderMap(header.FromHTTP(http.Header{
			header.CacheControl: []string{"max-age=60"},
			header.Vary:         []string{"Accept-Encoding"},
		})),
		Body: []byte("hello"),
	}
	r := httptest.NewRequest("GET", "http://example.com/", nil)
	r.Header.Set(header.AcceptEncoding, "gzip")
	obj := buildObject(api.Key{}, r, res, 0, 0, 0, 0, 0, 0, nil, time.Now())
	require.NotNil(t, obj)
	// VaryKey should be non-empty (the object has a Vary header).
	assert.NotEqual(t, "", obj.VaryKey)
}

func TestDoBackgroundRefresh_EntryNil(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	ctx := context.Background()
	// Call with a key that's not registered — should skip.
	h.doBackgroundRefresh(ctx, testkey.Key(999), &api.Object{
		StoredAt: time.Now(),
		TTL:      60 * time.Second,
	}, 0)
	// No panic, no store change.
}

func TestDoBackgroundRefresh_BadURL(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	key := testkey.Key(1)
	// Register with a URL containing a control character that url.Parse rejects.
	h.refreshRegistry.Register(key, requestInfoFromHTTP("GET", "", "", "", false, header.Map{}), "", 0)
	// This should unregister and skip without panicking.
	h.doBackgroundRefresh(context.Background(), key, &api.Object{
		StoredAt: time.Now(),
		TTL:      60 * time.Second,
	}, 0)
	assert.Equal(t, 0, h.refreshRegistry.Len())
}

func TestDoBackgroundRefresh_ResErr_Backoff(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	key := testkey.Key(1)
	// Register the key with a valid URL.
	req := &http.Request{
		Method: "GET",
		URL:    mustURL("http://example.com/test"),
		Header: http.Header{},
	}
	h.refreshRegistry.Register(key, requestInfoFromHTTP(req.Method, req.URL.String(), req.Host, req.URL.Path, req.TLS != nil, header.FromHTTP(req.Header)), "", 0)
	// Use an upstream that returns 502 (error response).
	h.upstream = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(502)
	})
	stale := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=60"}, header.ETag: []string{`"v1"`}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		ETag:       `"v1"`,
	}
	h.refreshTimeout = 5 * time.Second
	h.doBackgroundRefresh(context.Background(), key, stale, 0)
	// The entry should still be registered (re-scheduled with backoff).
	assert.Equal(t, 1, h.refreshRegistry.Len())
}

func TestDoBackgroundRefresh_ContextCancelled(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	key := testkey.Key(2)
	req := &http.Request{
		Method: "GET",
		URL:    mustURL("http://example.com/test"),
		Header: http.Header{},
	}
	h.refreshRegistry.Register(key, requestInfoFromHTTP(req.Method, req.URL.String(), req.Host, req.URL.Path, req.TLS != nil, header.FromHTTP(req.Header)), "", 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	stale := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=60"}, header.ETag: []string{`"v1"`}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		ETag:       `"v1"`,
	}
	h.refreshTimeout = 5 * time.Second
	// Should return without panicking.
	h.doBackgroundRefresh(ctx, key, stale, 0)
}

func TestDoBackgroundRefresh_UncacheableSkip(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	key := testkey.Key(3)
	req := &http.Request{
		Method: "GET",
		URL:    mustURL("http://example.com/test"),
		Header: http.Header{},
	}
	h.refreshRegistry.Register(key, requestInfoFromHTTP(req.Method, req.URL.String(), req.Host, req.URL.Path, req.TLS != nil, header.FromHTTP(req.Header)), "", 0)
	// Upstream returns no-store (uncacheable).
	h.upstream = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "no-store")
		w.WriteHeader(200)
	})
	stale := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=60"}, header.ETag: []string{`"v1"`}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		ETag:       `"v1"`,
	}
	h.refreshTimeout = 5 * time.Second
	h.doBackgroundRefresh(context.Background(), key, stale, 0)
}

func TestDoBackgroundRefresh_SetCookieSkip(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	h.allowSetCookie = false
	key := testkey.Key(4)
	req := &http.Request{
		Method: "GET",
		URL:    mustURL("http://example.com/test"),
		Header: http.Header{},
	}
	h.refreshRegistry.Register(key, requestInfoFromHTTP(req.Method, req.URL.String(), req.Host, req.URL.Path, req.TLS != nil, header.FromHTTP(req.Header)), "", 0)
	h.upstream = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.SetCookie, "sid=abc")
		w.WriteHeader(200)
	})
	stale := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=60"}, header.ETag: []string{`"v1"`}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		ETag:       `"v1"`,
	}
	h.refreshTimeout = 5 * time.Second
	h.doBackgroundRefresh(context.Background(), key, stale, 0)
}

func TestDoBackgroundRefresh_MaxObjectSizeSkip(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	h.maxObjectSize = 5
	key := testkey.Key(5)
	req := &http.Request{
		Method: "GET",
		URL:    mustURL("http://example.com/test"),
		Header: http.Header{},
	}
	h.refreshRegistry.Register(key, requestInfoFromHTTP(req.Method, req.URL.String(), req.Host, req.URL.Path, req.TLS != nil, header.FromHTTP(req.Header)), "", 0)
	h.upstream = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "this is a very long body that exceeds the max object size")
	})
	stale := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=60"}, header.ETag: []string{`"v1"`}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		ETag:       `"v1"`,
	}
	h.refreshTimeout = 5 * time.Second
	h.doBackgroundRefresh(context.Background(), key, stale, 0)
}

func TestWriteAndMaybeStore_VariantStore(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(header.CacheControl, "max-age=60")
			w.Header().Set(header.Vary, "Accept-Encoding")
			w.Header().Set(header.ETag, `"v1"`)
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "body-"+r.Header.Get(header.AcceptEncoding))
		}),
		Store: store,
	})
	// First request — MISS, stores with Vary.
	r1 := httptest.NewRequest("GET", "http://example.com/v", nil)
	r1.Header.Set(header.AcceptEncoding, "gzip")
	rr1 := httptest.NewRecorder()
	h.ServeHTTP(rr1, r1)
	require.Equal(t, "MISS", rr1.Header().Get(header.XCache))
	// Second request with different encoding — MISS, stores variant.
	r2 := httptest.NewRequest("GET", "http://example.com/v", nil)
	r2.Header.Set(header.AcceptEncoding, "br")
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, r2)
	require.Equal(t, "MISS", rr2.Header().Get(header.XCache))
	// Third request with gzip — HIT.
	r3 := httptest.NewRequest("GET", "http://example.com/v", nil)
	r3.Header.Set(header.AcceptEncoding, "gzip")
	rr3 := httptest.NewRecorder()
	h.ServeHTTP(rr3, r3)
	require.Equal(t, "HIT", rr3.Header().Get(header.XCache))
}

func TestTryConditional304_Match(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.ETag: []string{`"v1"`}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		ETag:       `"v1"`,
	}
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.IfNoneMatch, `"v1"`)
	rr := httptest.NewRecorder()
	ok := h.tryConditional304(rr, r, obj, api.SourceHot)
	require.True(t, ok)
	assert.Equal(t, http.StatusNotModified, rr.Code)
	assert.Equal(t, `"v1"`, rr.Header().Get(header.ETag))
	assert.Equal(t, "HIT", rr.Header().Get(header.XCache))
}

func TestTryConditional304_NoMatch(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.ETag: []string{`"v1"`}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		ETag:       `"v1"`,
	}
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.IfNoneMatch, `"v2"`)
	rr := httptest.NewRecorder()
	ok := h.tryConditional304(rr, r, obj, api.SourceHot)
	require.False(t, ok)
}

func TestAgeHeader_Fallback(t *testing.T) {
	t.Parallel()
	// Age >= 600s uses the fallback allocation path.
	got := ageHeader(600 * time.Second)
	assert.Equal(t, "600", got[0])
	got = ageHeader(-1 * time.Second)
	assert.Equal(t, "-1", got[0])
}

func TestShouldRefresh_ScoreGate(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	h.refreshMinScore = 1000
	// staleHits * BodySize < 1000 → false.
	obj := &api.Object{BodySize: 10}
	assert.False(t, h.shouldRefresh(5, obj))
	// staleHits * BodySize >= 1000 → true.
	obj = &api.Object{BodySize: 200}
	assert.True(t, h.shouldRefresh(5, obj))
}

func TestDoFetch_Truncated(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		// Write more than maxResponseBytes.
		_, _ = w.Write(make([]byte, 2*(1<<20))) // 2 MiB
	}))
	h.maxResponseBytes = 1 << 20 // 1 MiB
	req := httptest.NewRequest("GET", "/", nil)
	res := h.doFetch(req)
	require.NotNil(t, res.Err)
	assert.Contains(t, res.Err.Error(), "exceeds")
}

func TestHandleBypass_OnlyIfCached_504(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	r := httptest.NewRequest("GET", "http://example.com/nonexistent", nil)
	r.Header.Set(header.CacheControl, "only-if-cached")
	rr := httptest.NewRecorder()
	h.handleBypass(rr, r)
	require.Equal(t, http.StatusGatewayTimeout, rr.Code)
}

func TestBuildLocationKey_AbsoluteURL(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	r := httptest.NewRequest("POST", "http://example.com/submit", nil)
	key := h.buildLocationKey(r, "http://example.com/other")
	require.False(t, key.IsZero())
}

func TestBuildLocationKey_RelativePath(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	r := httptest.NewRequest("POST", "http://example.com/api/submit", nil)
	key := h.buildLocationKey(r, "../v2")
	require.False(t, key.IsZero())
}

func TestBuildLocationKey_InvalidURL(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	r := httptest.NewRequest("POST", "http://example.com/submit", nil)
	key := h.buildLocationKey(r, "ht\ttp://invalid")
	require.True(t, key.IsZero())
}

func TestLookupForRefresh_StaleObject(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	// Store a stale object.
	key := BuildKey(requestInfoFromHTTP("GET", "http://example.com/stale", "example.com", "/stale", false, header.Map{}), nil)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=1"}}),
		Body:       []byte("stale"),
		BodySize:   5,
		StoredAt:   time.Now().Add(-10 * time.Second),
		TTL:        time.Second,
	}
	_ = h.store.Put(context.Background(), key, obj)
	// lookupForRefresh should return nil for stale.
	result := h.lookupForRefresh(key)
	require.Nil(t, result)
}

func TestLookupForRefresh_FreshObject(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	key := BuildKey(requestInfoFromHTTP("GET", "http://example.com/fresh", "", "", false, header.Map{}), nil)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=60"}}),
		Body:       []byte("fresh"),
		BodySize:   5,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	_ = h.store.Put(context.Background(), key, obj)
	result := h.lookupForRefresh(key)
	require.NotNil(t, result)
}

func TestLookupForRefresh_NotFound(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	key := BuildKey(requestInfoFromHTTP("GET", "http://example.com/missing", "", "", false, header.Map{}), nil)
	result := h.lookupForRefresh(key)
	require.Nil(t, result)
}

func TestStoreObject_RefreshScheduling(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	key := testkey.Key(10)
	r := httptest.NewRequest("GET", "http://example.com/test", nil)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=60"}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	h.storeObject(context.Background(), key, obj, r, false, 0)
	// Should be registered in the refresh registry.
	assert.Equal(t, 1, h.refreshRegistry.Len())
	// Should be scheduled in the scheduler.
	assert.Equal(t, 1, h.scheduler.Len())
}

func TestStoreObject_NegativeCacheableSkipRefresh(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	key := testkey.Key(11)
	r := httptest.NewRequest("GET", "http://example.com/404", nil)
	obj := &api.Object{
		Key:        key,
		StatusCode: 404,
		Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=30"}}),
		Body:       []byte("not found"),
		BodySize:   9,
		StoredAt:   time.Now(),
		TTL:        30 * time.Second,
	}
	h.storeObject(context.Background(), key, obj, r, false, 0)
	// Negative cacheable objects should NOT be scheduled for refresh.
	assert.Equal(t, 0, h.refreshRegistry.Len())
}

func TestInvalidateAndProxy_5xxNoInvalidation(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			w.WriteHeader(500)
			return
		}
		calls.Add(1)
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	}))
	// Populate cache with GET.
	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("GET", "http://example.com/res", nil))
	// POST with 5xx should NOT invalidate.
	r := httptest.NewRequest("POST", "http://example.com/res", strings.NewReader("data"))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	assert.Equal(t, 500, rr.Code)
	// GET should still be HIT (not invalidated).
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/res", nil))
	assert.Equal(t, "HIT", rr2.Header().Get(header.XCache))
}

func TestMaybeStorePostResponse_NonPOST(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	r := httptest.NewRequest("GET", "http://example.com/res", nil)
	getReq := httptest.NewRequest("GET", "http://example.com/res", nil)
	key := h.buildKey(getReq)
	rec := acquireRecorder(h.maxResponseBytes)
	rec.statusCode = 200
	rec.header.Set(header.CacheControl, "max-age=60")
	_, _ = rec.Write([]byte("body"))
	releaseRecorder(rec)
	// Non-POST should be a no-op.
	h.maybeStorePostResponse(r, getReq, key, rec)
}

func TestHandleCacheMiss_PeerFetch(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	peerObj := &api.Object{
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=60"}}),
		Body:       []byte("from-peer"),
		BodySize:   9,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	var originCalls atomic.Int32
	h := NewHandler(HandlerConfig{
		Upstream: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			originCalls.Add(1)
			w.Header().Set(header.CacheControl, "max-age=60")
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "from-origin")
		}),
		Store: store,
		OwnerFn: func(key api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "peer1:8080"}, false // not local
		},
		PeerFetch: func(_ context.Context, _ api.PeerInfo, _ api.Key) (*api.Object, error) {
			return peerObj, nil
		},
	})
	r := httptest.NewRequest("GET", "http://example.com/peer", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	require.Equal(t, 200, rr.Code)
	assert.Equal(t, "from-peer", rr.Body.String())
	assert.Equal(t, int32(0), originCalls.Load())
}

// TestHandleCacheMiss_NonOwnerDoesNotStoreOriginFetch pins the strong-mode
// owner-gated storage invariant: a non-owner that misses locally, misses
// peer-fetch, then fetches from origin must serve the response to the
// client but NOT store it locally. The object is forwarded to the owner
// via the write-to-owner RPC instead.
func TestHandleCacheMiss_NonOwnerDoesNotStoreOriginFetch(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	var originCalls atomic.Int32
	var peerPutCalls atomic.Int32
	var peerPutObj *api.Object
	h := NewHandler(HandlerConfig{
		Upstream: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			originCalls.Add(1)
			w.Header().Set(header.CacheControl, "max-age=60")
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "from-origin")
		}),
		Store: store,
		OwnerFn: func(key api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "owner:8080"}, false // not local
		},
		PeerFetch: func(_ context.Context, _ api.PeerInfo, _ api.Key) (*api.Object, error) {
			return nil, nil // peer miss
		},
		PeerPut: func(_ context.Context, _ api.PeerInfo, obj *api.Object) {
			peerPutCalls.Add(1)
			peerPutObj = obj
		},
	})

	r := httptest.NewRequest("GET", "http://example.com/non-owner-origin", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	require.Equal(t, 200, rr.Code)
	assert.Equal(t, "from-origin", rr.Body.String())
	assert.Equal(t, int32(1), originCalls.Load())

	// The non-owner must NOT have cached the object locally: a second
	// request must MISS again (peer-fetch miss → origin fetch).
	r2 := httptest.NewRequest("GET", "http://example.com/non-owner-origin", nil)
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, r2)
	assert.Equal(t, "MISS", rr2.Header().Get(header.XCache))
	assert.Equal(t, int32(2), originCalls.Load(), "non-owner must re-fetch from origin (no local cache)")

	// Each origin-fetch must forward the object to the owner.
	assert.Equal(t, int32(2), peerPutCalls.Load(), "each origin fetch must fire a write-to-owner RPC")
	require.NotNil(t, peerPutObj)
	assert.Equal(t, "from-origin", string(peerPutObj.Body))
}

// TestHandleCacheMiss_NonOwnerDoesNotStorePeerFetch pins that a non-owner
// that peer-fetches an object from the owner serves it to the client but
// does NOT store it locally. Without this, the fleet cache is 3× redundant.
func TestHandleCacheMiss_NonOwnerDoesNotStorePeerFetch(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	var peerPutCalls atomic.Int32
	peerObj := &api.Object{
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=60"}}),
		Body:       []byte("from-peer"),
		BodySize:   9,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	h := NewHandler(HandlerConfig{
		Upstream: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			t.Fatal("origin should not be called when peer fetch hits")
		}),
		Store: store,
		OwnerFn: func(key api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "owner:8080"}, false // not local
		},
		PeerFetch: func(_ context.Context, _ api.PeerInfo, _ api.Key) (*api.Object, error) {
			return peerObj, nil
		},
		PeerPut: func(_ context.Context, _ api.PeerInfo, _ *api.Object) {
			peerPutCalls.Add(1)
		},
	})

	r := httptest.NewRequest("GET", "http://example.com/non-owner-peer", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	require.Equal(t, 200, rr.Code)
	assert.Equal(t, "from-peer", rr.Body.String())

	// The non-owner must NOT have cached the object locally: a second
	// request must peer-fetch again, not HIT locally.
	r2 := httptest.NewRequest("GET", "http://example.com/non-owner-peer", nil)
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, r2)
	assert.Equal(t, "HIT", rr2.Header().Get(header.XCache), "peer-fetched object should be served as HIT")
	assert.Equal(t, "peer", rr2.Header().Get(header.XCacheSource), "second request should also come from peer")

	// Peer-fetch promotion must not trigger a write-to-owner RPC: the
	// object came FROM the owner, forwarding it back would be redundant.
	assert.Equal(t, int32(0), peerPutCalls.Load())
}

// TestHandleCacheMiss_OwnerStoresLocally pins that the owner node stores
// origin-fetched objects locally (the normal path). This guards against
// over-gating that would break the owner's own caching.
func TestHandleCacheMiss_OwnerStoresLocally(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	var peerPutCalls atomic.Int32
	h := NewHandler(HandlerConfig{
		Upstream: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set(header.CacheControl, "max-age=60")
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "owner-body")
		}),
		Store: store,
		OwnerFn: func(key api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "self:8080"}, true // local owner
		},
		PeerFetch: func(_ context.Context, _ api.PeerInfo, _ api.Key) (*api.Object, error) {
			t.Fatal("owner should not peer-fetch its own keys")
			return nil, nil
		},
		PeerPut: func(_ context.Context, _ api.PeerInfo, _ *api.Object) {
			peerPutCalls.Add(1)
		},
	})

	r := httptest.NewRequest("GET", "http://example.com/owner-key", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	require.Equal(t, 200, rr.Code)
	assert.Equal(t, "MISS", rr.Header().Get(header.XCache))

	// The owner must have cached locally: second request is a HIT.
	r2 := httptest.NewRequest("GET", "http://example.com/owner-key", nil)
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, r2)
	assert.Equal(t, "HIT", rr2.Header().Get(header.XCache))

	// The owner must not forward to itself via peerPut.
	assert.Equal(t, int32(0), peerPutCalls.Load())
}

func TestLookup_VaryVariantMiss(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.Vary, "Accept-Encoding")
		w.Header().Set(header.ETag, `"v1"`)
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body-"+r.Header.Get(header.AcceptEncoding))
	}))
	// Populate with gzip variant.
	r1 := httptest.NewRequest("GET", "http://example.com/vary-miss", nil)
	r1.Header.Set(header.AcceptEncoding, "gzip")
	h.ServeHTTP(httptest.NewRecorder(), r1)
	// Request with br — should miss (variant not found), then store.
	r2 := httptest.NewRequest("GET", "http://example.com/vary-miss", nil)
	r2.Header.Set(header.AcceptEncoding, "br")
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, r2)
	assert.Equal(t, "MISS", rr2.Header().Get(header.XCache))
}

func TestAppendCanonicalQueryString_Policy(t *testing.T) {
	t.Parallel()
	var buf [256]byte
	policy := NewKeyPolicy(nil, map[string]bool{"q": true}, nil, nil, false, false)
	// "q=test&utm=x" → should strip utm, keep q.
	n := appendCanonicalQueryString(buf[:], 0, "q=test&utm=x", policy)
	result := string(buf[:n])
	assert.Contains(t, result, "q=test")
	assert.NotContains(t, result, "utm")
}

func TestAppendCanonicalQueryString_PercentEncoded(t *testing.T) {
	t.Parallel()
	var buf [256]byte
	n := appendCanonicalQueryStringNoPolicy(buf[:], 0, "a=%31&b=2")
	// Should produce sorted: a=1&b=2 (after percent-decode).
	result := string(buf[:n])
	assert.Contains(t, result, "a=")
	assert.Contains(t, result, "b=2")
}

func TestAppendCanonicalQueryString_MoreThan8Params(t *testing.T) {
	t.Parallel()
	var buf [512]byte
	raw := "a=1&b=2&c=3&d=4&e=5&f=6&g=7&h=8&i=9"
	n := appendCanonicalQueryStringNoPolicy(buf[:], 0, raw)
	require.Greater(t, n, 0)
}

func TestAppendCanonicalQuerySlowString_Policy(t *testing.T) {
	t.Parallel()
	var buf [512]byte
	policy := NewKeyPolicy(nil, map[string]bool{"q": true}, nil, nil, false, false)
	n := appendCanonicalQuerySlowString(buf[:], 0, "q=test&utm=x&fbclid=123", policy)
	result := string(buf[:n])
	assert.Contains(t, result, "q=test")
	assert.NotContains(t, result, "utm")
	assert.NotContains(t, result, "fbclid")
}

func mustURL(raw string) *url.URL {
	u, _ := url.Parse(raw)
	return u
}

func TestTriggerBgRefresh_304Refresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		defer store.Close(context.Background())
		h := NewHandler(HandlerConfig{
			Upstream: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get(header.IfNoneMatch) != "" {
					w.Header().Set(header.CacheControl, "max-age=120")
					w.WriteHeader(304)
					return
				}
				calls.Add(1)
				w.Header().Set(header.CacheControl, "max-age=60")
				w.Header().Set(header.ETag, `"v1"`)
				w.WriteHeader(200)
				_, _ = w.Write([]byte("body"))
			}),
			Store:               store,
			DefaultTTL:          60 * time.Second,
			RefreshBeforeExpiry: true,
			RefreshMargin:       6 * time.Second,
			RefreshTimeout:      5 * time.Second,
			RefreshConcurrency:  4,
			RefreshMinHits:      1,
			RouteName:           "test",
		})
		defer h.Close(context.Background())
		key := BuildKey(requestInfoFromHTTP("GET", "http://example.com/refresh304", "", "", false, header.Map{}), nil)
		obj := &api.Object{
			Key:        key,
			StatusCode: 200,
			Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=60"}, header.ETag: []string{`"v1"`}}),
			Body:       []byte("body"),
			BodySize:   4,
			StoredAt:   time.Now(),
			TTL:        60 * time.Second,
			ETag:       `"v1"`,
		}
		_ = h.store.Put(context.Background(), key, obj)
		h.refreshRegistry.Register(key, requestInfoFromHTTP("GET", "", "", "", false, header.Map{}), "", 0)
		h.scheduler.Schedule(key, time.Now().Add(50*time.Millisecond))
		synctest.Sleep(200 * time.Millisecond)
		updated, _, _ := h.store.Get(context.Background(), key)
		require.NotNil(t, updated)
		assert.Equal(t, 120*time.Second, updated.TTL)
	})
}

func TestTriggerBgRefresh_RateLimited(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		defer store.Close(context.Background())
		h := NewHandler(HandlerConfig{
			Upstream: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get(header.IfNoneMatch) == `"v1"` {
					w.WriteHeader(http.StatusNotModified)
					return
				}
				w.Header().Set(header.CacheControl, "max-age=60")
				w.Header().Set(header.ETag, `"v1"`)
				w.WriteHeader(200)
				_, _ = io.WriteString(w, "body")
			}),
			Store:               store,
			DefaultTTL:          60 * time.Second,
			RefreshBeforeExpiry: true,
			RefreshMargin:       6 * time.Second,
			RefreshTimeout:      5 * time.Second,
			RefreshConcurrency:  4,
			RefreshMinHits:      1,
			RouteName:           "test",
		})
		defer h.Close(context.Background())
		h.refreshLimiter = newRefreshRateLimiter(0)
		key := BuildKey(requestInfoFromHTTP("GET", "http://example.com/rl", "", "", false, header.Map{}), nil)
		obj := &api.Object{
			Key:        key,
			StatusCode: 200,
			Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=60"}, header.ETag: []string{`"v1"`}}),
			Body:       []byte("body"),
			BodySize:   4,
			StoredAt:   time.Now(),
			TTL:        60 * time.Second,
			ETag:       `"v1"`,
		}
		_ = h.store.Put(context.Background(), key, obj)
		h.refreshRegistry.Register(key, requestInfoFromHTTP("GET", "", "", "", false, header.Map{}), "", 0)
		h.scheduler.Schedule(key, time.Now().Add(50*time.Millisecond))
		synctest.Sleep(200 * time.Millisecond)
		assert.Equal(t, 1, h.refreshRegistry.Len())
	})
}

func TestTriggerBgRefresh_SemaphoreFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		defer store.Close(context.Background())
		h := NewHandler(HandlerConfig{
			Upstream: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get(header.IfNoneMatch) == `"v1"` {
					w.WriteHeader(http.StatusNotModified)
					return
				}
				w.Header().Set(header.CacheControl, "max-age=60")
				w.Header().Set(header.ETag, `"v1"`)
				w.WriteHeader(200)
				_, _ = io.WriteString(w, "body")
			}),
			Store:               store,
			DefaultTTL:          60 * time.Second,
			RefreshBeforeExpiry: true,
			RefreshMargin:       6 * time.Second,
			RefreshTimeout:      5 * time.Second,
			RefreshConcurrency:  4,
			RefreshMinHits:      1,
			RouteName:           "test",
		})
		defer h.Close(context.Background())
		for range cap(h.refreshSem) {
			h.refreshSem <- struct{}{}
		}
		key := BuildKey(requestInfoFromHTTP("GET", "http://example.com/sem", "", "", false, header.Map{}), nil)
		obj := &api.Object{
			Key:        key,
			StatusCode: 200,
			Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=60"}, header.ETag: []string{`"v1"`}}),
			Body:       []byte("body"),
			BodySize:   4,
			StoredAt:   time.Now(),
			TTL:        60 * time.Second,
			ETag:       `"v1"`,
		}
		_ = h.store.Put(context.Background(), key, obj)
		h.refreshRegistry.Register(key, requestInfoFromHTTP("GET", "", "", "", false, header.Map{}), "", 0)
		h.scheduler.Schedule(key, time.Now().Add(50*time.Millisecond))
		synctest.Sleep(200 * time.Millisecond)
		assert.Equal(t, 1, h.refreshRegistry.Len())
		for range cap(h.refreshSem) {
			<-h.refreshSem
		}
	})
}

func TestHeaderGuard_BypassWrite(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.ContentType, "text/html")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("bypass-body"))
	}))
	r := httptest.NewRequest("GET", "http://example.com/bypass-write", nil)
	r.Header.Set(header.CacheControl, "no-store")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	assert.Equal(t, "BYPASS", rr.Header().Get(header.XCache))
	assert.Equal(t, "bypass-body", rr.Body.String())
}

func TestPurge_WithVariants(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.Vary, "Accept-Encoding")
		w.Header().Set(header.ETag, `"v1"`)
		w.WriteHeader(200)
		_, _ = w.Write([]byte("body-" + r.Header.Get(header.AcceptEncoding)))
	}))
	r1 := httptest.NewRequest("GET", "http://example.com/purge-var", nil)
	r1.Header.Set(header.AcceptEncoding, "gzip")
	h.ServeHTTP(httptest.NewRecorder(), r1)
	r2 := httptest.NewRequest("GET", "http://example.com/purge-var", nil)
	r2.Header.Set(header.AcceptEncoding, "br")
	h.ServeHTTP(httptest.NewRecorder(), r2)
	key := BuildKey(requestInfoFromURL("GET", "http://example.com/purge-var"), nil)
	owned, err := h.Purge(context.Background(), key)
	require.NoError(t, err)
	require.True(t, owned)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/purge-var", nil))
	assert.Equal(t, "MISS", rr.Header().Get(header.XCache))
}

func TestPurge_NotOwned(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	key := BuildKey(requestInfoFromHTTP("GET", "http://example.com/nonexistent", "", "", false, header.Map{}), nil)
	owned, err := h.Purge(context.Background(), key)
	require.NoError(t, err)
	require.False(t, owned)
}

func TestDoBackgroundRevalidate_DirectCall(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(header.IfNoneMatch) != "" {
			w.Header().Set(header.CacheControl, "max-age=120")
			w.WriteHeader(304)
			return
		}
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.ETag, `"v1"`)
		w.WriteHeader(200)
		_, _ = w.Write([]byte("body"))
	}))
	url := "http://example.com/reval-direct"
	r := httptest.NewRequest("GET", url, nil)
	key := BuildKey(requestInfoFromHTTP(r.Method, r.URL.String(), r.Host, r.URL.Path, r.TLS != nil, header.FromHTTP(r.Header)), nil)
	stale := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=60"}, header.ETag: []string{`"v1"`}}),
		Body:       []byte("old"),
		BodySize:   3,
		StoredAt:   time.Now().Add(-10 * time.Second),
		TTL:        60 * time.Second,
		ETag:       `"v1"`,
	}
	_ = h.store.Put(context.Background(), key, stale)
	h.doBackgroundRevalidate(context.Background(), r, key, stale)
	obj, _, _ := h.store.Get(context.Background(), key)
	require.NotNil(t, obj)
}

func TestTriggerBgRefresh_NotFound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		defer store.Close(context.Background())
		h := NewHandler(HandlerConfig{
			Upstream: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get(header.IfNoneMatch) == `"v1"` {
					w.WriteHeader(http.StatusNotModified)
					return
				}
				w.Header().Set(header.CacheControl, "max-age=60")
				w.Header().Set(header.ETag, `"v1"`)
				w.WriteHeader(200)
				_, _ = io.WriteString(w, "body")
			}),
			Store:               store,
			DefaultTTL:          60 * time.Second,
			RefreshBeforeExpiry: true,
			RefreshMargin:       6 * time.Second,
			RefreshTimeout:      5 * time.Second,
			RefreshConcurrency:  4,
			RefreshMinHits:      1,
			RouteName:           "test",
		})
		defer h.Close(context.Background())
		key := testkey.Key(999)
		h.refreshRegistry.Register(key, requestInfoFromHTTP("GET", "", "", "", false, header.Map{}), "", 0)
		h.scheduler.Schedule(key, time.Now().Add(50*time.Millisecond))
		synctest.Sleep(200 * time.Millisecond)
		assert.Equal(t, 0, h.refreshRegistry.Len())
	})
}

func TestTriggerBgRefresh_StaleObject(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		defer store.Close(context.Background())
		h := NewHandler(HandlerConfig{
			Upstream: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get(header.IfNoneMatch) == `"v1"` {
					w.WriteHeader(http.StatusNotModified)
					return
				}
				w.Header().Set(header.CacheControl, "max-age=60")
				w.Header().Set(header.ETag, `"v1"`)
				w.WriteHeader(200)
				_, _ = io.WriteString(w, "body")
			}),
			Store:               store,
			DefaultTTL:          60 * time.Second,
			RefreshBeforeExpiry: true,
			RefreshMargin:       6 * time.Second,
			RefreshTimeout:      5 * time.Second,
			RefreshConcurrency:  4,
			RefreshMinHits:      1,
			RouteName:           "test",
		})
		defer h.Close(context.Background())
		key := BuildKey(requestInfoFromHTTP("GET", "http://example.com/stale", "example.com", "/stale", false, header.Map{}), nil)
		obj := &api.Object{
			Key:        key,
			StatusCode: 200,
			Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=1"}}),
			Body:       []byte("stale"),
			BodySize:   5,
			StoredAt:   time.Now().Add(-10 * time.Second),
			TTL:        time.Second,
		}
		_ = h.store.Put(context.Background(), key, obj)
		h.refreshRegistry.Register(key, requestInfoFromHTTP("GET", "", "", "", false, header.Map{}), "", 0)
		h.scheduler.Schedule(key, time.Now().Add(50*time.Millisecond))
		synctest.Sleep(200 * time.Millisecond)
		assert.Equal(t, 0, h.refreshRegistry.Len())
	})
}

func TestTriggerBgRefresh_FreshObject(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		defer store.Close(context.Background())
		h := NewHandler(HandlerConfig{
			Upstream: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get(header.IfNoneMatch) == `"v1"` {
					w.WriteHeader(http.StatusNotModified)
					return
				}
				w.Header().Set(header.CacheControl, "max-age=60")
				w.Header().Set(header.ETag, `"v1"`)
				w.WriteHeader(200)
				_, _ = io.WriteString(w, "body")
			}),
			Store:               store,
			DefaultTTL:          60 * time.Second,
			RefreshBeforeExpiry: true,
			RefreshMargin:       6 * time.Second,
			RefreshTimeout:      5 * time.Second,
			RefreshConcurrency:  4,
			RefreshMinHits:      1,
			RouteName:           "test",
		})
		defer h.Close(context.Background())
		key := BuildKey(requestInfoFromHTTP("GET", "http://example.com/fresh", "", "", false, header.Map{}), nil)
		obj := &api.Object{
			Key:        key,
			StatusCode: 200,
			Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=60"}, header.ETag: []string{`"v1"`}}),
			Body:       []byte("fresh"),
			BodySize:   5,
			StoredAt:   time.Now(),
			TTL:        60 * time.Second,
			ETag:       `"v1"`,
		}
		_ = h.store.Put(context.Background(), key, obj)
		h.refreshRegistry.Register(key, requestInfoFromHTTP("GET", "", "", "", false, header.Map{}), "", 0)
		h.scheduler.Schedule(key, time.Now().Add(50*time.Millisecond))
		synctest.Sleep(200 * time.Millisecond)
	})
}

func TestHeaderGuard_Write(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	// Create a bypass request to test headerGuard.
	r := httptest.NewRequest("GET", "http://example.com/bypass", nil)
	r.Header.Set(header.CacheControl, "no-store")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	// The bypass path wraps the writer in headerGuard which sets X-Cache.
	assert.Equal(t, "BYPASS", rr.Header().Get(header.XCache))
}

func TestResponseRecorder_WriteHeader(t *testing.T) {
	t.Parallel()
	rec := acquireRecorder(1 << 20)
	rec.WriteHeader(200)
	assert.Equal(t, 200, rec.statusCode)
	releaseRecorder(rec)
}

func TestResponseRecorder_Write(t *testing.T) {
	t.Parallel()
	rec := acquireRecorder(1 << 20)
	_, _ = rec.Write([]byte("hello"))
	assert.Equal(t, 5, rec.body.Len())
	releaseRecorder(rec)
}

func TestFetchAndStoreStayinAlive_5xxFallback(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(503)
		}),
		Store:       store,
		StayinAlive: true,
	})
	// Store a stale object manually.
	key := BuildKey(requestInfoFromHTTP("GET", "http://example.com/stayin5xx", "", "", false, header.Map{}), nil)
	stale := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=1, stale-while-revalidate=60"}, header.ETag: []string{`"v1"`}}),
		Body:       []byte("stale-body"),
		BodySize:   9,
		StoredAt:   time.Now().Add(-10 * time.Second),
		TTL:        time.Second,
		ETag:       `"v1"`,
	}
	_ = store.Put(context.Background(), key, stale)
	// Call fetchAndStoreStayinAlive directly.
	r := httptest.NewRequest("GET", "http://example.com/stayin5xx", nil)
	rr := httptest.NewRecorder()
	h.fetchAndStoreStayinAlive(rr, r, key, key, stale, time.Now(), api.SourceHot)
	assert.Equal(t, "STALE", rr.Header().Get(header.XCache))
	assert.Equal(t, "stale-body", rr.Body.String())
}

func TestFetchAndStoreStayinAlive_ErrorFallback(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Origin down: block until context cancelled (timeout).
			<-r.Context().Done()
		}),
		Store:        store,
		StayinAlive:  true,
		FetchTimeout: 100 * time.Millisecond,
	})
	key := BuildKey(requestInfoFromHTTP("GET", "http://example.com/stayin-err", "", "", false, header.Map{}), nil)
	stale := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=1, stale-while-revalidate=60"}, header.ETag: []string{`"v1"`}}),
		Body:       []byte("stale-body"),
		BodySize:   9,
		StoredAt:   time.Now().Add(-10 * time.Second),
		TTL:        time.Second,
		ETag:       `"v1"`,
	}
	_ = store.Put(context.Background(), key, stale)
	r := httptest.NewRequest("GET", "http://example.com/stayin-err", nil)
	rr := httptest.NewRecorder()
	h.fetchAndStoreStayinAlive(rr, r, key, key, stale, time.Now(), api.SourceHot)
	assert.Equal(t, "STALE", rr.Header().Get(header.XCache))
}

func TestHandler_SyntheticTimeHitMiss(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		defer store.Close(context.Background())
		h := NewHandler(HandlerConfig{
			Upstream: origin200("body"),
			Store:    store,
		})
		// First request: MISS, stores object with max-age=60.
		r1 := httptest.NewRequest("GET", "http://example.com/synctime", nil)
		rr1 := httptest.NewRecorder()
		h.ServeHTTP(rr1, r1)
		require.Equal(t, "MISS", rr1.Header().Get(header.XCache))

		// Advance synthetic clock by 30s — object still fresh.
		synctest.Sleep(30 * time.Second)
		r2 := httptest.NewRequest("GET", "http://example.com/synctime", nil)
		rr2 := httptest.NewRecorder()
		h.ServeHTTP(rr2, r2)
		require.Equal(t, "HIT", rr2.Header().Get(header.XCache))

		// Advance past TTL — object is stale, triggers revalidation.
		synctest.Sleep(40 * time.Second)
		r3 := httptest.NewRequest("GET", "http://example.com/synctime", nil)
		rr3 := httptest.NewRecorder()
		h.ServeHTTP(rr3, r3)
		// Origin returns 200 with max-age=60, so it re-fetches (not HIT).
		assert.NotEqual(t, "HIT", rr3.Header().Get(header.XCache))
	})
}

func TestHandler_SyntheticTimeBackgroundRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := storage.NewHotStore(storage.HotConfig{
			MaxBytes:  1 << 20,
			NumShards: 2,
		})
		defer store.Close(context.Background())
		upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get(header.IfNoneMatch) == `"v1"` {
				w.Header().Set(header.CacheControl, "max-age=120")
				w.WriteHeader(http.StatusNotModified)
				return
			}
			w.Header().Set(header.CacheControl, "max-age=60")
			w.Header().Set(header.ETag, `"v1"`)
			w.WriteHeader(200)
			_, _ = w.Write([]byte("body"))
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
			RefreshMinHits:      1,
			RouteName:           "test",
		})
		defer h.Close(context.Background())

		key := BuildKey(requestInfoFromHTTP("GET", "http://example.com/bgrefresh", "", "", false, header.Map{}), nil)
		obj := &api.Object{
			Key:        key,
			StatusCode: 200,
			Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=60"}, header.ETag: []string{`"v1"`}}),
			Body:       []byte("body"),
			BodySize:   4,
			StoredAt:   time.Now(),
			TTL:        60 * time.Second,
			ETag:       `"v1"`,
		}
		_ = h.store.Put(context.Background(), key, obj)
		h.refreshRegistry.Register(key, requestInfoFromHTTP("GET", "", "", "", false, header.Map{}), "", 0)

		// Schedule refresh at now + 50ms and advance synthetic time.
		// synctest.Sleep advances the fake clock AND waits for all
		// goroutines to settle, so the background refresh completes
		// before we check the store.
		h.scheduler.Schedule(key, time.Now().Add(50*time.Millisecond))
		synctest.Sleep(200 * time.Millisecond)

		// The refresh should have completed (304 path updates TTL to 120s).
		updated, _, _ := h.store.Get(context.Background(), key)
		require.NotNil(t, updated)
		assert.Equal(t, 120*time.Second, updated.TTL)
	})
}

// ---------------------------------------------------------------------------
// SoftPurge tests
// ---------------------------------------------------------------------------

// TestSoftPurge_MarksObjectStale verifies that SoftPurge reduces the
// TTL to zero, making the object immediately stale while keeping it in
// the store (unlike Purge which deletes it).
func TestSoftPurge_MarksObjectStale(t *testing.T) {
	t.Parallel()

	orig := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=3600, stale-while-revalidate=60")
		w.Header().Set(header.ETag, `"v1"`)
		_, _ = w.Write([]byte("cached-body"))
	})

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20})
	h := NewHandler(HandlerConfig{
		Upstream: orig,
		Store:    store,
		Logger:   slog.Default(),
	})

	req := httptest.NewRequest("GET", "http://example.com/soft-purge", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	key := h.buildKey(req)
	obj, _, _ := store.Get(context.Background(), key)
	require.NotNil(t, obj)
	require.True(t, obj.Fresh(time.Now()), "object should be fresh before soft purge")

	owned, err := h.SoftPurge(context.Background(), key)
	require.NoError(t, err)
	require.True(t, owned)

	// Object must still be in the store (not deleted).
	obj2, _, _ := store.Get(context.Background(), key)
	require.NotNil(t, obj2, "object must still be in store after soft purge")
	// Object must now be stale (TTL reduced to zero).
	require.False(t, obj2.Fresh(time.Now()), "object should be stale after soft purge")
	// Object must be within the SWR window (servable while revalidating).
	require.True(t, obj2.StaleForSWR(time.Now()), "object should be in SWR window after soft purge")
	// Body must be preserved.
	require.Equal(t, "cached-body", string(obj2.Body))
}

// TestSoftPurge_UnknownKeyReturnsFalse verifies that SoftPurge on a key
// that was never cached returns owned=false with no error.
func TestSoftPurge_UnknownKeyReturnsFalse(t *testing.T) {
	t.Parallel()

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20})
	h := NewHandler(HandlerConfig{
		Upstream: origin200("x"),
		Store:    store,
		Logger:   slog.Default(),
	})

	owned, err := h.SoftPurge(context.Background(), testkey.Key(999))
	require.NoError(t, err)
	require.False(t, owned, "should not own unknown key")
}

// TestSoftPurge_NoSWRFallsBackToDelete verifies that when an object has
// no stale-while-revalidate window, SoftPurge falls back to a hard delete
// (since the stale body would not be servable).
func TestSoftPurge_NoSWRFallsBackToDelete(t *testing.T) {
	t.Parallel()

	orig := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=3600")
		w.Header().Set(header.ETag, `"v1"`)
		_, _ = w.Write([]byte("no-swr"))
	})

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20})
	h := NewHandler(HandlerConfig{
		Upstream: orig,
		Store:    store,
		Logger:   slog.Default(),
	})

	req := httptest.NewRequest("GET", "http://example.com/no-swr", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	key := h.buildKey(req)
	obj, _, _ := store.Get(context.Background(), key)
	require.NotNil(t, obj)
	require.Equal(t, time.Duration(0), obj.StaleWhileRevalidate)

	owned, err := h.SoftPurge(context.Background(), key)
	require.NoError(t, err)
	require.True(t, owned)

	// Object must be deleted (no SWR window to serve stale).
	obj2, _, _ := store.Get(context.Background(), key)
	require.Nil(t, obj2, "object should be deleted when no SWR window")
}

// TestSoftPurge_PreservesValidators verifies that ETag and Last-Modified
// are preserved after a soft purge, enabling conditional revalidation.
func TestSoftPurge_PreservesValidators(t *testing.T) {
	t.Parallel()

	orig := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=3600, stale-while-revalidate=60")
		w.Header().Set(header.ETag, `"etag-123"`)
		w.Header().Set(header.LastModified, "Mon, 01 Jan 2024 00:00:00 GMT")
		_, _ = w.Write([]byte("body"))
	})

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20})
	h := NewHandler(HandlerConfig{
		Upstream: orig,
		Store:    store,
		Logger:   slog.Default(),
	})

	req := httptest.NewRequest("GET", "http://example.com/validators", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	key := h.buildKey(req)
	_, err := h.SoftPurge(context.Background(), key)
	require.NoError(t, err)

	obj, _, _ := store.Get(context.Background(), key)
	require.NotNil(t, obj)
	require.Equal(t, `"etag-123"`, obj.ETag, "ETag must be preserved")
	require.False(t, obj.LastModified.IsZero(), "LastModified must be preserved")
}

// TestSoftPurge_ServeStaleThenRevalidate verifies the end-to-end soft
// purge flow: after soft purge, the next request serves the stale body
// (STALE) and triggers a background revalidation.
func TestSoftPurge_ServeStaleThenRevalidate(t *testing.T) {
	t.Parallel()

	var originHits atomic.Int64
	orig := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		originHits.Add(1)
		w.Header().Set(header.CacheControl, "max-age=3600, stale-while-revalidate=60")
		w.Header().Set(header.ETag, `"v1"`)
		_, _ = w.Write([]byte("original"))
	})

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20})
	h := NewHandler(HandlerConfig{
		Upstream: orig,
		Store:    store,
		Logger:   slog.Default(),
	})

	req := httptest.NewRequest("GET", "http://example.com/swr-flow", nil)

	// First request: cache miss, fetches from origin.
	rr1 := httptest.NewRecorder()
	h.ServeHTTP(rr1, req)
	require.Equal(t, "MISS", rr1.Header().Get(header.XCache))
	require.Equal(t, int64(1), originHits.Load())

	key := h.buildKey(req)

	// Soft purge: mark the object stale.
	_, err := h.SoftPurge(context.Background(), key)
	require.NoError(t, err)

	// Second request: should serve stale body (STALE) from cache.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, req)
	require.Equal(t, "STALE", rr2.Header().Get(header.XCache), "should serve stale after soft purge")
	require.Equal(t, "original", rr2.Body.String(), "should serve the original cached body")

	// The background revalidation should have fetched from origin
	// (conditional request). Wait briefly for the background goroutine.
	// The origin should have been hit again for the revalidation.
	require.Eventually(t, func() bool {
		return originHits.Load() >= 2
	}, 2*time.Second, 10*time.Millisecond, "background revalidation should hit origin")
}

// TestSoftPurge_Variants verifies that SoftPurge marks all variants
// stale when soft-purging the primary key.
func TestSoftPurge_Variants(t *testing.T) {
	t.Parallel()

	orig := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=3600, stale-while-revalidate=60")
		w.Header().Set(header.ETag, `"v1"`)
		w.Header().Set(header.Vary, "X-Test-Variant")
		_, _ = w.Write([]byte(r.Header.Get("X-Test-Variant")))
	})

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20})
	h := NewHandler(HandlerConfig{
		Upstream: orig,
		Store:    store,
		Logger:   slog.Default(),
	})

	reqA := httptest.NewRequest("GET", "http://example.com/vary-sp", nil)
	reqA.Header.Set("X-Test-Variant", "a")
	h.ServeHTTP(httptest.NewRecorder(), reqA)

	reqB := httptest.NewRequest("GET", "http://example.com/vary-sp", nil)
	reqB.Header.Set("X-Test-Variant", "b")
	h.ServeHTTP(httptest.NewRecorder(), reqB)

	primaryKey := h.buildKey(reqA)
	keyA := VariantKey(primaryKey, "X-Test-Variant", header.FromHTTP(reqA.Header), nil)
	keyB := VariantKey(primaryKey, "X-Test-Variant", header.FromHTTP(reqB.Header), nil)

	// All should be fresh.
	objA, _, _ := store.Get(context.Background(), keyA)
	require.NotNil(t, objA)
	require.True(t, objA.Fresh(time.Now()))
	objB, _, _ := store.Get(context.Background(), keyB)
	require.NotNil(t, objB)
	require.True(t, objB.Fresh(time.Now()))

	// Soft purge the primary key.
	owned, err := h.SoftPurge(context.Background(), primaryKey)
	require.NoError(t, err)
	require.True(t, owned)

	// All variants should be stale but still in the store.
	objA2, _, _ := store.Get(context.Background(), keyA)
	require.NotNil(t, objA2, "variant A should still be in store")
	require.False(t, objA2.Fresh(time.Now()), "variant A should be stale")
	require.True(t, objA2.StaleForSWR(time.Now()), "variant A should be in SWR window")

	objB2, _, _ := store.Get(context.Background(), keyB)
	require.NotNil(t, objB2, "variant B should still be in store")
	require.False(t, objB2.Fresh(time.Now()), "variant B should be stale")
	require.True(t, objB2.StaleForSWR(time.Now()), "variant B should be in SWR window")
}

// TestSoftPurge_AlreadyStaleObject verifies that soft-purging an
// already-stale object is a no-op that still returns owned=true.
func TestSoftPurge_AlreadyStaleObject(t *testing.T) {
	t.Parallel()

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20})
	key := testkey.Key(42)
	obj := &api.Object{
		Key:                  key,
		StatusCode:           200,
		Header:               header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=1, stale-while-revalidate=60"}, header.ETag: []string{`"v1"`}}),
		Body:                 []byte("stale"),
		BodySize:             5,
		StoredAt:             time.Now().Add(-10 * time.Second),
		TTL:                  1 * time.Second,
		StaleWhileRevalidate: 60 * time.Second,
		ETag:                 `"v1"`,
	}
	require.NoError(t, store.Put(context.Background(), key, obj))
	require.False(t, obj.Fresh(time.Now()))

	h := NewHandler(HandlerConfig{
		Upstream: origin200("x"),
		Store:    store,
		Logger:   slog.Default(),
	})

	owned, err := h.SoftPurge(context.Background(), key)
	require.NoError(t, err)
	require.True(t, owned, "should own the stale object")

	// Object should still be in the store and still stale.
	obj2, _, _ := store.Get(context.Background(), key)
	require.NotNil(t, obj2)
	require.False(t, obj2.Fresh(time.Now()))
	require.True(t, obj2.StaleForSWR(time.Now()))
}

// TestSoftPurge_SIEOnlyNoSWR verifies that an object with only
// stale-if-error (no SWR) is still soft-purged (kept in store), since SIE
// allows serving the stale body when the origin returns an error.
func TestSoftPurge_SIEOnlyNoSWR(t *testing.T) {
	t.Parallel()

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20})
	key := testkey.Key(77)
	obj := &api.Object{
		Key:          key,
		StatusCode:   200,
		Header:       header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=3600, stale-if-error=60"}, header.ETag: []string{`"v1"`}}),
		Body:         []byte("sie-only"),
		BodySize:     8,
		StoredAt:     time.Now(),
		TTL:          3600 * time.Second,
		StaleIfError: 60 * time.Second,
		ETag:         `"v1"`,
	}
	require.NoError(t, store.Put(context.Background(), key, obj))

	h := NewHandler(HandlerConfig{
		Upstream: origin200("x"),
		Store:    store,
		Logger:   slog.Default(),
	})

	owned, err := h.SoftPurge(context.Background(), key)
	require.NoError(t, err)
	require.True(t, owned)

	// SIE-only object should still be in the store and stale.
	obj2, _, _ := store.Get(context.Background(), key)
	require.NotNil(t, obj2, "SIE-only object should be kept in store (can serve stale on origin error)")
	require.False(t, obj2.Fresh(time.Now()), "object should be stale")
	require.True(t, obj2.StaleForSIE(time.Now()), "object should be in SIE window")
}

// TestSoftPurge_NoSWRAndNoSIE_FallsBackToDelete verifies that an object
// with neither stale-while-revalidate nor stale-if-error is hard-deleted
// by SoftPurge, since the stale body would never be servable.
func TestSoftPurge_NoSWRAndNoSIE_FallsBackToDelete(t *testing.T) {
	t.Parallel()

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20})
	key := testkey.Key(88)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=3600"}, header.ETag: []string{`"v1"`}}),
		Body:       []byte("no-grace"),
		BodySize:   8,
		StoredAt:   time.Now(),
		TTL:        3600 * time.Second,
		ETag:       `"v1"`,
	}
	require.NoError(t, store.Put(context.Background(), key, obj))

	h := NewHandler(HandlerConfig{
		Upstream: origin200("x"),
		Store:    store,
		Logger:   slog.Default(),
	})

	owned, err := h.SoftPurge(context.Background(), key)
	require.NoError(t, err)
	require.True(t, owned)

	obj2, _, _ := store.Get(context.Background(), key)
	require.Nil(t, obj2, "object with no SWR and no SIE should be deleted")
}

// errorOnPutStore wraps a real store but returns an error on Put.
type errorOnPutStore struct {
	storage.Store
	putErr error
}

func (s *errorOnPutStore) Put(_ context.Context, _ api.Key, _ *api.Object) error {
	return s.putErr
}

// TestSoftPurge_PutError verifies that SoftPurge returns the error when
// store.Put fails during the soft-purge operation.
func TestSoftPurge_PutError(t *testing.T) {
	t.Parallel()

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20})
	key := testkey.Key(55)
	obj := &api.Object{
		Key:                  key,
		StatusCode:           200,
		Header:               header.FromHTTP(http.Header{header.CacheControl: []string{"max-age=3600, stale-while-revalidate=60"}, header.ETag: []string{`"v1"`}}),
		Body:                 []byte("body"),
		BodySize:             4,
		StoredAt:             time.Now(),
		TTL:                  3600 * time.Second,
		StaleWhileRevalidate: 60 * time.Second,
		ETag:                 `"v1"`,
	}
	require.NoError(t, store.Put(context.Background(), key, obj))

	errStore := &errorOnPutStore{Store: store, putErr: errors.New("disk full")}
	h := NewHandler(HandlerConfig{
		Upstream: origin200("x"),
		Store:    errStore,
		Logger:   slog.Default(),
	})

	owned, err := h.SoftPurge(context.Background(), key)
	require.Error(t, err, "should return Put error")
	require.True(t, owned, "should still report owned=true since the key was found")
}

// TestSoftPurge_RefreshRegistryUnregistered verifies that when SoftPurge
// falls back to hard delete (no SWR/SIE), the refresh registry is also
// unregistered, preventing background refresh from resurrecting the key.
func TestSoftPurge_RefreshRegistryUnregistered(t *testing.T) {
	t.Parallel()

	orig := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=3600")
		w.Header().Set(header.ETag, `"v1"`)
		_, _ = w.Write([]byte("no-swr"))
	})

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20})
	h := NewHandler(HandlerConfig{
		Upstream:            orig,
		Store:               store,
		Logger:              slog.Default(),
		RefreshBeforeExpiry: true,
		RefreshMargin:       30 * time.Second,
	})

	req := httptest.NewRequest("GET", "http://example.com/refresh-reg", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	key := h.buildKey(req)
	require.NotNil(t, h.refreshRegistry)
	require.Equal(t, 1, h.refreshRegistry.Len(), "object should be registered for refresh")

	owned, err := h.SoftPurge(context.Background(), key)
	require.NoError(t, err)
	require.True(t, owned)

	require.Equal(t, 0, h.refreshRegistry.Len(), "refresh registry should be cleared after soft purge with hard delete")
}
