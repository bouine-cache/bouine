package cache

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
)

func newSetCookieHandler(t *testing.T, upstream http.Handler, allow bool) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	return NewHandler(HandlerConfig{
		Upstream:       wrapUpstream(upstream),
		FastClient:     &handlerFastClient{handler: upstream},
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
	rr1 := newRR()
	h.ServeHTTPCompat(rr1, httptest.NewRequest("GET", url, nil))

	require.Equal(t, 200, rr1.Code)
	assert.Equal(t, "session=abc123; Path=/", rr1.Header().Get(header.SetCookie))

	rr2 := newRR()
	h.ServeHTTPCompat(rr2, httptest.NewRequest("GET", url, nil))

	assert.Equal(t, 2, calls)
	assert.NotEqual(t, "HIT", rr2.Header().Get(header.XCache))
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
	rr1 := newRR()
	h.ServeHTTPCompat(rr1, httptest.NewRequest("GET", url, nil))
	require.Equal(t, 200, rr1.Code)
	assert.NotEqual(t, "", rr1.Header().Get(header.SetCookie))

	// Second request: HIT — must NOT have Set-Cookie.
	rr2 := newRR()
	h.ServeHTTPCompat(rr2, httptest.NewRequest("GET", url, nil))
	assert.Equal(t, "HIT", rr2.Header().Get(header.XCache))
	assert.Equal(t, "", rr2.Header().Get(header.SetCookie))
	assert.Equal(t, 1, calls)
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
		rr := newRR()
		h.ServeHTTPCompat(rr, httptest.NewRequest("GET", url, nil))
	}
	assert.Equal(t, 3, calls)
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
	rr := newRR()
	h.ServeHTTPCompat(rr, httptest.NewRequest("GET", url, nil))
	rr2 := newRR()
	h.ServeHTTPCompat(rr2, httptest.NewRequest("GET", url, nil))

	assert.Equal(t, "HIT", rr2.Header().Get(header.XCache))
	assert.Equal(t, 1, calls)
}

// TestSetCookie_DefaultBlocksEvenWithExplicitFreshness is the security
// regression test: Set-Cookie + max-age=3600 must NOT be cached when
// AllowSetCookie defaults to false. This was the pre-fix vulnerability.
func TestSetCookie_DefaultBlocksEvenWithExplicitFreshness(t *testing.T) {
	t.Parallel()
	upstream := originWithSetCookie("body", "max-age=3600", "token=secret123")
	h := newSetCookieHandler(t, upstream, false)

	url := "http://example.com/important"
	rr := newRR()
	h.ServeHTTPCompat(rr, httptest.NewRequest("GET", url, nil))

	// Verify the object was NOT stored.
	key := BuildKey(requestInfoFromURL("GET", url), nil)
	obj, _, _ := h.store.Get(httptest.NewRequest("GET", url, nil).Context(), key)
	require.Nil(t, obj)
}
