package cache

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
)

// newDefaultTTLHandler builds a Handler with DefaultTTL set, backed by an
// in-process hot store.
func newDefaultTTLHandler(t *testing.T, upstream http.Handler, def time.Duration) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	return NewHandler(HandlerConfig{
		Upstream:   wrapUpstream(upstream),
		FastClient: &mockOriginClient{status: 200, body: []byte("body"), headers: http.Header{header.CacheControl: []string{"max-age=60"}}},
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

	require.False(t, IsCacheable(200, header.FromHTTP(req), header.FromHTTP(resp)))
	assert.True(t, IsCacheableWithDefault(200, header.FromHTTP(req), header.FromHTTP(resp), 0, 5*time.Second))
	assert.False(t, IsCacheableWithDefault(200, header.FromHTTP(req), header.FromHTTP(resp), 0, 0))
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
		{"no-store", 200, http.Header{}, http.Header{header.CacheControl: {"no-store"}}, false},
		{"private", 200, http.Header{}, http.Header{header.CacheControl: {"private"}}, false},
		{"set-cookie", 200, http.Header{}, http.Header{header.SetCookie: {"sid=abc"}}, false},
		{"vary-star", 200, http.Header{}, http.Header{header.Vary: {"*"}}, false},
		{"pragma-no-cache", 200, http.Header{}, http.Header{header.Pragma: {"no-cache"}}, false},
		{"authorization", 200, http.Header{header.Authorization: {"Bearer x"}}, http.Header{}, false},
		{"5xx-excluded", 500, http.Header{}, http.Header{}, false},
		{"plain-200-ok", 200, http.Header{}, http.Header{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := IsCacheableWithDefault(tc.status, header.FromHTTP(tc.req), header.FromHTTP(tc.resp), 0, def)
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
	rr1 := newRR()
	h.ServeHTTPCompat(rr1, req)
	got := rr1.Header().Get(header.XCache)
	require.Equal(t, "MISS", got)

	rr2 := newRR()
	h.ServeHTTPCompat(rr2, httptest.NewRequest("GET", "http://example.com/r", nil))
	got = rr2.Header().Get(header.XCache)
	require.Equal(t, "HIT", got)
	assert.Equal(t, 1, hits)
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

	for range 2 {
		var rr *httptest.ResponseRecorder
		rr = newRR()
		h.ServeHTTPCompat(rr, httptest.NewRequest("GET", "http://example.com/r", nil))
		got := rr.Header().Get(header.XCache)
		require.Equal(t, "MISS", got)
	}
}

// TestDefaultTTL_NoStoreStillBypasses verifies an explicit no-store response
// is never cached even with DefaultTTL set.
func TestDefaultTTL_NoStoreStillBypasses(t *testing.T) {
	t.Parallel()
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "no-store")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	h := newDefaultTTLHandler(t, upstream, 5*time.Second)

	for range 2 {
		var rr *httptest.ResponseRecorder
		rr = newRR()
		h.ServeHTTPCompat(rr, httptest.NewRequest("GET", "http://example.com/r", nil))
		got := rr.Header().Get(header.XCache)
		require.Equal(t, "MISS", got)
	}
}
