package cache

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/header"
)

func TestBuildKey_Deterministic(t *testing.T) {
	t.Parallel()
	r1 := httptest.NewRequest("GET", "http://example.com/foo?b=2&a=1", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/foo?a=1&b=2", nil)
	require.Equal(t, BuildKey(r2, nil), BuildKey(r1, nil))
}

func TestBuildKey_HeadSharesGet(t *testing.T) {
	t.Parallel()
	r1 := httptest.NewRequest("GET", "http://example.com/x", nil)
	r2 := httptest.NewRequest("HEAD", "http://example.com/x", nil)
	require.Equal(t, BuildKey(r2, nil), BuildKey(r1, nil))
}

func TestBuildKey_DifferentPaths(t *testing.T) {
	t.Parallel()
	r1 := httptest.NewRequest("GET", "http://example.com/a", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/b", nil)
	require.NotEqual(t, BuildKey(r2, nil), BuildKey(r1, nil))
}

func TestBuildKey_SchemeMatters(t *testing.T) {
	t.Parallel()
	r1 := httptest.NewRequest("GET", "http://example.com/", nil)
	r2 := httptest.NewRequest("GET", "https://example.com/", nil)
	require.NotEqual(t, BuildKey(r2, nil), BuildKey(r1, nil))
}

func TestBuildKey_DefaultPortStripped(t *testing.T) {
	t.Parallel()
	r1 := httptest.NewRequest("GET", "http://example.com/", nil)
	r2 := httptest.NewRequest("GET", "http://example.com:80/", nil)
	require.Equal(t, BuildKey(r2, nil), BuildKey(r1, nil))
}

func TestBuildKey_DuplicateSlashes(t *testing.T) {
	t.Parallel()
	r1 := httptest.NewRequest("GET", "http://example.com/a/b", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/a//b", nil)
	require.Equal(t, BuildKey(r2, nil), BuildKey(r1, nil))
}

func TestBuildKey_HostNormalization(t *testing.T) {
	t.Parallel()
	// Same host, different casing → same key.
	r1 := httptest.NewRequest("GET", "http://Example.COM/a", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/a", nil)
	require.Equal(t, BuildKey(r2, nil), BuildKey(r1, nil))

	// Non-default port produces different key.
	r3 := httptest.NewRequest("GET", "http://example.com:8080/a", nil)
	require.NotEqual(t, BuildKey(r3, nil), BuildKey(r1, nil))
}

func TestBuildKey_LongURLNoPanic(t *testing.T) {
	t.Parallel()
	// Regression: URLs whose canonical key exceeds 512 bytes must not
	// panic with "index out of range [512]". This was a production crash
	// in a staging deployment (see key.go:67).
	longPath := strings.Repeat("a", 600)
	r := httptest.NewRequest("GET", "http://example.com/"+longPath+"?b=2&a=1", nil)
	// Must not panic.
	k := BuildKey(r, nil)
	require.False(t, k.IsZero())
}

func TestBuildKey_VaryKeyLongNoPanic(t *testing.T) {
	t.Parallel()
	// Regression: BuildVaryKey must not panic when Vary header values
	// exceed the 256-byte stack buffer.
	longVal := strings.Repeat("x", 300)
	reqHeader := http.Header{
		header.AcceptLanguage: {longVal},
		header.AcceptEncoding: {longVal},
	}
	// Must not panic.
	_ = BuildVaryKey("Accept-Language, Accept-Encoding", reqHeader, nil)
}

func TestBuildVaryKey_ExcludeHeader(t *testing.T) {
	t.Parallel()
	excludePolicy := NewKeyPolicy(nil, nil, map[string]bool{"x-request-id": true}, nil, false, false)
	h1 := http.Header{header.AcceptEncoding: {"gzip"}, "X-Request-Id": {"abc"}}
	h2 := http.Header{header.AcceptEncoding: {"gzip"}, "X-Request-Id": {"xyz"}}
	k1 := BuildVaryKey("Accept-Encoding, X-Request-Id", h1, excludePolicy)
	k2 := BuildVaryKey("Accept-Encoding, X-Request-Id", h2, excludePolicy)
	require.Equal(t, k2, k1)
	// Without exclusion, keys should differ.
	k3 := BuildVaryKey("Accept-Encoding, X-Request-Id", h1, nil)
	k4 := BuildVaryKey("Accept-Encoding, X-Request-Id", h2, nil)
	require.NotEqual(t, k4, k3)
}

func TestBuildVaryKey_ExcludeAllHeaders(t *testing.T) {
	t.Parallel()
	excludePolicy := NewKeyPolicy(nil, nil, map[string]bool{"x-request-id": true}, nil, false, false)
	h1 := http.Header{"X-Request-Id": {"abc"}}
	h2 := http.Header{"X-Request-Id": {"xyz"}}
	k1 := BuildVaryKey("X-Request-Id", h1, excludePolicy)
	k2 := BuildVaryKey("X-Request-Id", h2, excludePolicy)
	require.Equal(t, k2, k1)
}

func TestParseCacheControl(t *testing.T) {
	t.Parallel()
	d := ParseCacheControl("max-age=300, public, stale-while-revalidate=60")
	if !d.MaxAgeSet || d.MaxAge.Seconds() != 300 {
		t.Errorf("max-age = %v", d.MaxAge)
	}
	assert.True(t, d.Public)
	if !d.StaleWhileRevalidSet || d.StaleWhileRevalid.Seconds() != 60 {
		t.Errorf("swr = %v", d.StaleWhileRevalid)
	}
}

func TestParseCacheControl_NoStore(t *testing.T) {
	t.Parallel()
	d := ParseCacheControl("no-store")
	assert.True(t, d.NoStore)
}

func TestParseCacheControl_MaxStaleNoValue(t *testing.T) {
	t.Parallel()
	d := ParseCacheControl("max-stale")
	assert.True(t, d.MaxStaleSet)
	if d.MaxStale <= 0 {
		t.Error("max-stale without value should be infinite")
	}
}

func TestIsCacheable_BasicPositive(t *testing.T) {
	t.Parallel()
	resp := http.Header{header.CacheControl: {"max-age=60"}}
	require.True(t, IsCacheable(200, http.Header{}, resp))
}

func TestIsCacheable_NoStore(t *testing.T) {
	t.Parallel()
	resp := http.Header{header.CacheControl: {"no-store"}}
	require.False(t, IsCacheable(200, http.Header{}, resp))
}

func TestIsCacheable_Private(t *testing.T) {
	t.Parallel()
	resp := http.Header{header.CacheControl: {"private, max-age=60"}}
	require.False(t, IsCacheable(200, http.Header{}, resp))
}

func TestIsCacheable_SetCookie(t *testing.T) {
	t.Parallel()
	// Set-Cookie WITHOUT explicit freshness blocks caching.
	resp := http.Header{
		header.SetCookie: {"sid=abc"},
	}
	require.False(t, IsCacheable(200, http.Header{}, resp))
	// Set-Cookie WITH explicit max-age is cacheable (shared cache behavior).
	resp2 := http.Header{
		header.CacheControl: {"max-age=60"},
		header.SetCookie:    {"sid=abc"},
	}
	require.True(t, IsCacheable(200, http.Header{}, resp2))
}

func TestIsCacheable_Authorization(t *testing.T) {
	t.Parallel()
	req := http.Header{header.Authorization: {"Bearer tok"}}
	resp := http.Header{header.CacheControl: {"max-age=60"}}
	require.False(t, IsCacheable(200, req, resp))

	resp2 := http.Header{header.CacheControl: {"max-age=60, public"}}
	require.True(t, IsCacheable(200, req, resp2))
}

func TestIsCacheable_HeuristicStatus(t *testing.T) {
	t.Parallel()
	// 301 with Last-Modified is heuristically cacheable.
	resp := http.Header{header.LastModified: {"Mon, 01 Jan 2024 00:00:00 GMT"}}
	require.True(t, IsCacheable(301, http.Header{}, resp))
	// 301 without Last-Modified is NOT heuristically cacheable.
	require.False(t, IsCacheable(301, http.Header{}, http.Header{}))
	// 302 is never heuristically cacheable.
	require.False(t, IsCacheable(302, http.Header{}, http.Header{header.LastModified: {"Mon, 01 Jan 2024 00:00:00 GMT"}}))
}
