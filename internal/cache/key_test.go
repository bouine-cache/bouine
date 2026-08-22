package cache

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestBuildKey_Deterministic(t *testing.T) {
	t.Parallel()
	r1 := httptest.NewRequest("GET", "http://example.com/foo?b=2&a=1", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/foo?a=1&b=2", nil)
	require.Equal(t, BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), nil), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), nil))
}

func TestBuildKey_HeadSharesGet(t *testing.T) {
	t.Parallel()
	r1 := httptest.NewRequest("GET", "http://example.com/x", nil)
	r2 := httptest.NewRequest("HEAD", "http://example.com/x", nil)
	require.Equal(t, BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), nil), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), nil))
}

func TestBuildKey_DifferentPaths(t *testing.T) {
	t.Parallel()
	r1 := httptest.NewRequest("GET", "http://example.com/a", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/b", nil)
	require.NotEqual(t, BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), nil), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), nil))
}

func TestBuildKey_SchemeMatters(t *testing.T) {
	t.Parallel()
	r1 := httptest.NewRequest("GET", "http://example.com/", nil)
	r2 := httptest.NewRequest("GET", "https://example.com/", nil)
	require.NotEqual(t, BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), nil), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), nil))
}

func TestBuildKey_DefaultPortStripped(t *testing.T) {
	t.Parallel()
	r1 := httptest.NewRequest("GET", "http://example.com/", nil)
	r2 := httptest.NewRequest("GET", "http://example.com:80/", nil)
	require.Equal(t, BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), nil), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), nil))
}

func TestBuildKey_DuplicateSlashes(t *testing.T) {
	t.Parallel()
	r1 := httptest.NewRequest("GET", "http://example.com/a/b", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/a//b", nil)
	require.Equal(t, BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), nil), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), nil))
}

func TestBuildKey_HostNormalization(t *testing.T) {
	t.Parallel()
	// Same host, different casing → same key.
	r1 := httptest.NewRequest("GET", "http://Example.COM/a", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/a", nil)
	require.Equal(t, BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), nil), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), nil))

	// Non-default port produces different key.
	r3 := httptest.NewRequest("GET", "http://example.com:8080/a", nil)
	require.NotEqual(t, BuildKey(requestInfoFromHTTP(r3.Method, r3.URL.String(), r3.Host, r3.URL.Path, r3.TLS != nil, header.FromHTTP(r3.Header)), nil), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), nil))
}

func TestBuildKey_LongURLNoPanic(t *testing.T) {
	t.Parallel()
	// Regression: URLs whose canonical key exceeds 512 bytes must not
	// panic with "index out of range [512]". This was a production crash
	// in a staging deployment (see key.go:67).
	longPath := strings.Repeat("a", 600)
	r := httptest.NewRequest("GET", "http://example.com/"+longPath+"?b=2&a=1", nil)
	// Must not panic.
	k := BuildKey(requestInfoFromHTTP(r.Method, r.URL.String(), r.Host, r.URL.Path, r.TLS != nil, header.FromHTTP(r.Header)), nil)
	require.NotEqual(t, 0, k)
}

func TestBuildKey_VaryKeyLongNoPanic(t *testing.T) {
	t.Parallel()
	// Regression: BuildVaryKey must not panic when Vary header values
	// exceed the 256-byte stack buffer.
	longVal := strings.Repeat("x", 300)
	reqHeader := http.Header{
		header.AcceptLanguage: []string{longVal},
		header.AcceptEncoding: []string{longVal},
	}
	// Must not panic.
	_ = BuildVaryKey("Accept-Language, Accept-Encoding", header.FromHTTP(reqHeader), nil)
}

func TestBuildVaryKey_ExcludeHeader(t *testing.T) {
	t.Parallel()
	excludePolicy := NewKeyPolicy(nil, nil, map[string]bool{"x-request-id": true}, nil, false, false)
	h1 := http.Header{header.AcceptEncoding: []string{"gzip"}, "X-Request-Id": {"abc"}}
	h2 := http.Header{header.AcceptEncoding: []string{"gzip"}, "X-Request-Id": {"xyz"}}
	k1 := BuildVaryKey("Accept-Encoding, X-Request-Id", header.FromHTTP(h1), excludePolicy)
	k2 := BuildVaryKey("Accept-Encoding, X-Request-Id", header.FromHTTP(h2), excludePolicy)
	require.Equal(t, k2, k1)
	// Without exclusion, keys should differ.
	k3 := BuildVaryKey("Accept-Encoding, X-Request-Id", header.FromHTTP(h1), nil)
	k4 := BuildVaryKey("Accept-Encoding, X-Request-Id", header.FromHTTP(h2), nil)
	require.NotEqual(t, k4, k3)
}

func TestBuildVaryKey_ExcludeAllHeaders(t *testing.T) {
	t.Parallel()
	excludePolicy := NewKeyPolicy(nil, nil, map[string]bool{"x-request-id": true}, nil, false, false)
	h1 := http.Header{"X-Request-Id": {"abc"}}
	h2 := http.Header{"X-Request-Id": {"xyz"}}
	k1 := BuildVaryKey("X-Request-Id", header.FromHTTP(h1), excludePolicy)
	k2 := BuildVaryKey("X-Request-Id", header.FromHTTP(h2), excludePolicy)
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
	resp := http.Header{header.CacheControl: []string{"max-age=60"}}
	require.True(t, IsCacheable(200, header.FromHTTP(http.Header{}), header.FromHTTP(resp)))
}

func TestIsCacheable_NoStore(t *testing.T) {
	t.Parallel()
	resp := http.Header{header.CacheControl: []string{"no-store"}}
	require.False(t, IsCacheable(200, header.FromHTTP(http.Header{}), header.FromHTTP(resp)))
}

func TestIsCacheable_Private(t *testing.T) {
	t.Parallel()
	resp := http.Header{header.CacheControl: []string{"private, max-age=60"}}
	require.False(t, IsCacheable(200, header.FromHTTP(http.Header{}), header.FromHTTP(resp)))
}

func TestIsCacheable_SetCookie(t *testing.T) {
	t.Parallel()
	// Set-Cookie WITHOUT explicit freshness blocks caching.
	resp := http.Header{
		header.SetCookie: []string{"sid=abc"},
	}
	require.False(t, IsCacheable(200, header.FromHTTP(http.Header{}), header.FromHTTP(resp)))
	// Set-Cookie WITH explicit max-age is cacheable (shared cache behavior).
	resp2 := http.Header{
		header.CacheControl: []string{"max-age=60"},
		header.SetCookie:    []string{"sid=abc"},
	}
	require.True(t, IsCacheable(200, header.FromHTTP(http.Header{}), header.FromHTTP(resp2)))
}

func TestIsCacheable_Authorization(t *testing.T) {
	t.Parallel()
	req := http.Header{header.Authorization: []string{"Bearer tok"}}
	resp := http.Header{header.CacheControl: []string{"max-age=60"}}
	require.False(t, IsCacheable(200, header.FromHTTP(req), header.FromHTTP(resp)))

	resp2 := http.Header{header.CacheControl: []string{"max-age=60, public"}}
	require.True(t, IsCacheable(200, header.FromHTTP(req), header.FromHTTP(resp2)))
}

func TestIsCacheable_HeuristicStatus(t *testing.T) {
	t.Parallel()
	// 301 with Last-Modified is heuristically cacheable.
	resp := http.Header{header.LastModified: []string{"Mon, 01 Jan 2024 00:00:00 GMT"}}
	require.True(t, IsCacheable(301, header.FromHTTP(http.Header{}), header.FromHTTP(resp)))
	// 301 without Last-Modified is NOT heuristically cacheable.
	require.False(t, IsCacheable(301, header.FromHTTP(http.Header{}), header.FromHTTP(http.Header{})))
	// 302 is never heuristically cacheable.
	require.False(t, IsCacheable(302, header.FromHTTP(http.Header{}), header.FromHTTP(http.Header{header.LastModified: []string{"Mon, 01 Jan 2024 00:00:00 GMT"}})))
}

func TestBuildKeyFromURL_Empty(t *testing.T) {
	t.Parallel()
	assert.Equal(t, api.Key{}, BuildKeyFromURL("", nil))
}

func TestBuildKeyFromURL_Invalid(t *testing.T) {
	t.Parallel()
	// url.Parse rejects control characters.
	assert.Equal(t, api.Key{}, BuildKeyFromURL("ht\x00tp://invalid", nil))
}

func TestBuildKeyFromURL_Valid(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "http://example.com/foo?a=1&b=2", nil)
	expected := BuildKey(requestInfoFromHTTP(r.Method, r.URL.String(), r.Host, r.URL.Path, r.TLS != nil, header.FromHTTP(r.Header)), nil)
	assert.Equal(t, expected, BuildKeyFromURL("http://example.com/foo?a=1&b=2", nil))
}

func TestBuildKey_HTTPS_DefaultPortStripped(t *testing.T) {
	t.Parallel()
	r1 := httptest.NewRequest("GET", "https://example.com/", nil)
	r2 := httptest.NewRequest("GET", "https://example.com:443/", nil)
	assert.Equal(t, BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), nil), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), nil))
}

func TestBuildKey_NormaliseListHeader_NoComma(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "GZIP", normaliseListHeader("GZIP"))
	assert.Equal(t, "", normaliseListHeader(""))
}

func TestBuildKey_PolicySlowPath(t *testing.T) {
	t.Parallel()
	// Exercise the policy slow path (appendCanonicalQuerySlow) with keepParams.
	policy := NewKeyPolicy(nil, map[string]bool{"q": true}, nil, nil, false, false)
	r1 := httptest.NewRequest("GET", "http://example.com/search?q=test&utm=x", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/search?q=test", nil)
	assert.Equal(t, BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), policy), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), policy))
}

func TestBuildKey_PolicySlowPath_StripParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(map[string]bool{"utm": true}, nil, nil, nil, false, false)
	r1 := httptest.NewRequest("GET", "http://example.com/?a=1&utm=x", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?a=1", nil)
	assert.Equal(t, BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), policy), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), policy))
}

func TestBuildKey_PolicySlowPath_StripEmpty(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, true, false)
	r1 := httptest.NewRequest("GET", "http://example.com/?a=1&empty=", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?a=1", nil)
	assert.Equal(t, BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), policy), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), policy))
}

func TestBuildKey_PolicySlowPath_Dedup(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, false, true)
	r1 := httptest.NewRequest("GET", "http://example.com/?a=2&a=1", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?a=2", nil)
	assert.Equal(t, BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), policy), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), policy))
}

func TestBuildKey_PercentEncodedNoPolicy(t *testing.T) {
	t.Parallel()
	// Percent-encoded params trigger the slow path.
	r1 := httptest.NewRequest("GET", "http://example.com/?a=%31&b=2", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?b=2&a=1", nil)
	assert.Equal(t, BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), nil), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), nil))
}

func TestBuildKey_MoreThan8Params(t *testing.T) {
	t.Parallel()
	// >8 params triggers the slow path.
	r1 := httptest.NewRequest("GET", "http://example.com/?a=1&b=2&c=3&d=4&e=5&f=6&g=7&h=8&i=9", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?i=9&a=1&b=2&c=3&d=4&e=5&f=6&g=7&h=8", nil)
	assert.Equal(t, BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), nil), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), nil))
	require.NotEqual(t, api.Key{}, BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), nil))
}

func TestAppendCanonicalQuerySlow_KeepParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, map[string]bool{"q": true}, nil, nil, false, false)
	// Use percent-encoded params to force the slow path.
	r1 := httptest.NewRequest("GET", "http://example.com/?q=%74est&utm=x", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?q=test", nil)
	assert.Equal(t, BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), policy), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), policy))
}

func TestAppendCanonicalQuerySlow_StripParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(map[string]bool{"utm": true}, nil, nil, nil, false, false)
	r1 := httptest.NewRequest("GET", "http://example.com/?a=%31&utm=x", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?a=1", nil)
	assert.Equal(t, BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), policy), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), policy))
}

func TestAppendCanonicalQuerySlow_StripPrefixes(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, []string{"utm_"}, false, false)
	r1 := httptest.NewRequest("GET", "http://example.com/?a=%31&utm_source=x", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?a=1", nil)
	assert.Equal(t, BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), policy), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), policy))
}

func TestAppendCanonicalQuerySlow_StripEmpty(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, true, false)
	r1 := httptest.NewRequest("GET", "http://example.com/?a=%31&empty=", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?a=1", nil)
	assert.Equal(t, BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), policy), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), policy))
}

func TestAppendCanonicalQuerySlow_Dedup(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, false, true)
	r1 := httptest.NewRequest("GET", "http://example.com/?a=%32&a=%31", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?a=2", nil)
	assert.Equal(t, BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), policy), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), policy))
}

func TestAppendCanonicalQuerySlow_AllFeatures(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(
		map[string]bool{"q": true},
		nil, nil, nil, true, true,
	)
	r1 := httptest.NewRequest("GET", "http://example.com/?q=%74est&q=dup&empty=", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/?q=test", nil)
	assert.Equal(t, BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), policy), BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), policy))
}

func TestBuildKeyFromRaw_SchemeDefault(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{
		Method: "GET",
		Path:   "/",
		Host:   "example.com",
		Scheme: "", // empty scheme should default to "http"
	}
	key := buildKeyFromRaw(req, nil)
	require.NotEqual(t, api.Key{}, key)
}

func TestBuildKeyFromRaw_TLS(t *testing.T) {
	t.Parallel()
	req1 := &api.RawRequest{Method: "GET", Path: "/", Host: "example.com", Scheme: "http"}
	req2 := &api.RawRequest{Method: "GET", Path: "/", Host: "example.com", Scheme: "https"}
	assert.NotEqual(t, buildKeyFromRaw(req2, nil), buildKeyFromRaw(req1, nil))
}
