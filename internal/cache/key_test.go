package cache

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestBuildKey_Deterministic(t *testing.T) {
	t.Parallel()
	require.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/foo?a=1&b=2"), nil), BuildKey(requestInfoFromURL("GET", "http://example.com/foo?b=2&a=1"), nil))
}

func TestBuildKey_HeadSharesGet(t *testing.T) {
	t.Parallel()
	require.Equal(t, BuildKey(requestInfoFromURL("HEAD", "http://example.com/x"), nil), BuildKey(requestInfoFromURL("GET", "http://example.com/x"), nil))
}

func TestBuildKey_DifferentPaths(t *testing.T) {
	t.Parallel()
	require.NotEqual(t, BuildKey(requestInfoFromURL("GET", "http://example.com/b"), nil), BuildKey(requestInfoFromURL("GET", "http://example.com/a"), nil))
}

func TestBuildKey_SchemeMatters(t *testing.T) {
	t.Parallel()
	require.NotEqual(t, BuildKey(requestInfoFromURL("GET", "https://example.com/"), nil), BuildKey(requestInfoFromURL("GET", "http://example.com/"), nil))
}

func TestBuildKey_DefaultPortStripped(t *testing.T) {
	t.Parallel()
	require.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com:80/"), nil), BuildKey(requestInfoFromURL("GET", "http://example.com/"), nil))
}

func TestBuildKey_DuplicateSlashes(t *testing.T) {
	t.Parallel()
	require.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/a//b"), nil), BuildKey(requestInfoFromURL("GET", "http://example.com/a/b"), nil))
}

func TestBuildKey_HostNormalization(t *testing.T) {
	t.Parallel()
	// Same host, different casing → same key.
	require.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/a"), nil), BuildKey(requestInfoFromURL("GET", "http://Example.COM/a"), nil))

	// Non-default port produces different key.
	require.NotEqual(t, BuildKey(requestInfoFromURL("GET", "http://example.com:8080/a"), nil), BuildKey(requestInfoFromURL("GET", "http://Example.COM/a"), nil))
}

func TestBuildKey_LongURLNoPanic(t *testing.T) {
	t.Parallel()
	// Regression: URLs whose canonical key exceeds 512 bytes must not
	// panic with "index out of range [512]". This was a production crash
	// in a staging deployment (see key.go:67).
	longPath := strings.Repeat("a", 600)
	// Must not panic.
	k := BuildKey(requestInfoFromURL("GET", "http://example.com/"+longPath+"?b=2&a=1"), nil)
	require.NotEqual(t, 0, k)
}

func TestBuildKey_VaryKeyLongNoPanic(t *testing.T) {
	t.Parallel()
	// Regression: BuildVaryKey must not panic when Vary header values
	// exceed the 256-byte stack buffer.
	longVal := strings.Repeat("x", 300)
	reqHeader := headerMap(header.AcceptLanguage, longVal, header.AcceptEncoding, longVal)
	// Must not panic.
	_ = BuildVaryKey("Accept-Language, Accept-Encoding", reqHeader, nil)
}

func TestBuildVaryKey_ExcludeHeader(t *testing.T) {
	t.Parallel()
	excludePolicy := NewKeyPolicy(nil, nil, map[string]bool{"x-request-id": true}, nil, false, false)
	h1 := headerMap(header.AcceptEncoding, "gzip", "X-Request-Id", "abc")
	h2 := headerMap(header.AcceptEncoding, "gzip", "X-Request-Id", "xyz")
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
	h1 := headerMap("X-Request-Id", "abc")
	h2 := headerMap("X-Request-Id", "xyz")
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
	resp := headerMap(header.CacheControl, "max-age=60")
	require.True(t, IsCacheable(200, header.NewMap(0), resp))
}

func TestIsCacheable_NoStore(t *testing.T) {
	t.Parallel()
	resp := headerMap(header.CacheControl, "no-store")
	require.False(t, IsCacheable(200, header.NewMap(0), resp))
}

func TestIsCacheable_Private(t *testing.T) {
	t.Parallel()
	resp := headerMap(header.CacheControl, "private, max-age=60")
	require.False(t, IsCacheable(200, header.NewMap(0), resp))
}

func TestIsCacheable_SetCookie(t *testing.T) {
	t.Parallel()
	// Set-Cookie WITHOUT explicit freshness blocks caching.
	resp := headerMap(header.SetCookie, "sid=abc")
	require.False(t, IsCacheable(200, header.NewMap(0), resp))
	// Set-Cookie WITH explicit max-age is cacheable (shared cache behavior).
	resp2 := headerMap(header.CacheControl, "max-age=60", header.SetCookie, "sid=abc")
	require.True(t, IsCacheable(200, header.NewMap(0), resp2))
}

func TestIsCacheable_Authorization(t *testing.T) {
	t.Parallel()
	req := headerMap(header.Authorization, "Bearer tok")
	resp := headerMap(header.CacheControl, "max-age=60")
	require.False(t, IsCacheable(200, req, resp))

	resp2 := headerMap(header.CacheControl, "max-age=60, public")
	require.True(t, IsCacheable(200, req, resp2))
}

func TestIsCacheable_HeuristicStatus(t *testing.T) {
	t.Parallel()
	// 301 with Last-Modified is heuristically cacheable.
	resp := headerMap(header.LastModified, "Mon, 01 Jan 2024 00:00:00 GMT")
	require.True(t, IsCacheable(301, header.NewMap(0), resp))
	// 301 without Last-Modified is NOT heuristically cacheable.
	require.False(t, IsCacheable(301, header.NewMap(0), header.NewMap(0)))
	// 302 is never heuristically cacheable.
	require.False(t, IsCacheable(302, header.NewMap(0), headerMap(header.LastModified, "Mon, 01 Jan 2024 00:00:00 GMT")))
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
	expected := BuildKey(requestInfoFromURL("GET", "http://example.com/foo?a=1&b=2"), nil)
	assert.Equal(t, expected, BuildKeyFromURL("http://example.com/foo?a=1&b=2", nil))
}

func TestBuildKey_HTTPS_DefaultPortStripped(t *testing.T) {
	t.Parallel()
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "https://example.com:443/"), nil), BuildKey(requestInfoFromURL("GET", "https://example.com/"), nil))
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
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/search?q=test"), policy), BuildKey(requestInfoFromURL("GET", "http://example.com/search?q=test&utm=x"), policy))
}

func TestBuildKey_PolicySlowPath_StripParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(map[string]bool{"utm": true}, nil, nil, nil, false, false)
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/?a=1"), policy), BuildKey(requestInfoFromURL("GET", "http://example.com/?a=1&utm=x"), policy))
}

func TestBuildKey_PolicySlowPath_StripEmpty(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, true, false)
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/?a=1"), policy), BuildKey(requestInfoFromURL("GET", "http://example.com/?a=1&empty="), policy))
}

func TestBuildKey_PolicySlowPath_Dedup(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, false, true)
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/?a=2"), policy), BuildKey(requestInfoFromURL("GET", "http://example.com/?a=2&a=1"), policy))
}

func TestBuildKey_PercentEncodedNoPolicy(t *testing.T) {
	t.Parallel()
	// Percent-encoded params trigger the slow path.
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/?b=2&a=1"), nil), BuildKey(requestInfoFromURL("GET", "http://example.com/?a=%31&b=2"), nil))
}

func TestBuildKey_MoreThan8Params(t *testing.T) {
	t.Parallel()
	// >8 params triggers the slow path.
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/?i=9&a=1&b=2&c=3&d=4&e=5&f=6&g=7&h=8"), nil), BuildKey(requestInfoFromURL("GET", "http://example.com/?a=1&b=2&c=3&d=4&e=5&f=6&g=7&h=8&i=9"), nil))
	require.NotEqual(t, api.Key{}, BuildKey(requestInfoFromURL("GET", "http://example.com/?a=1&b=2&c=3&d=4&e=5&f=6&g=7&h=8&i=9"), nil))
}

func TestAppendCanonicalQuerySlow_KeepParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, map[string]bool{"q": true}, nil, nil, false, false)
	// Use percent-encoded params to force the slow path.
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/?q=test"), policy), BuildKey(requestInfoFromURL("GET", "http://example.com/?q=%74est&utm=x"), policy))
}

func TestAppendCanonicalQuerySlow_StripParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(map[string]bool{"utm": true}, nil, nil, nil, false, false)
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/?a=1"), policy), BuildKey(requestInfoFromURL("GET", "http://example.com/?a=%31&utm=x"), policy))
}

func TestAppendCanonicalQuerySlow_StripPrefixes(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, []string{"utm_"}, false, false)
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/?a=1"), policy), BuildKey(requestInfoFromURL("GET", "http://example.com/?a=%31&utm_source=x"), policy))
}

func TestAppendCanonicalQuerySlow_StripEmpty(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, true, false)
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/?a=1"), policy), BuildKey(requestInfoFromURL("GET", "http://example.com/?a=%31&empty="), policy))
}

func TestAppendCanonicalQuerySlow_Dedup(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, false, true)
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/?a=2"), policy), BuildKey(requestInfoFromURL("GET", "http://example.com/?a=%32&a=%31"), policy))
}

func TestAppendCanonicalQuerySlow_AllFeatures(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(
		map[string]bool{"q": true},
		nil, nil, nil, true, true,
	)
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/?q=test"), policy), BuildKey(requestInfoFromURL("GET", "http://example.com/?q=%74est&q=dup&empty="), policy))
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
