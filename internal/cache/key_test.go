package cache

import (
	"net/url"
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

// hostAgnostic is the policy produced by cache.key.include_host: false.
// Every key builder must agree on it, so the parity test below drives
// BuildKey, BuildKeyFast, and buildKeyFromRaw through the same URL.
func hostAgnostic() *KeyPolicy {
	return NewKeyPolicy(nil, nil, nil, nil, false, false, nil, true)
}

func TestBuildKey_ExcludeHost(t *testing.T) {
	t.Parallel()
	// Different hosts, same URL → one key under include_host: false.
	require.Equal(t,
		BuildKey(requestInfoFromURL("GET", "http://a.example.com/x?q=1"), hostAgnostic()),
		BuildKey(requestInfoFromURL("GET", "http://b.example.com/x?q=1"), hostAgnostic()))

	// The default (nil policy and includeHostKey(nil)) still keys host.
	require.NotEqual(t,
		BuildKey(requestInfoFromURL("GET", "http://a.example.com/x"), nil),
		BuildKey(requestInfoFromURL("GET", "http://b.example.com/x"), nil))
	require.True(t, includeHostKey(nil))
	require.False(t, includeHostKey(hostAgnostic()))

	// Scheme and method stay keyed: dropping host must not drop them.
	require.NotEqual(t,
		BuildKey(requestInfoFromURL("GET", "http://a.example.com/x"), hostAgnostic()),
		BuildKey(requestInfoFromURL("GET", "https://a.example.com/x"), hostAgnostic()))
	require.NotEqual(t,
		BuildKey(requestInfoFromURL("POST", "http://a.example.com/x"), hostAgnostic()),
		BuildKey(requestInfoFromURL("GET", "http://a.example.com/x"), hostAgnostic()))
}

func TestBuildKey_ExcludeHost_OverflowParity(t *testing.T) {
	t.Parallel()
	// The >512-byte heap path must gate host exactly like the stack
	// path: same URL under two hosts, one key.
	long := strings.Repeat("a", 600)
	k1 := BuildKey(requestInfoFromURL("GET", "http://a.example.com/"+long+"?b=2&a=1"), hostAgnostic())
	k2 := BuildKey(requestInfoFromURL("GET", "http://b.example.com/"+long+"?b=2&a=1"), hostAgnostic())
	require.Equal(t, k1, k2)
	require.NotEqual(t, api.Key{}, k1)
}

// TestExcludeHost_KeyBuilderParity pins the three primary-key builders
// against each other under include_host: false. They are separate
// implementations kept in lockstep (stack and heap variants inside
// each); a divergence routes the same URL to different stored objects
// on the slow path vs the H1 fast path, which is a wrong-body hazard,
// not a performance one.
func TestExcludeHost_KeyBuilderParity(t *testing.T) {
	t.Parallel()
	pol := hostAgnostic()
	for _, tc := range []struct{ method, url, altHost string }{
		{"GET", "http://example.com/", "other.example.com"},
		{"GET", "http://example.com/p/q?b=2&a=1", "example.io"},
		{"HEAD", "http://example.com:8080/long/path", "example.org"},
		{"GET", "https://example.com/x", "ssl.example.com"},
	} {
		ri := requestInfoFromURL(tc.method, tc.url)
		alt := requestInfoFromURL(tc.method, tc.url)
		alt.Host = tc.altHost

		kBuild := BuildKey(ri, pol)
		kBuildAlt := BuildKey(alt, pol)
		require.Equal(t, kBuild, kBuildAlt, "BuildKey must collapse %s vs %s", ri.Host, tc.altHost)

		kFast := BuildKeyFast([]byte(tc.method), []byte(tc.url), []byte(ri.Host), []byte(ri.Path), ri.TLS, pol)
		kFastAlt := BuildKeyFast([]byte(tc.method), []byte(tc.url), []byte(alt.Host), []byte(alt.Path), alt.TLS, pol)
		require.Equal(t, kFast, kFastAlt, "BuildKeyFast must collapse hosts")

		u, err := url.Parse(tc.url)
		require.NoError(t, err)
		q := ""
		if i := strings.IndexByte(tc.url, '?'); i >= 0 {
			q = tc.url[i+1:]
		}
		raw := &api.RawRequest{Method: tc.method, Host: ri.Host, Path: u.Path, Query: q, Scheme: u.Scheme}
		rawAlt := &api.RawRequest{Method: tc.method, Host: tc.altHost, Path: u.Path, Query: q, Scheme: u.Scheme}
		kRaw := buildKeyFromRaw(raw, pol)
		kRawAlt := buildKeyFromRaw(rawAlt, pol)
		require.Equal(t, kRaw, kRawAlt, "buildKeyFromRaw must collapse hosts")

		// Cross-builder: the same (host, path, query) must produce the
		// same key in every builder.
		require.Equal(t, kBuild, kFast, "BuildKey vs BuildKeyFast for %s", tc.url)
		require.Equal(t, kBuild, kRaw, "BuildKey vs buildKeyFromRaw for %s", tc.url)
	}
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
	excludePolicy := NewKeyPolicy(nil, nil, map[string]bool{"x-request-id": true}, nil, false, false, nil, false)
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
	excludePolicy := NewKeyPolicy(nil, nil, map[string]bool{"x-request-id": true}, nil, false, false, nil, false)
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

func TestParseCacheControlBytes_ParityWithString(t *testing.T) {
	t.Parallel()
	cases := []string{
		"max-age=300, public, stale-while-revalidate=60",
		"no-store",
		"max-stale",
		"no-cache, no-transform, immutable",
		"max-age=3600, s-maxage=600, stale-if-error=86400",
		`no-cache="Accept-Encoding"`,
		"only-if-cached",
		"",
	}
	for _, cc := range cases {
		want := ParseCacheControl(cc)
		got := ParseCacheControlBytes([]byte(cc))
		assert.Equal(t, want, got, "mismatch for %q", cc)
	}
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
	require.True(t, IsCacheable(200, header.NewMap(0), resp, nil))
}

func TestIsCacheable_NoStore(t *testing.T) {
	t.Parallel()
	resp := headerMap(header.CacheControl, "no-store")
	require.False(t, IsCacheable(200, header.NewMap(0), resp, nil))
}

func TestIsCacheable_Private(t *testing.T) {
	t.Parallel()
	resp := headerMap(header.CacheControl, "private, max-age=60")
	require.False(t, IsCacheable(200, header.NewMap(0), resp, nil))
}

func TestIsCacheable_SetCookie(t *testing.T) {
	t.Parallel()
	// Set-Cookie WITHOUT explicit freshness blocks caching.
	resp := headerMap(header.SetCookie, "sid=abc")
	require.False(t, IsCacheable(200, header.NewMap(0), resp, nil))
	// Set-Cookie WITH explicit max-age is cacheable (shared cache behavior).
	resp2 := headerMap(header.CacheControl, "max-age=60", header.SetCookie, "sid=abc")
	require.True(t, IsCacheable(200, header.NewMap(0), resp2, nil))
}

func TestIsCacheable_Authorization(t *testing.T) {
	t.Parallel()
	req := headerMap(header.Authorization, "Bearer tok")
	resp := headerMap(header.CacheControl, "max-age=60")
	require.False(t, IsCacheable(200, req, resp, nil))

	resp2 := headerMap(header.CacheControl, "max-age=60, public")
	require.True(t, IsCacheable(200, req, resp2, nil))
}

func TestIsCacheable_HeuristicStatus(t *testing.T) {
	t.Parallel()
	// 301 with Last-Modified is heuristically cacheable.
	resp := headerMap(header.LastModified, "Mon, 01 Jan 2024 00:00:00 GMT")
	require.True(t, IsCacheable(301, header.NewMap(0), resp, nil))
	// 301 without Last-Modified is NOT heuristically cacheable.
	require.False(t, IsCacheable(301, header.NewMap(0), header.NewMap(0), nil))
	// 302 is never heuristically cacheable.
	require.False(t, IsCacheable(302, header.NewMap(0), headerMap(header.LastModified, "Mon, 01 Jan 2024 00:00:00 GMT"), nil))
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
	policy := NewKeyPolicy(nil, map[string]bool{"q": true}, nil, nil, false, false, nil, false)
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/search?q=test"), policy), BuildKey(requestInfoFromURL("GET", "http://example.com/search?q=test&utm=x"), policy))
}

func TestBuildKey_PolicySlowPath_StripParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(map[string]bool{"utm": true}, nil, nil, nil, false, false, nil, false)
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/?a=1"), policy), BuildKey(requestInfoFromURL("GET", "http://example.com/?a=1&utm=x"), policy))
}

func TestBuildKey_PolicySlowPath_StripEmpty(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, true, false, nil, false)
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/?a=1"), policy), BuildKey(requestInfoFromURL("GET", "http://example.com/?a=1&empty="), policy))
}

func TestBuildKey_PolicySlowPath_Dedup(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, false, true, nil, false)
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
	policy := NewKeyPolicy(nil, map[string]bool{"q": true}, nil, nil, false, false, nil, false)
	// Use percent-encoded params to force the slow path.
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/?q=test"), policy), BuildKey(requestInfoFromURL("GET", "http://example.com/?q=%74est&utm=x"), policy))
}

func TestAppendCanonicalQuerySlow_StripParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(map[string]bool{"utm": true}, nil, nil, nil, false, false, nil, false)
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/?a=1"), policy), BuildKey(requestInfoFromURL("GET", "http://example.com/?a=%31&utm=x"), policy))
}

func TestAppendCanonicalQuerySlow_StripPrefixes(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, []string{"utm_"}, false, false, nil, false)
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/?a=1"), policy), BuildKey(requestInfoFromURL("GET", "http://example.com/?a=%31&utm_source=x"), policy))
}

func TestAppendCanonicalQuerySlow_StripEmpty(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, true, false, nil, false)
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/?a=1"), policy), BuildKey(requestInfoFromURL("GET", "http://example.com/?a=%31&empty="), policy))
}

func TestAppendCanonicalQuerySlow_Dedup(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, false, true, nil, false)
	assert.Equal(t, BuildKey(requestInfoFromURL("GET", "http://example.com/?a=2"), policy), BuildKey(requestInfoFromURL("GET", "http://example.com/?a=%32&a=%31"), policy))
}

func TestAppendCanonicalQuerySlow_AllFeatures(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(
		map[string]bool{"q": true},
		nil, nil, nil, true, true,
		nil,
		false,
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

// FuzzBuildKeyExcludeHost fuzzes the host-agnostic key policy: for any
// URL, the key built with include_host: false must (a) never panic,
// (b) be identical across arbitrary host substitutions — host never
// enters the key — and (c) differ from the default host-ful key for
// the same URL, so the empty segment cannot silently collide with the
// legacy key space. All three builders must agree on (a) and (b).
func FuzzBuildKeyExcludeHost(f *testing.F) {
	f.Add("GET", "http://example.com/", "other.example.com")
	f.Add("GET", "http://a.b.c:8080/p?q=1&r=%20z", "x.y.z")
	f.Add("HEAD", "https://example.com/a//b/./c", "EXAMPLE.COM")
	f.Add("GET", "http://[::1]/x", "[::2]")
	f.Add("GET", "http://h/long?"+strings.Repeat("p=1&", 30)+"q=2", "h2")

	f.Fuzz(func(t *testing.T, method, rawURL, altHost string) {
		ri := requestInfoFromURL(method, rawURL)
		if ri.Host == "" {
			t.Skip() // unparseable URL — no host dimension to test
		}
		alt := requestInfoFromURL(method, rawURL)
		alt.Host = altHost

		pol := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, true)
		k1 := BuildKey(ri, pol)
		k2 := BuildKey(alt, pol)
		if k1 != k2 {
			t.Fatalf("host leaked into key for %q: %s vs %s", rawURL, k1, k2)
		}
		if k1 == BuildKey(ri, nil) && ri.Host != altHost {
			t.Fatalf("host-agnostic key collides with host-ful key for %q", rawURL)
		}
	})
}
