package cache

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBuildKey_Deterministic(t *testing.T) {
	t.Parallel()
	r1 := httptest.NewRequest("GET", "http://example.com/foo?b=2&a=1", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/foo?a=1&b=2", nil)
	if BuildKey(r1) != BuildKey(r2) {
		t.Fatal("query order should not affect key")
	}
}

func TestBuildKey_HeadSharesGet(t *testing.T) {
	t.Parallel()
	r1 := httptest.NewRequest("GET", "http://example.com/x", nil)
	r2 := httptest.NewRequest("HEAD", "http://example.com/x", nil)
	if BuildKey(r1) != BuildKey(r2) {
		t.Fatal("HEAD and GET should share key space")
	}
}

func TestBuildKey_DifferentPaths(t *testing.T) {
	t.Parallel()
	r1 := httptest.NewRequest("GET", "http://example.com/a", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/b", nil)
	if BuildKey(r1) == BuildKey(r2) {
		t.Fatal("different paths should produce different keys")
	}
}

func TestBuildKey_SchemeMatters(t *testing.T) {
	t.Parallel()
	r1 := httptest.NewRequest("GET", "http://example.com/", nil)
	r2 := httptest.NewRequest("GET", "https://example.com/", nil)
	if BuildKey(r1) == BuildKey(r2) {
		t.Fatal("different schemes should produce different keys")
	}
}

func TestBuildKey_DefaultPortStripped(t *testing.T) {
	t.Parallel()
	r1 := httptest.NewRequest("GET", "http://example.com/", nil)
	r2 := httptest.NewRequest("GET", "http://example.com:80/", nil)
	if BuildKey(r1) != BuildKey(r2) {
		t.Fatal("default port should be stripped")
	}
}

func TestBuildKey_DuplicateSlashes(t *testing.T) {
	t.Parallel()
	r1 := httptest.NewRequest("GET", "http://example.com/a/b", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/a//b", nil)
	if BuildKey(r1) != BuildKey(r2) {
		t.Fatal("duplicate slashes should be collapsed")
	}
}

func TestBuildKey_HostNormalization(t *testing.T) {
	t.Parallel()
	// Same host, different casing → same key.
	r1 := httptest.NewRequest("GET", "http://Example.COM/a", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/a", nil)
	if BuildKey(r1) != BuildKey(r2) {
		t.Fatal("host casing should not affect key")
	}

	// Non-default port produces different key.
	r3 := httptest.NewRequest("GET", "http://example.com:8080/a", nil)
	if BuildKey(r1) == BuildKey(r3) {
		t.Fatal("non-default port should produce different key")
	}
}

func TestBuildKey_LongURLNoPanic(t *testing.T) {
	t.Parallel()
	// Regression: URLs whose canonical key exceeds 512 bytes must not
	// panic with "index out of range [512]". This was a production crash
	// in preprod-eu (see key.go:67).
	longPath := strings.Repeat("a", 600)
	r := httptest.NewRequest("GET", "http://example.com/"+longPath+"?b=2&a=1", nil)
	// Must not panic.
	k := BuildKey(r)
	if k == 0 {
		t.Fatal("expected non-zero key for long URL")
	}
}

func TestBuildKey_VaryKeyLongNoPanic(t *testing.T) {
	t.Parallel()
	// Regression: BuildVaryKey must not panic when Vary header values
	// exceed the 256-byte stack buffer.
	longVal := strings.Repeat("x", 300)
	reqHeader := http.Header{
		"Accept-Language": {longVal},
		"Accept-Encoding": {longVal},
	}
	// Must not panic.
	_ = BuildVaryKey("Accept-Language, Accept-Encoding", reqHeader)
}

func TestParseCacheControl(t *testing.T) {
	t.Parallel()
	d := ParseCacheControl("max-age=300, public, stale-while-revalidate=60")
	if !d.MaxAgeSet || d.MaxAge.Seconds() != 300 {
		t.Errorf("max-age = %v", d.MaxAge)
	}
	if !d.Public {
		t.Error("expected public")
	}
	if !d.StaleWhileRevalidSet || d.StaleWhileRevalid.Seconds() != 60 {
		t.Errorf("swr = %v", d.StaleWhileRevalid)
	}
}

func TestParseCacheControl_NoStore(t *testing.T) {
	t.Parallel()
	d := ParseCacheControl("no-store")
	if !d.NoStore {
		t.Error("expected no-store")
	}
}

func TestParseCacheControl_MaxStaleNoValue(t *testing.T) {
	t.Parallel()
	d := ParseCacheControl("max-stale")
	if !d.MaxStaleSet {
		t.Error("expected max-stale set")
	}
	if d.MaxStale <= 0 {
		t.Error("max-stale without value should be infinite")
	}
}

func TestIsCacheable_BasicPositive(t *testing.T) {
	t.Parallel()
	resp := http.Header{"Cache-Control": {"max-age=60"}}
	if !IsCacheable(200, http.Header{}, resp) {
		t.Fatal("200 with max-age should be cacheable")
	}
}

func TestIsCacheable_NoStore(t *testing.T) {
	t.Parallel()
	resp := http.Header{"Cache-Control": {"no-store"}}
	if IsCacheable(200, http.Header{}, resp) {
		t.Fatal("no-store should not be cacheable")
	}
}

func TestIsCacheable_Private(t *testing.T) {
	t.Parallel()
	resp := http.Header{"Cache-Control": {"private, max-age=60"}}
	if IsCacheable(200, http.Header{}, resp) {
		t.Fatal("private should not be cacheable by shared cache")
	}
}

func TestIsCacheable_SetCookie(t *testing.T) {
	t.Parallel()
	// Set-Cookie WITHOUT explicit freshness blocks caching.
	resp := http.Header{
		"Set-Cookie": {"sid=abc"},
	}
	if IsCacheable(200, http.Header{}, resp) {
		t.Fatal("Set-Cookie without max-age should block caching")
	}
	// Set-Cookie WITH explicit max-age is cacheable (shared cache behavior).
	resp2 := http.Header{
		"Cache-Control": {"max-age=60"},
		"Set-Cookie":    {"sid=abc"},
	}
	if !IsCacheable(200, http.Header{}, resp2) {
		t.Fatal("Set-Cookie with max-age should be cacheable")
	}
}

func TestIsCacheable_Authorization(t *testing.T) {
	t.Parallel()
	req := http.Header{"Authorization": {"Bearer tok"}}
	resp := http.Header{"Cache-Control": {"max-age=60"}}
	if IsCacheable(200, req, resp) {
		t.Fatal("Authorization without public/must-revalidate should block")
	}

	resp2 := http.Header{"Cache-Control": {"max-age=60, public"}}
	if !IsCacheable(200, req, resp2) {
		t.Fatal("Authorization + public should be cacheable")
	}
}

func TestIsCacheable_HeuristicStatus(t *testing.T) {
	t.Parallel()
	// 301 with Last-Modified is heuristically cacheable.
	resp := http.Header{"Last-Modified": {"Mon, 01 Jan 2024 00:00:00 GMT"}}
	if !IsCacheable(301, http.Header{}, resp) {
		t.Fatal("301 with Last-Modified should be heuristically cacheable")
	}
	// 301 without Last-Modified is NOT heuristically cacheable.
	if IsCacheable(301, http.Header{}, http.Header{}) {
		t.Fatal("301 without Last-Modified should NOT be heuristically cacheable")
	}
	// 302 is never heuristically cacheable.
	if IsCacheable(302, http.Header{}, http.Header{"Last-Modified": {"Mon, 01 Jan 2024 00:00:00 GMT"}}) {
		t.Fatal("302 should NOT be heuristically cacheable")
	}
}
