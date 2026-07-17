package cache

import (
	"net/http/httptest"
	"testing"
)

func TestBuildKey_KeepQueryParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, map[string]bool{"q": true, "page": true}, nil, nil, false, false)

	r1 := httptest.NewRequest("GET", "http://example.com/search?q=test&page=1&utm_source=email&fbclid=xyz", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/search?q=test&page=1", nil)

	k1 := BuildKey(r1, policy)
	k2 := BuildKey(r2, policy)

	if k1 != k2 {
		t.Errorf("keys should match with keep_query_params: got %v vs %v", k1, k2)
	}
}

func TestBuildKey_KeepQueryParams_EmptyValue(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, map[string]bool{"q": true}, nil, nil, true, false)

	r1 := httptest.NewRequest("GET", "http://example.com/search?q=&other=1", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/search?q=", nil)

	k1 := BuildKey(r1, policy)
	k2 := BuildKey(r2, policy)

	if k1 != k2 {
		t.Errorf("allowlisted param with empty value should be kept: got %v vs %v", k1, k2)
	}
}

func TestBuildKey_StripQueryPrefix(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, []string{"utm_"}, false, false)

	r1 := httptest.NewRequest("GET", "http://example.com/page?a=1&utm_source=email&utm_medium=social&utm_campaign=launch", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/page?a=1", nil)

	k1 := BuildKey(r1, policy)
	k2 := BuildKey(r2, policy)

	if k1 != k2 {
		t.Errorf("keys should match with strip_query_prefix: got %v vs %v", k1, k2)
	}
}

func TestBuildKey_StripEmptyParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, true, false)

	r1 := httptest.NewRequest("GET", "http://example.com/page?foo=&bar=1", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/page?bar=1", nil)

	k1 := BuildKey(r1, policy)
	k2 := BuildKey(r2, policy)

	if k1 != k2 {
		t.Errorf("keys should match with strip_empty_params: got %v vs %v", k1, k2)
	}
}

func TestBuildKey_DedupQueryParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, false, true)

	r1 := httptest.NewRequest("GET", "http://example.com/page?a=2&a=1", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/page?a=2", nil)

	k1 := BuildKey(r1, policy)
	k2 := BuildKey(r2, policy)

	if k1 != k2 {
		t.Errorf("keys should match with dedup (first in request order): got %v vs %v", k1, k2)
	}
}

func TestBuildKey_Dedup_WithoutDedup(t *testing.T) {
	t.Parallel()

	r1 := httptest.NewRequest("GET", "http://example.com/page?a=2&a=1", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/page?a=1&a=2", nil)

	k1 := BuildKey(r1, nil)
	k2 := BuildKey(r2, nil)

	if k1 != k2 {
		t.Errorf("without dedup, sorted multi-value params should produce same key: got %v vs %v", k1, k2)
	}
}

func TestBuildKey_AllFeaturesCombined(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(
		nil,
		map[string]bool{"q": true, "page": true},
		nil,
		nil,
		true, true,
	)

	r1 := httptest.NewRequest("GET", "http://example.com/search?q=test&page=1&q=duplicate&tracker=x&empty=", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/search?q=test&page=1", nil)

	k1 := BuildKey(r1, policy)
	k2 := BuildKey(r2, policy)

	if k1 != k2 {
		t.Errorf("keys should match with all features combined: got %v vs %v", k1, k2)
	}
}

func TestBuildKey_NoPolicyParity(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest("GET", "http://example.com/page?a=1&b=2&c=3", nil)
	k := BuildKey(r, nil)

	if k == 0 {
		t.Fatal("key should not be zero")
	}

	r2 := httptest.NewRequest("GET", "http://example.com/page?a=1&b=2&c=3", nil)
	k2 := BuildKey(r2, nil)

	if k != k2 {
		t.Errorf("same URL should produce same key with nil policy: got %v vs %v", k, k2)
	}
}

func TestBuildKey_FastSlowPathParity(t *testing.T) {
	t.Parallel()

	r1 := httptest.NewRequest("GET", "http://example.com/page?a=1&b=2&a=3", nil)

	k1 := BuildKey(r1, nil)

	r2 := httptest.NewRequest("GET", "http://example.com/page?a=1&b=2&a=3", nil)
	k2 := BuildKey(r2, nil)

	if k1 != k2 {
		t.Errorf("fast and slow paths should produce same key: got %v vs %v", k1, k2)
	}
}
