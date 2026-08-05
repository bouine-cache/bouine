package cache

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildKey_KeepQueryParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, map[string]bool{"q": true, "page": true}, nil, nil, false, false)

	r1 := httptest.NewRequest("GET", "http://example.com/search?q=test&page=1&utm_source=email&fbclid=xyz", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/search?q=test&page=1", nil)

	k1 := BuildKey(r1, policy)
	k2 := BuildKey(r2, policy)

	assert.Equal(t, k2, k1)
}

func TestBuildKey_KeepQueryParams_EmptyValue(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, map[string]bool{"q": true}, nil, nil, true, false)

	r1 := httptest.NewRequest("GET", "http://example.com/search?q=&other=1", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/search?q=", nil)

	k1 := BuildKey(r1, policy)
	k2 := BuildKey(r2, policy)

	assert.Equal(t, k2, k1)
}

func TestBuildKey_StripQueryPrefix(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, []string{"utm_"}, false, false)

	r1 := httptest.NewRequest("GET", "http://example.com/page?a=1&utm_source=email&utm_medium=social&utm_campaign=launch", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/page?a=1", nil)

	k1 := BuildKey(r1, policy)
	k2 := BuildKey(r2, policy)

	assert.Equal(t, k2, k1)
}

func TestBuildKey_StripEmptyParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, true, false)

	r1 := httptest.NewRequest("GET", "http://example.com/page?foo=&bar=1", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/page?bar=1", nil)

	k1 := BuildKey(r1, policy)
	k2 := BuildKey(r2, policy)

	assert.Equal(t, k2, k1)
}

func TestBuildKey_DedupQueryParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, false, true)

	r1 := httptest.NewRequest("GET", "http://example.com/page?a=2&a=1", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/page?a=2", nil)

	k1 := BuildKey(r1, policy)
	k2 := BuildKey(r2, policy)

	assert.Equal(t, k2, k1)
}

func TestBuildKey_Dedup_WithoutDedup(t *testing.T) {
	t.Parallel()

	r1 := httptest.NewRequest("GET", "http://example.com/page?a=2&a=1", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/page?a=1&a=2", nil)

	k1 := BuildKey(r1, nil)
	k2 := BuildKey(r2, nil)

	assert.Equal(t, k2, k1)
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

	assert.Equal(t, k2, k1)
}

func TestBuildKey_NoPolicyParity(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest("GET", "http://example.com/page?a=1&b=2&c=3", nil)
	k := BuildKey(r, nil)

	require.NotEqual(t, 0, k)

	r2 := httptest.NewRequest("GET", "http://example.com/page?a=1&b=2&c=3", nil)
	k2 := BuildKey(r2, nil)

	assert.Equal(t, k2, k)
}

func TestBuildKey_FastSlowPathParity(t *testing.T) {
	t.Parallel()

	r1 := httptest.NewRequest("GET", "http://example.com/page?a=1&b=2&a=3", nil)

	k1 := BuildKey(r1, nil)

	r2 := httptest.NewRequest("GET", "http://example.com/page?a=1&b=2&a=3", nil)
	k2 := BuildKey(r2, nil)

	assert.Equal(t, k2, k1)
}
