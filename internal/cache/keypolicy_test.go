package cache

import (
	"net/http/httptest"
	"testing"

	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildKey_KeepQueryParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, map[string]bool{"q": true, "page": true}, nil, nil, false, false)

	r1 := httptest.NewRequest("GET", "http://example.com/search?q=test&page=1&utm_source=email&fbclid=xyz", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/search?q=test&page=1", nil)

	k1 := BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), policy)
	k2 := BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), policy)

	assert.Equal(t, k2, k1)
}

func TestBuildKey_KeepQueryParams_EmptyValue(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, map[string]bool{"q": true}, nil, nil, true, false)

	r1 := httptest.NewRequest("GET", "http://example.com/search?q=&other=1", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/search?q=", nil)

	k1 := BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), policy)
	k2 := BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), policy)

	assert.Equal(t, k2, k1)
}

func TestBuildKey_StripQueryPrefix(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, []string{"utm_"}, false, false)

	r1 := httptest.NewRequest("GET", "http://example.com/page?a=1&utm_source=email&utm_medium=social&utm_campaign=launch", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/page?a=1", nil)

	k1 := BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), policy)
	k2 := BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), policy)

	assert.Equal(t, k2, k1)
}

func TestBuildKey_StripEmptyParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, true, false)

	r1 := httptest.NewRequest("GET", "http://example.com/page?foo=&bar=1", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/page?bar=1", nil)

	k1 := BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), policy)
	k2 := BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), policy)

	assert.Equal(t, k2, k1)
}

func TestBuildKey_DedupQueryParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, false, true)

	r1 := httptest.NewRequest("GET", "http://example.com/page?a=2&a=1", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/page?a=2", nil)

	k1 := BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), policy)
	k2 := BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), policy)

	assert.Equal(t, k2, k1)
}

func TestBuildKey_Dedup_WithoutDedup(t *testing.T) {
	t.Parallel()

	r1 := httptest.NewRequest("GET", "http://example.com/page?a=2&a=1", nil)
	r2 := httptest.NewRequest("GET", "http://example.com/page?a=1&a=2", nil)

	k1 := BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), nil)
	k2 := BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), nil)

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

	k1 := BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), policy)
	k2 := BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), policy)

	assert.Equal(t, k2, k1)
}

func TestBuildKey_NoPolicyParity(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest("GET", "http://example.com/page?a=1&b=2&c=3", nil)
	k := BuildKey(requestInfoFromHTTP(r.Method, r.URL.String(), r.Host, r.URL.Path, r.TLS != nil, header.FromHTTP(r.Header)), nil)

	require.NotEqual(t, 0, k)

	r2 := httptest.NewRequest("GET", "http://example.com/page?a=1&b=2&c=3", nil)
	k2 := BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), nil)

	assert.Equal(t, k2, k)
}

func TestBuildKey_FastSlowPathParity(t *testing.T) {
	t.Parallel()

	r1 := httptest.NewRequest("GET", "http://example.com/page?a=1&b=2&a=3", nil)

	k1 := BuildKey(requestInfoFromHTTP(r1.Method, r1.URL.String(), r1.Host, r1.URL.Path, r1.TLS != nil, header.FromHTTP(r1.Header)), nil)

	r2 := httptest.NewRequest("GET", "http://example.com/page?a=1&b=2&a=3", nil)
	k2 := BuildKey(requestInfoFromHTTP(r2.Method, r2.URL.String(), r2.Host, r2.URL.Path, r2.TLS != nil, header.FromHTTP(r2.Header)), nil)

	assert.Equal(t, k2, k1)
}

func TestHasQueryPolicy(t *testing.T) {
	t.Parallel()
	t.Run("nil_policy", func(t *testing.T) {
		t.Parallel()
		var p *KeyPolicy
		assert.False(t, p.HasQueryPolicy())
	})
	t.Run("no_active_policy", func(t *testing.T) {
		t.Parallel()
		p := NewKeyPolicy(nil, nil, nil, nil, false, false)
		assert.False(t, p.HasQueryPolicy())
	})
	t.Run("strip_params", func(t *testing.T) {
		t.Parallel()
		p := NewKeyPolicy(map[string]bool{"utm_source": true}, nil, nil, nil, false, false)
		assert.True(t, p.HasQueryPolicy())
	})
	t.Run("keep_params", func(t *testing.T) {
		t.Parallel()
		p := NewKeyPolicy(nil, map[string]bool{"id": true}, nil, nil, false, false)
		assert.True(t, p.HasQueryPolicy())
	})
	t.Run("strip_prefixes", func(t *testing.T) {
		t.Parallel()
		p := NewKeyPolicy(nil, nil, nil, []string{"utm_"}, false, false)
		assert.True(t, p.HasQueryPolicy())
	})
	t.Run("strip_empty", func(t *testing.T) {
		t.Parallel()
		p := NewKeyPolicy(nil, nil, nil, nil, true, false)
		assert.True(t, p.HasQueryPolicy())
	})
	t.Run("dedup", func(t *testing.T) {
		t.Parallel()
		p := NewKeyPolicy(nil, nil, nil, nil, false, true)
		assert.True(t, p.HasQueryPolicy())
	})
}

func TestShouldStripParam(t *testing.T) {
	t.Parallel()
	t.Run("nil_policy", func(t *testing.T) {
		t.Parallel()
		var p *KeyPolicy
		assert.False(t, p.shouldStripParam("k", "v", nil))
	})
	t.Run("keep_params_strips_non_member", func(t *testing.T) {
		t.Parallel()
		p := NewKeyPolicy(nil, map[string]bool{"id": true}, nil, nil, false, false)
		assert.True(t, p.shouldStripParam("utm", "v", &stackSeen{}))
	})
	t.Run("keep_params_allows_member", func(t *testing.T) {
		t.Parallel()
		p := NewKeyPolicy(nil, map[string]bool{"id": true}, nil, nil, false, false)
		assert.False(t, p.shouldStripParam("id", "v", &stackSeen{}))
	})
	t.Run("strip_empty_strips_empty_value", func(t *testing.T) {
		t.Parallel()
		p := NewKeyPolicy(nil, nil, nil, nil, true, false)
		assert.True(t, p.shouldStripParam("k", "", &stackSeen{}))
	})
	t.Run("strip_empty_keeps_non_empty", func(t *testing.T) {
		t.Parallel()
		p := NewKeyPolicy(nil, nil, nil, nil, true, false)
		assert.False(t, p.shouldStripParam("k", "v", &stackSeen{}))
	})
	t.Run("strip_params_blocklist", func(t *testing.T) {
		t.Parallel()
		p := NewKeyPolicy(map[string]bool{"utm_source": true}, nil, nil, nil, false, false)
		assert.True(t, p.shouldStripParam("utm_source", "v", &stackSeen{}))
	})
	t.Run("strip_prefixes", func(t *testing.T) {
		t.Parallel()
		p := NewKeyPolicy(nil, nil, nil, []string{"utm_"}, false, false)
		assert.True(t, p.shouldStripParam("utm_source", "v", &stackSeen{}))
		assert.False(t, p.shouldStripParam("id", "v", &stackSeen{}))
	})
	t.Run("dedup_strips_duplicate", func(t *testing.T) {
		t.Parallel()
		p := NewKeyPolicy(nil, nil, nil, nil, false, true)
		seen := &stackSeen{}
		seen.add("k")
		assert.True(t, p.shouldStripParam("k", "v2", seen))
	})
	t.Run("dedup_keeps_first", func(t *testing.T) {
		t.Parallel()
		p := NewKeyPolicy(nil, nil, nil, nil, false, true)
		seen := &stackSeen{}
		assert.False(t, p.shouldStripParam("k", "v1", seen))
	})
}

func TestMarkSeen(t *testing.T) {
	t.Parallel()
	t.Run("nil_policy", func(t *testing.T) {
		t.Parallel()
		var p *KeyPolicy
		p.markSeen("k", &stackSeen{})
	})
	t.Run("dedup_false", func(t *testing.T) {
		t.Parallel()
		p := NewKeyPolicy(nil, nil, nil, nil, false, false)
		seen := &stackSeen{}
		p.markSeen("k", seen)
		assert.Equal(t, 0, seen.n)
	})
	t.Run("nil_seen", func(t *testing.T) {
		t.Parallel()
		p := NewKeyPolicy(nil, nil, nil, nil, false, true)
		p.markSeen("k", nil)
	})
}

func TestStackSeen_Contains(t *testing.T) {
	t.Parallel()
	s := &stackSeen{}
	s.add("a")
	s.add("b")
	assert.True(t, s.contains("a"))
	assert.True(t, s.contains("b"))
	assert.False(t, s.contains("c"))
}

func TestStackSeen_Add_Overflow(t *testing.T) {
	t.Parallel()
	s := &stackSeen{}
	for i := range 8 {
		s.add("k" + string(rune('0'+i)))
	}
	assert.Equal(t, 8, s.n)
	s.add("overflow")
	assert.Equal(t, 8, s.n)
	assert.False(t, s.contains("overflow"))
}

func TestShouldExcludeHeader(t *testing.T) {
	t.Parallel()
	t.Run("nil_policy", func(t *testing.T) {
		t.Parallel()
		var p *KeyPolicy
		assert.False(t, p.ShouldExcludeHeader("x-request-id"))
	})
	t.Run("nil_map", func(t *testing.T) {
		t.Parallel()
		p := NewKeyPolicy(nil, nil, nil, nil, false, false)
		assert.False(t, p.ShouldExcludeHeader("x-request-id"))
	})
	t.Run("present", func(t *testing.T) {
		t.Parallel()
		p := NewKeyPolicy(nil, nil, map[string]bool{"x-request-id": true}, nil, false, false)
		assert.True(t, p.ShouldExcludeHeader("x-request-id"))
	})
	t.Run("absent", func(t *testing.T) {
		t.Parallel()
		p := NewKeyPolicy(nil, nil, map[string]bool{"x-request-id": true}, nil, false, false)
		assert.False(t, p.ShouldExcludeHeader("accept"))
	})
}
