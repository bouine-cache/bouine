package cache

import (
	"net/http/httptest"
	"testing"
)

func BenchmarkBuildKey_NoPolicy(b *testing.B) {
	r := httptest.NewRequest("GET", "http://example.com/page?a=1&b=2&c=3&d=4", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = BuildKey(r, nil)
	}
}

func BenchmarkBuildKey_KeepParams(b *testing.B) {
	policy := NewKeyPolicy(nil, map[string]bool{"a": true, "b": true}, nil, nil, false, false)
	r := httptest.NewRequest("GET", "http://example.com/page?a=1&b=2&utm_source=x&c=3&d=4", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = BuildKey(r, policy)
	}
}

func BenchmarkBuildKey_StripPrefix(b *testing.B) {
	policy := NewKeyPolicy(nil, nil, nil, []string{"utm_", "fbclid", "gclid", "_ga"}, false, false)
	r := httptest.NewRequest("GET", "http://example.com/page?a=1&b=2&utm_source=x&c=3&d=4", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = BuildKey(r, policy)
	}
}

func BenchmarkBuildKey_StripEmpty(b *testing.B) {
	policy := NewKeyPolicy(nil, nil, nil, nil, true, false)
	r := httptest.NewRequest("GET", "http://example.com/page?a=1&b=&c=3&d=4&e=", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = BuildKey(r, policy)
	}
}

func BenchmarkBuildKey_Dedup(b *testing.B) {
	policy := NewKeyPolicy(nil, nil, nil, nil, false, true)
	r := httptest.NewRequest("GET", "http://example.com/page?a=1&b=2&c=3&a=4&d=5", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = BuildKey(r, policy)
	}
}

func BenchmarkBuildKey_AllPolicies(b *testing.B) {
	policy := NewKeyPolicy(
		map[string]bool{"tracker": true},
		nil,
		map[string]bool{"x-debug": true},
		[]string{"utm_", "fbclid"},
		true, true,
	)
	r := httptest.NewRequest("GET", "http://example.com/page?a=1&b=2&utm_source=x&c=3&a=4&tracker=x&d=", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = BuildKey(r, policy)
	}
}
