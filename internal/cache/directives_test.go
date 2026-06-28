package cache

import (
	"net/http"
	"testing"
	"time"
)

func mkHeader(kv map[string]string) http.Header {
	h := http.Header{}
	for k, v := range kv {
		h.Set(k, v)
	}
	return h
}

func TestFreshnessLifetimeH_CDNCCNoMaxAgeFallsThrough(t *testing.T) {
	t.Parallel()
	date := "Mon, 01 Jan 2024 00:00:00 GMT"
	future := "Mon, 01 Jan 2024 01:00:00 GMT"

	cases := []struct {
		name    string
		respCC  Directives
		header  http.Header
		wantTTL time.Duration
		wantOK  bool
	}{
		{
			name:    "cdn-cc public falls through to cc max-age",
			respCC:  ParseCacheControl("max-age=3600"),
			header:  mkHeader(map[string]string{"CDN-Cache-Control": "public"}),
			wantTTL: 3600 * time.Second,
			wantOK:  true,
		},
		{
			name:    "cdn-cc public falls through to cc s-maxage",
			respCC:  ParseCacheControl("s-maxage=600"),
			header:  mkHeader(map[string]string{"CDN-Cache-Control": "public"}),
			wantTTL: 600 * time.Second,
			wantOK:  true,
		},
		{
			name:    "cdn-cc stale-while-revalidate falls through to expires",
			respCC:  Directives{},
			header:  mkHeader(map[string]string{"CDN-Cache-Control": "stale-while-revalidate=60", "Date": date, "Expires": future}),
			wantTTL: time.Hour,
			wantOK:  true,
		},
		{
			name:    "cdn-cc public no other freshness returns not explicit",
			respCC:  Directives{},
			header:  mkHeader(map[string]string{"CDN-Cache-Control": "public"}),
			wantTTL: 0,
			wantOK:  false,
		},
		{
			name:    "cdn-cc max-age still takes precedence over cc max-age",
			respCC:  Directives{},
			header:  mkHeader(map[string]string{"CDN-Cache-Control": "max-age=120"}),
			wantTTL: 120 * time.Second,
			wantOK:  true,
		},
		{
			name:    "cdn-cc no-store still blocks",
			respCC:  Directives{},
			header:  mkHeader(map[string]string{"CDN-Cache-Control": "no-store"}),
			wantTTL: 0,
			wantOK:  true,
		},
		{
			name:    "cdn-cc private still blocks",
			respCC:  Directives{},
			header:  mkHeader(map[string]string{"CDN-Cache-Control": "private"}),
			wantTTL: 0,
			wantOK:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ttl, ok := FreshnessLifetimeH(tc.respCC, tc.header)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if ttl != tc.wantTTL {
				t.Fatalf("ttl = %v, want %v", ttl, tc.wantTTL)
			}
		})
	}
}

func TestFreshnessLifetime_CDNCCNoMaxAgeFallsThrough(t *testing.T) {
	t.Parallel()
	date := "Mon, 01 Jan 2024 00:00:00 GMT"
	future := "Mon, 01 Jan 2024 01:00:00 GMT"

	cases := []struct {
		name    string
		respCC  Directives
		header  http.Header
		wantTTL time.Duration
		wantOK  bool
	}{
		{
			name:    "cdn-cc public falls through to cc max-age",
			respCC:  ParseCacheControl("max-age=3600"),
			header:  mkHeader(map[string]string{"CDN-Cache-Control": "public"}),
			wantTTL: 3600 * time.Second,
			wantOK:  true,
		},
		{
			name:    "cdn-cc stale-while-revalidate falls through to expires",
			respCC:  Directives{},
			header:  mkHeader(map[string]string{"CDN-Cache-Control": "stale-while-revalidate=60", "Date": date, "Expires": future}),
			wantTTL: time.Hour,
			wantOK:  true,
		},
		{
			name:    "cdn-cc no-store still blocks",
			respCC:  Directives{},
			header:  mkHeader(map[string]string{"CDN-Cache-Control": "no-store"}),
			wantTTL: 0,
			wantOK:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ttl, ok := FreshnessLifetime(tc.respCC, func(key string) string { return tc.header.Get(key) })
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if ttl != tc.wantTTL {
				t.Fatalf("ttl = %v, want %v", ttl, tc.wantTTL)
			}
		})
	}
}

func TestIsCacheable_CDNCCPublicWithCCMaxAge(t *testing.T) {
	t.Parallel()
	resp := mkHeader(map[string]string{
		"CDN-Cache-Control": "public",
		"Cache-Control":     "max-age=3600",
	})
	if !IsCacheable(200, http.Header{}, resp) {
		t.Fatal("CDN-Cache-Control: public + Cache-Control: max-age=3600 should be cacheable")
	}
}
