package cloudflare_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"

	cf "github.com/bouine-cache/bouine/internal/cloudflare"
)

func TestMapURL(t *testing.T) {
	t.Parallel()
	r := cf.MapURL("https://example.com/page")
	if len(r.URLs) != 1 || r.Skipped {
		t.Fatalf("unexpected: %+v", r)
	}
}

func TestMapURL_Empty(t *testing.T) {
	t.Parallel()
	r := cf.MapURL("")
	require.True(t, r.Skipped)
}

func TestMapSurrogateKey(t *testing.T) {
	t.Parallel()
	r := cf.MapSurrogateKey("product-456")
	if len(r.Tags) != 1 || r.Tags[0] != "product-456" {
		t.Fatalf("unexpected: %+v", r)
	}
}

func TestMapPathRegex_PlainPrefix(t *testing.T) {
	t.Parallel()
	cases := []struct{ in, want string }{
		{"^/api/v1/", "/api/v1/"},
		{"/products/", "/products/"},
		{"^/", "/"},
	}
	for _, tc := range cases {
		r := cf.MapPathRegex(tc.in)
		assert.False(t, r.Skipped)
		if len(r.Prefixes) == 0 || r.Prefixes[0] != tc.want {
			t.Errorf("%q: want prefix %q, got %v", tc.in, tc.want, r.Prefixes)
		}
	}
}

func TestMapPathRegex_Metacharacters(t *testing.T) {
	t.Parallel()
	cases := []string{"^/api/[a-z]+", "^/api/.*", "/api/(v1|v2)"}
	for _, tc := range cases {
		r := cf.MapPathRegex(tc)
		assert.True(t, r.Skipped)
		assert.Contains(t, r.SkipReason, "metacharacters")
	}
}

func TestMapHostRegex_Literal(t *testing.T) {
	t.Parallel()
	cases := []struct{ in, want string }{
		{`example\.com`, "example.com"},
		{"example.com", "example.com"},
		{"api.example.com", "api.example.com"},
	}
	for _, tc := range cases {
		r := cf.MapHostRegex(tc.in)
		assert.False(t, r.Skipped)
		if len(r.Hosts) == 0 || r.Hosts[0] != tc.want {
			t.Errorf("%q: want host %q, got %v", tc.in, tc.want, r.Hosts)
		}
	}
}

func TestMapHostRegex_Metacharacters(t *testing.T) {
	t.Parallel()
	cases := []string{".*\\.example\\.com", "(api|www)\\.example\\.com"}
	for _, tc := range cases {
		r := cf.MapHostRegex(tc)
		assert.True(t, r.Skipped)
	}
}

func TestMapPathRegex_SuffixAnchor(t *testing.T) {
	t.Parallel()
	cases := []string{"^/api/v1/$", "/exact$"}
	for _, tc := range cases {
		r := cf.MapPathRegex(tc)
		assert.True(t, r.Skipped)
		assert.Contains(t, r.SkipReason, "suffix anchor")
	}
}

func TestMapHostRegex_Anchor(t *testing.T) {
	t.Parallel()
	cases := []string{"^example.com", "^api\\.example\\.com"}
	for _, tc := range cases {
		r := cf.MapHostRegex(tc)
		assert.True(t, r.Skipped)
		assert.Contains(t, r.SkipReason, "anchors")
	}
}

func TestSkipCategory(t *testing.T) {
	t.Parallel()
	tests := []struct {
		reason string
		want   string
	}{
		{"empty URL", cf.SkipCategoryEmpty},
		{"path_regex contains metacharacters — cannot map to CF prefix: ^/api/[0-9]+", cf.SkipCategoryPathMetachar},
		{"path_regex is a suffix anchor (ends with $) — cannot map to CF prefix: ^/api/$", cf.SkipCategoryPathSuffix},
		{"host_regex contains metacharacters — cannot map to CF hostname: .*example.*", cf.SkipCategoryHostMetachar},
		{"host_regex contains anchors — cannot map to CF hostname: ^example.com", cf.SkipCategoryHostAnchor},
		{"compound ban (host AND path) cannot be mapped to a single CF purge operation", cf.SkipCategoryCompoundBan},
		{"something weird", "unknown"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()
			got := cf.SkipCategory(tc.reason)
			if got != tc.want {
				t.Fatalf("SkipCategory(%q) = %q, want %q", tc.reason, got, tc.want)
			}
		})
	}
}

func TestMergeResults(t *testing.T) {
	t.Parallel()
	a := cf.MapURL("https://a.com/")
	b := cf.MapSurrogateKey("tag-1")
	c := cf.MapPathRegex("^/api/")
	m := cf.MergeResults(a, b, c)
	if len(m.URLs) != 1 || len(m.Tags) != 1 || len(m.Prefixes) != 1 {
		t.Fatalf("merge failed: %+v", m)
	}
	require.False(t, m.Skipped)
}
