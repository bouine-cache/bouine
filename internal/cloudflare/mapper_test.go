package cloudflare_test

import (
	"strings"
	"testing"

	cf "github.com/thylong/bouine/internal/cloudflare"
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
	if !r.Skipped {
		t.Fatal("empty URL should be skipped")
	}
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
		if r.Skipped {
			t.Errorf("%q: unexpected skip: %s", tc.in, r.SkipReason)
		}
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
		if !r.Skipped {
			t.Errorf("%q: expected skip for metacharacters", tc)
		}
		if !strings.Contains(r.SkipReason, "metacharacters") {
			t.Errorf("%q: skip reason should mention metacharacters: %s", tc, r.SkipReason)
		}
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
		if r.Skipped {
			t.Errorf("%q: unexpected skip: %s", tc.in, r.SkipReason)
		}
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
		if !r.Skipped {
			t.Errorf("%q: expected skip for metacharacters", tc)
		}
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
	if m.Skipped {
		t.Fatal("merge of non-skipped results should not be skipped")
	}
}
