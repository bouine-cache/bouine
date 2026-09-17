package cache

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/valyala/fasthttp"
)

// TestRewriteURI_Matrix pins the RewriteURI contract: query split before
// matching, first-match-only (nginx semantics), capture expansion, the
// absolute-path guard, and the pass-through cases. Each row is a
// (pattern, template, input, expected) tuple; expected "" means the
// input must pass through unchanged.
func TestRewriteURI_Matrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		pattern  string
		replace  string
		in       string
		expected string
	}{
		{"nginx callback shape", `^/payment/orchestrator/callback/(.*)$`, "/scrooge/callback/$1",
			"/payment/orchestrator/callback/payin123", "/scrooge/callback/payin123"},
		{"nginx shape, nested tail", `^/payment/orchestrator/callback/(.*)$`, "/scrooge/callback/$1",
			"/payment/orchestrator/callback/a/b/c", "/scrooge/callback/a/b/c"},
		{"nginx shape, query preserved", `^/payment/orchestrator/callback/(.*)$`, "/scrooge/callback/$1",
			"/payment/orchestrator/callback/x?sig=1&z=2", "/scrooge/callback/x?sig=1&z=2"},
		{"query never matched", `^/payment(.*)$`, "/internal$1",
			"/payment?x=/secret", "/internal?x=/secret"},
		{"query containing question mark kept whole", `^/a/(.*)$`, "/b/$1",
			"/a/x?y=1?z=2", "/b/x?y=1?z=2"},
		{"unanchored first match only", `/v1/`, "/v2/",
			"/api/v1/a/v1/b", "/api/v2/a/v1/b"},
		{"no match passes through", `^/other/`, "/x/",
			"/payment/orchestrator/callback/y", "/payment/orchestrator/callback/y"},
		{"no query input", `^/old/(.*)$`, "/new/$1",
			"/old/file", "/new/file"},
		{"exact path capture empty", `^/cb/?$`, "/root",
			"/cb", "/root"},
		{"optional slash consumes one", `^/cb/?$`, "/root",
			"/cb/", "/root"},
		{"multiple groups reordered", `^/shop/(\d+)/item/(\d+)`, "/items/$2/shop/$1",
			"/shop/42/item/7?ref=x", "/items/7/shop/42?ref=x"},
		{"named group via index", `^/u/(?P<name>[a-z]+)$`, "/user/$1",
			"/u/alice", "/user/alice"},
		{"absolute result required: relative discarded", `^/foo/`, "bar/",
			"/foo/x", "/foo/x"},
		{"empty result discarded", `^/foo$`, "",
			"/foo", "/foo"},
		{"literal dollar via $$", `^/price/(.*)$`, "/p/$$$1",
			"/price/9", "/p/$9"},
		{"pattern matching empty path", `^$`, "/root",
			"", "/root"},
		{"root path rewrite", `^/$`, "/home",
			"/", "/home"},
		{"rewrite to root", `^/deep/nested/(.*)$`, "/$1",
			"/deep/nested/p", "/p"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rw := NewPathRewrite(tt.pattern, tt.replace)
			got := string(rw.RewriteURI([]byte(tt.in)))
			assert.Equal(t, tt.expected, got)
		})
	}
}

// TestRewriteURI_OutputCap pins the amplification guard: a template
// that quadruples the path must stop at maxRewriteOutputBytes and pass
// the original URI through rather than building a multi-KiB path.
func TestRewriteURI_OutputCap(t *testing.T) {
	t.Parallel()
	rw := NewPathRewrite(`^/(.*)$`, "/$1$1$1$1")
	// 5000 a's would expand to 20001 bytes — past the 16 KiB cap.
	long := "/" + strings.Repeat("a", 5000)
	got := string(rw.RewriteURI([]byte(long)))
	assert.Equal(t, long, got, "oversized output must pass through unchanged")

	// Sanity: a small input still rewrites (the cap is a guard, not a
	// blanket refusal). "/" + 10 a's -> "/" + 4×10 a's = 41 bytes.
	small := "/" + strings.Repeat("a", 10)
	got = string(rw.RewriteURI([]byte(small)))
	assert.Equal(t, 41, len(got))
}

// TestRewriteURI_NoAliasMutation pins that the returned slice never
// aliases the input for a real rewrite: callers hand the result to
// SetRequestURIBytes and must not see later mutations of the source.
func TestRewriteURI_NoAliasMutation(t *testing.T) {
	t.Parallel()
	rw := NewPathRewrite(`^/payment/(.*)$`, "/internal/$1")
	src := []byte("/payment/orchestrator")
	out := rw.RewriteURI(src)
	require.Equal(t, "/internal/orchestrator", string(out))
	// Mutating the source after the rewrite must not change the result.
	for i := range src {
		src[i] = 'x'
	}
	assert.Equal(t, "/internal/orchestrator", string(out))
}

// TestRewriteURI_PassThroughAliasesInput pins the cheap case: when no
// rewrite applies, the original slice is returned as-is (no copy) so
// the miss path stays allocation-lean on routes where nothing matches.
func TestRewriteURI_PassThroughAliasesInput(t *testing.T) {
	t.Parallel()
	rw := NewPathRewrite(`^/nomatch/`, "/x")
	src := []byte("/untouched/path?q=1")
	out := rw.RewriteURI(src)
	// Same backing array + same length: the slice was returned, not
	// copied. (&src[0] is valid: src is non-empty.)
	require.Same(t, &src[0], &out[0])
	require.Equal(t, len(src), len(out))
}

// TestPathRewrite_String pins the dashboard/log rendering.
func TestPathRewrite_String(t *testing.T) {
	t.Parallel()
	rw := NewPathRewrite(`^/a/(.*)$`, "/b/$1")
	assert.Equal(t, `^/a/(.*)$ -> /b/$1`, rw.String())
}

// TestPathRewrite_Apply pins the in-place fasthttp application used by
// the builder for non-cached static routes: match mutates the request
// URI; no match leaves it untouched.
func TestPathRewrite_Apply(t *testing.T) {
	t.Parallel()
	rw := NewPathRewrite(`^/cb/(.*)$`, "/scrooge/callback/$1")

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.SetRequestURI("/cb/payin?x=1")
	rw.Apply(req)
	assert.Equal(t, "/scrooge/callback/payin?x=1", string(req.RequestURI()))

	req2 := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req2)
	req2.SetRequestURI("/other/path")
	rw.Apply(req2)
	assert.Equal(t, "/other/path", string(req2.RequestURI()))
}

// TestNewPathRewrite_MustCompile panics on an invalid pattern. The
// pattern is compiled earlier by config validation, so an invalid
// pattern reaching this constructor is a programming error.
func TestNewPathRewrite_MustCompile(t *testing.T) {
	t.Parallel()
	assert.Panics(t, func() { NewPathRewrite("(", "/x") })
}
