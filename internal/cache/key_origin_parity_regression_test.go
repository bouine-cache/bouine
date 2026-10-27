package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/pkg/header"
)

// key_origin_parity_regression_test.go pins issue #847 (audit AUD-04):
// the cache key and the origin-bound request-target must claim the same
// identity. The slow path keys fasthttp's normalized path (percent
// decoding, dot-segment resolution) while the raw target used to be
// forwarded verbatim, so /a/../b and /b shared one key although the
// origin saw two different targets — the response fetched under one
// spelling was stored and served under the other.

// TestOriginTarget_NormalizedToKeyIdentity proves the poisoning is
// closed end to end: an origin that distinguishes request-target
// spellings can never observe the odd spelling through bouine, so a
// client using it is served exactly what the clean spelling's entry
// holds — never content fetched under a different spelling.
func TestOriginTarget_NormalizedToKeyIdentity(t *testing.T) {
	t.Parallel()

	var originTargets []string
	origin := func(ctx *fasthttp.RequestCtx) {
		originTargets = append(originTargets, string(ctx.RequestURI()))
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("raw:" + string(ctx.RequestURI()))
	}
	h := testHandler(t, origin)

	// Client A uses the dot-segment spelling; the origin must receive
	// the normalized form (the identity the key claims).
	a := testCtx("GET", "http://example.com/a/../secret")
	serveRequest(h, a)
	require.Equal(t, "MISS", respHeader(a, header.XCache))
	require.NotContains(t, respBody(a), "a/../",
		"the odd spelling must never reach the origin or the response")
	require.Len(t, originTargets, 1)
	assert.NotContains(t, originTargets[0], "a/../",
		"the origin-bound target must be normalized, not the raw spelling")

	// Client B uses the clean spelling: a HIT of the same entry A's
	// normalized request filled — identical bytes for identical keys.
	b := testCtx("GET", "http://example.com/secret")
	serveRequest(h, b)
	require.Equal(t, "HIT", respHeader(b, header.XCache))
	assert.Equal(t, respBody(a), respBody(b),
		"both spellings claim one key; both must be served one identity")
}

// TestOriginTarget_QueryPreservedVerbatim pins that only the PATH is
// normalized: the query string crosses to the origin byte-for-byte
// (the key sorts and re-escapes it, but reordering on the wire would
// change duplicate-param and order semantics origins may rely on).
func TestOriginTarget_QueryPreservedVerbatim(t *testing.T) {
	t.Parallel()

	const query = "b=2&a=1&a=0&x=%2F"
	var gotTarget string
	origin := func(ctx *fasthttp.RequestCtx) {
		gotTarget = string(ctx.RequestURI())
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("ok")
	}
	h := testHandler(t, origin)

	a := testCtx("GET", "http://example.com/a/../q?"+query)
	serveRequest(h, a)
	require.Equal(t, "MISS", respHeader(a, header.XCache))
	assert.Contains(t, gotTarget, "?"+query,
		"the query must cross to the origin verbatim")
	assert.NotContains(t, gotTarget, "a/../",
		"only the path is normalized")
}

// TestBuildKeyFromURL_MatchesDataPlaneKey pins the admin-plane parity:
// `bouine keys find` / `bouine purge` must compute the key the data
// plane stored, including for dot-segment spellings (net/url decodes
// percent escapes but does not resolve dot segments). The data-plane
// reference is the real serving form: BuildKeyFast over a served
// request's fasthttp ctx, whose Path() is fasthttp's normalized path.
func TestBuildKeyFromURL_MatchesDataPlaneKey(t *testing.T) {
	t.Parallel()

	cases := []string{
		"http://example.com/secret",
		"http://example.com/a/../secret",
		"http://example.com/x/./y",
		"http://example.com/trail/..",
		"http://example.com/trail/.",
		"http://example.com/caf%C3%A9",
		"http://example.com/caf%C3%A9/q?x=%2F",
	}
	for _, rawURL := range cases {
		ctx := testCtx("GET", rawURL)
		dataPlane := BuildKeyFast(ctx.Method(), ctx.RequestURI(), ctx.Host(), ctx.Path(), ctx.IsTLS(), nil)
		admin := BuildKeyFromURL(rawURL, nil)
		assert.Equal(t, dataPlane, admin,
			"admin-plane key diverged from the data-plane key for %q", rawURL)
	}
}

// TestPathNeedsNormalization pins the parity detector's truth table:
// everything fasthttp's normalizePath would rewrite is flagged;
// canonical spellings — including dotted names and duplicate slashes —
// are not (`//` is collapsed identically by both key builders).
func TestPathNeedsNormalization(t *testing.T) {
	t.Parallel()

	cases := []struct {
		path string
		want bool
	}{
		{"/", false},
		{"/a/b", false},
		{"/logo.png", false},
		{"/a.b/c", false},
		{"/static//js/app.js", false}, // both key builders collapse //
		{"/a/../b", true},
		{"/a/./b", true},
		{"/a/.", true},
		{"/a/..", true},
		{"/%2e%2e/b", true},
		{"/caf%C3%A9", true},
		{"/a#frag", true},
		{"http://example.com/a", true}, // absolute-form
		{"*", true},                    // asterisk-form
		{"", true},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, pathNeedsNormalization(tc.path),
			"pathNeedsNormalization(%q)", tc.path)
		assert.Equal(t, tc.want, pathNeedsNormalization([]byte(tc.path)),
			"pathNeedsNormalization([]byte(%q))", tc.path)
	}
}
