package cache

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/pkg/header"
)

// TestLangBucket covers the Accept-Language bucketing rules
// (docs/plans/accept-encoding-bucketing.md §10.2): the bucket is the
// highest-weight tag in the client's original casing (the KEY
// lowercases it at the dispatch; the outbound rewrite preserves the
// conventional casing), subtag preserved; ties resolve
// lexicographically so the winner is order-independent.
func TestLangBucket(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		in     string
		want   string
		wantOK bool
	}{
		{"empty", "", "", false},
		{"whitespace only", "   ", "", false},
		{"single tag", "fr", "fr", true},
		{"single tag case", "FR", "FR", true},
		{"subtag preserved", "fr-FR", "fr-FR", true},
		{"subtag case", "fr-FR,fr;q=0.9", "fr-FR", true},
		{"highest q wins", "fr;q=0.5, de;q=1.0", "de", true},
		{"first weight default", "fr, de;q=0.5", "fr", true},
		{"chain spelling en first", "fr-FR,fr;q=0.9,en;q=0.8", "fr-FR", true},
		{"chain spelling en-us mid", "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7", "fr-FR", true},
		{"tie lexicographic", "en, de", "de", true},
		{"tie lexicographic swapped", "de, en", "de", true},
		{"tie with mixed case", "eN, De", "De", true},
		{"tie at explicit q", "zstd;q=1, br;q=1", "br", true},
		{"tie three-way order 1", "c, b, a", "a", true},
		{"tie three-way order 2", "a, c, b", "a", true},
		{"tie three-way order 3", "b, a, c", "a", true},
		{"tie subtag vs base", "fr-FR, fr", "fr", true},
		{"tie base vs subtag", "fr, fr-FR", "fr", true},
		{"q=0 excluded", "fr;q=0, de", "de", true},
		{"all q=0", "fr;q=0, de;q=0.0", "", false},
		{"wildcard only", "*", "", false},
		{"wildcard with tag", "*, fr", "fr", true},
		{"wildcard first", "*, fr;q=0.8, de", "de", true},
		{"wildcard q=0 with tag", "*;q=0, fr", "fr", true},
		{"spaces around tokens", "  fr ,  de;q=0.5  ", "fr", true},
		{"space in weight", "fr; q=1, de; q=0.5", "fr", true},
		{"malformed weight defaults to 1", "fr;q=banana, de;q=0.5", "fr", true},
		{"malformed tags only", "!!!, ???", "", false},
		{"stray separators", ", ,,", "", false},
		{"empty items between tags", "fr, , de;q=0.5", "fr", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := langBucket(tc.in)
			require.Equal(t, tc.wantOK, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestLangBucket_OrderIndependence proves the property the tie rule
// exists for: the winner is a pure function of the candidate set, not
// chain order. A violation means fill and lookup disagree across
// spellings — the cross-variant body-swap bug class (ADR-0046).
func TestLangBucket_OrderIndependence(t *testing.T) {
	t.Parallel()
	sets := [][]string{
		{"en", "de"},
		{"de", "en", "fr"},
		{"fr-FR", "fr", "en"},
		{"zh-CN", "zh-TW", "en;q=0.5"},
		{"a", "b", "c", "d", "e"},
	}
	for _, set := range sets {
		first, ok := langBucket(strings.Join(set, ", "))
		require.True(t, ok)
		// Every permutation must yield the same bucket.
		permute(t, set, func(perm []string) {
			got, ok := langBucket(strings.Join(perm, ", "))
			require.True(t, ok)
			require.Equal(t, first, got, "order dependence for set %v", perm)
		})
	}
}

// permute invokes fn for every permutation of items (bounded: only
// used with short slices in tests).
func permute(t *testing.T, items []string, fn func([]string)) {
	t.Helper()
	var walk func(cur, rest []string)
	walk = func(cur, rest []string) {
		if len(rest) == 0 {
			fn(cur)
			return
		}
		for i := range rest {
			next := append([]string{}, cur...)
			nextRest := append([]string{}, rest[:i]...)
			nextRest = append(nextRest, rest[i+1:]...)
			walk(append(next, rest[i]), nextRest)
		}
	}
	walk(nil, items)
}

// TestVaryKeyLanguageBucket_ParityAcrossPaths extends the AE parity
// contract (ADR-0046 body-swap guard) to Accept-Language: every
// variant-key construction path must agree on the lang bucket, or one
// path stores and another resolves under different keys.
func TestVaryKeyLanguageBucket_ParityAcrossPaths(t *testing.T) {
	t.Parallel()
	primary := BuildKeyFromURL("http://example.com/page", nil)

	chains := []struct {
		name string
		al   string
	}{
		{"cascade", "fr-FR,fr;q=0.9,en;q=0.8"},
		{"cascade alt spelling", "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"},
		{"q winner", "fr;q=0.5, de;q=1.0"},
		{"tie", "en, de"},
		{"tie swapped", "de, en"},
		{"subtag kept", "zh-CN,zh;q=0.9,en;q=0.8"},
		{"unbucketable", "*"},
	}

	for _, c := range chains {
		hm := headerMap(header.AcceptLanguage, c.al)
		fh := &fasthttp.RequestHeader{}
		fh.Set(header.AcceptLanguage, c.al)

		vkMap := VariantKey(primary, "Accept-Language", hm, nil)
		vkFast := VariantKeyFast(primary, "Accept-Language", fh, nil)
		vkSlow := variantKeySlow(primary, "accept-language", hm, nil)
		bvk := BuildVaryKey("Accept-Language", hm, nil)

		require.Equal(t, vkMap, vkFast, "map vs fasthttp path disagree for %s", c.name)
		require.Equal(t, vkMap, vkSlow, "map vs slow path disagree for %s", c.name)
		require.NotEmpty(t, bvk, "peer-gate hex empty for %s", c.name)
	}

	// The two cascade spellings share one variant key; the tie
	// spellings share one; the q-winner is distinct from the fr cascade.
	collapse := []struct {
		a, b string
	}{
		{"fr-FR,fr;q=0.9,en;q=0.8", "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"},
		{"en, de", "de, en"},
	}
	for _, c := range collapse {
		ka := VariantKey(primary, "Accept-Language", headerMap(header.AcceptLanguage, c.a), nil)
		kb := VariantKey(primary, "Accept-Language", headerMap(header.AcceptLanguage, c.b), nil)
		require.Equal(t, ka, kb, "spelling pair must share one bucket: %q vs %q", c.a, c.b)
	}
}

// TestHandler_LanguageBucketPairing proves the §10.4 invariant end to
// end: two chain spellings with the same winner share one fill, the
// origin sees the winner tag only, and a chain-echoing origin stores
// bucket-consistent bodies.
func TestHandler_LanguageBucketPairing(t *testing.T) {
	t.Parallel()
	var originCalls int
	var seenAL []string
	upstream := func(ctx *fasthttp.RequestCtx) {
		originCalls++
		seenAL = append(seenAL, string(ctx.Request.Header.Peek(header.AcceptLanguage)))
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.Vary, "Accept-Language")
		ctx.Response.Header.Set("Content-Language", "fr")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("content"))
	}
	h := testHandler(t, upstream)

	// The upstream vary-lang-select scenario, replayed locally: fill
	// with "en, de" (tie — bucket is the lexicographic winner "de"),
	// then "fr;q=0.5, de;q=1.0" (q winner "de") must hit.
	r1 := testCtx("GET", "http://example.com/i18n")
	r1.Request.Header.Set(header.AcceptLanguage, "en, de")
	serveRequest(h, r1)
	require.Equal(t, "MISS", respHeader(r1, header.XCache))

	r2 := testCtx("GET", "http://example.com/i18n")
	r2.Request.Header.Set(header.AcceptLanguage, "fr;q=0.5, de;q=1.0")
	serveRequest(h, r2)
	require.Equal(t, "HIT", respHeader(r2, header.XCache),
		"chains selecting de must share the de bucket")
	require.Equal(t, "content", respBody(r2))

	require.Equal(t, 1, originCalls, "same-winner chains must be one fill")
	require.Equal(t, []string{"de"}, seenAL,
		"origin must receive the winner tag, not the raw chains")
}

// TestHandler_LanguageBucketSubtagsDistinct pins the layer-1 boundary:
// subtags do NOT collapse — en-US and en-GB are distinct variants
// until the Content-Language-anchored layer 2 licenses it.
func TestHandler_LanguageBucketSubtagsDistinct(t *testing.T) {
	t.Parallel()
	var originCalls int
	upstream := func(ctx *fasthttp.RequestCtx) {
		originCalls++
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.Vary, "Accept-Language")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body-" + string(ctx.Request.Header.Peek(header.AcceptLanguage))))
	}
	h := testHandler(t, upstream)

	rUS := testCtx("GET", "http://example.com/doc")
	rUS.Request.Header.Set(header.AcceptLanguage, "en-US")
	serveRequest(h, rUS)
	require.Equal(t, "MISS", respHeader(rUS, header.XCache))

	rGB := testCtx("GET", "http://example.com/doc")
	rGB.Request.Header.Set(header.AcceptLanguage, "en-GB")
	serveRequest(h, rGB)
	require.Equal(t, "MISS", respHeader(rGB, header.XCache),
		"en-US and en-GB must remain distinct variants in layer 1")
	require.Equal(t, 2, originCalls)
}

// BenchmarkGate_VaryKey_AcceptLanguageBucket gates the AL bucketing
// on the variant-key hot path. Every iteration pays langBucket over a
// realistic q-cascade inside VariantKey (variantKeyCore). The bucket
// replaces the legacy sort path (3-4 allocs on multi-token values) —
// budget 2 matches the AE sibling (the winner substring lowercasing
// plus the vary-field lookup).
func BenchmarkGate_VaryKey_AcceptLanguageBucket(b *testing.B) {
	primary := BuildKeyFromURL("http://example.com/page", nil)
	hm := headerMap(header.AcceptLanguage, "fr-FR,fr;q=0.9,en;q=0.8,en-US;q=0.7")

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = VariantKey(primary, "Accept-Language", hm, nil)
	}
}
