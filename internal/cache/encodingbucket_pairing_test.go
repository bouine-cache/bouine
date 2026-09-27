package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/pkg/header"
)

// TestVaryKeyEncodingBucket_ParityAcrossPaths pins the ADR-0046 contract
// for Accept-Encoding bucketing: every variant-key construction path must
// agree on the bucketed value, or one path stores and another resolves
// under different keys — the cross-variant body-swap bug class.
//
// Paths covered:
//   - VariantKey          (header.Map serving path, variantKeyCore)
//   - VariantKeyFast      (fasthttp Peek path, variantKeyCore)
//   - variantKeySlow      (>16 Vary fields, allocation fallback)
//   - BuildVaryKey        (peer-gate assertion hex, key.go)
func TestVaryKeyEncodingBucket_ParityAcrossPaths(t *testing.T) {
	t.Parallel()
	primary := BuildKeyFromURL("http://example.com/asset.js", nil)

	// Two request populations that negotiate the same best coding but
	// spell their Accept-Encoding differently. Pre-bucketing these were
	// distinct variants; they must collapse to one key on every path.
	dialects := []struct {
		name string
		ae   string
	}{
		{"full", "gzip, deflate, br"},
		{"swapped", "br, gzip, deflate"},
		{"sparse", "gzip, br"},
		{"zstd", "gzip, deflate, br, zstd"},
	}

	for _, d := range dialects {
		hm := headerMap(header.AcceptEncoding, d.ae)
		fh := &fasthttp.RequestHeader{}
		fh.Set(header.AcceptEncoding, d.ae)

		vkMap := VariantKey(primary, "Accept-Encoding", hm, nil)
		vkFast := VariantKeyFast(primary, "Accept-Encoding", fh, nil)
		vkSlow := variantKeySlow(primary, "accept-encoding", hm, nil)
		bvk := BuildVaryKey("Accept-Encoding", hm, nil)

		require.Equal(t, vkMap, vkFast, "map vs fasthttp path disagree for %s", d.name)
		require.Equal(t, vkMap, vkSlow, "map vs slow path disagree for %s", d.name)
		require.NotEmpty(t, bvk, "peer-gate hex empty for %s", d.name)
	}

	// All br-capable dialects — including the zstd spelling, since
	// equal weights tie-break br — collapse to one variant key.
	brKeys := map[string]bool{}
	for _, d := range dialects {
		hm := headerMap(header.AcceptEncoding, d.ae)
		brKeys[VariantKey(primary, "Accept-Encoding", hm, nil).String()] = true
	}
	require.Equal(t, 1, len(brKeys), "all br-capable dialects must share one bucket, got %v", brKeys)
}

// TestVaryKeyEncodingBucket_Verbatim restores the pre-bucketing key when
// encoding_policy: verbatim is set: the AE value keys on the
// lowercased+sorted raw string, so "gzip, deflate, br" and
// "br, gzip, deflate" collapse (order normalization) but "gzip, br"
// does not (different token set).
func TestVaryKeyEncodingBucket_Verbatim(t *testing.T) {
	t.Parallel()
	primary := BuildKeyFromURL("http://example.com/asset.js", nil)
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false)
	policy.SetVerbatimAE(true)

	same1 := VariantKey(primary, "Accept-Encoding", headerMap(header.AcceptEncoding, "gzip, deflate, br"), policy)
	same2 := VariantKey(primary, "Accept-Encoding", headerMap(header.AcceptEncoding, "br, gzip, deflate"), policy)
	other := VariantKey(primary, "Accept-Encoding", headerMap(header.AcceptEncoding, "gzip, br"), policy)
	require.Equal(t, same1, same2, "verbatim policy must keep order normalization")
	require.NotEqual(t, same1, other, "verbatim policy must not collapse distinct token sets")
}

// TestVaryKeyEncodingBucket_ExcludeHeadersWins pins precedence: an
// explicit exclude_headers: [accept-encoding] removes the header from
// the variant key entirely — bucketing must not resurrect it.
func TestVaryKeyEncodingBucket_ExcludeHeadersWins(t *testing.T) {
	t.Parallel()
	primary := BuildKeyFromURL("http://example.com/asset.js", nil)
	policy := NewKeyPolicy(nil, nil, map[string]bool{"accept-encoding": true}, nil, false, false, nil, false)

	withAE := VariantKey(primary, "Accept-Encoding", headerMap(header.AcceptEncoding, "gzip, deflate, br"), policy)
	withoutAE := VariantKey(primary, "Accept-Encoding", headerMap(header.AcceptEncoding, ""), policy)
	require.Equal(t, primary, withAE, "excluded header must drop the variant key to primary")
	require.Equal(t, primary, withoutAE)
}

// TestHandler_EncodingBucketPairing proves the §2.1 invariant end to end:
// the variant key claims a bucket, and the origin receives the canonical
// token for that bucket. Two clients with different AE spellings must
// share one origin fill, and the origin must see exactly "br".
func TestHandler_EncodingBucketPairing(t *testing.T) {
	t.Parallel()
	var originCalls int
	var seenAE []string
	upstream := func(ctx *fasthttp.RequestCtx) {
		originCalls++
		seenAE = append(seenAE, string(ctx.Request.Header.Peek(header.AcceptEncoding)))
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.Vary, "Accept-Encoding")
		ctx.Response.Header.Set(header.ContentEncoding, "br")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("compressed-body"))
	}
	h := testHandler(t, upstream)

	// Client A: classic Chrome dialect.
	rA := testCtx("GET", "http://example.com/asset")
	rA.Request.Header.Set(header.AcceptEncoding, "gzip, deflate, br")
	serveRequest(h, rA)
	require.Equal(t, "MISS", respHeader(rA, header.XCache))

	// Client B: different spelling of the same negotiation.
	rB := testCtx("GET", "http://example.com/asset")
	rB.Request.Header.Set(header.AcceptEncoding, "br, gzip")
	serveRequest(h, rB)
	require.Equal(t, "HIT", respHeader(rB, header.XCache), "br-negotiating dialects must share the bucket")
	require.Equal(t, "compressed-body", respBody(rB))

	require.Equal(t, 1, originCalls, "two AE dialects of one bucket must be one origin fill")
	require.Equal(t, []string{"br"}, seenAE, "origin must receive the canonical bucket token, not the raw client AE")
}

// TestHandler_EncodingBucketIdentity removes the header for the identity
// bucket: a request that cannot accept any compressed coding must not
// ask the origin for compression, and must key to the identity variant.
func TestHandler_EncodingBucketIdentity(t *testing.T) {
	t.Parallel()
	var originCalls int
	var sawAE bool
	upstream := func(ctx *fasthttp.RequestCtx) {
		originCalls++
		sawAE = len(ctx.Request.Header.Peek(header.AcceptEncoding)) > 0
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.Vary, "Accept-Encoding")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("plain-body"))
	}
	h := testHandler(t, upstream)

	r := testCtx("GET", "http://example.com/doc")
	r.Request.Header.Set(header.AcceptEncoding, "deflate") // deflate-only: no bucket coding acceptable
	serveRequest(h, r)
	require.Equal(t, "MISS", respHeader(r, header.XCache))
	require.False(t, sawAE, "identity bucket must not ask the origin for compression")

	// A request with no AE at all shares the identity variant.
	r2 := testCtx("GET", "http://example.com/doc")
	serveRequest(h, r2)
	require.Equal(t, "HIT", respHeader(r2, header.XCache), "absent AE and deflate-only both bucket to identity")
	require.Equal(t, 1, originCalls)
}
