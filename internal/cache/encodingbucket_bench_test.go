package cache

import (
	"testing"

	"github.com/bouine-cache/bouine/pkg/header"
)

// BenchmarkGate_VaryKey_AcceptEncodingBucket gates the AE-bucketing
// dispatch on the variant-key hot path. Every iteration pays
// encodingBucket over a realistic browser Accept-Encoding value inside
// VariantKey (variantKeyCore). The bucket must be computed with zero
// allocations — it replaces the sort path (normaliseListHeader), which
// allocated for multi-token values, so budget 0 is both the gate and
// an improvement over the pre-bucketing baseline.
func BenchmarkGate_VaryKey_AcceptEncodingBucket(b *testing.B) {
	primary := BuildKeyFromURL("http://example.com/asset.js", nil)
	hm := headerMap(header.AcceptEncoding, "gzip, deflate, br, zstd")

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = VariantKey(primary, "Accept-Encoding", hm, nil)
	}
}

// BenchmarkGate_VaryKey_AcceptEncodingBucketVerbatim is the paired
// verbatim-policy benchmark: the dispatch must skip bucketing and fall
// through to the legacy normalization with no added cost beyond the
// policy check.
func BenchmarkGate_VaryKey_AcceptEncodingBucketVerbatim(b *testing.B) {
	primary := BuildKeyFromURL("http://example.com/asset.js", nil)
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, nil)
	policy.SetVerbatimAE(true)
	hm := headerMap(header.AcceptEncoding, "gzip, deflate, br, zstd")

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = VariantKey(primary, "Accept-Encoding", hm, policy)
	}
}
