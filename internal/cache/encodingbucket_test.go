package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEncodingBucket covers the Accept-Encoding bucketing rules
// (docs/plans/accept-encoding-bucketing.md §3.1): the bucket is the
// negotiation outcome among {zstd, br, gzip}, not the token set.
// deflate, identity, and unknown tokens never contribute.
func TestEncodingBucket(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", "identity"},
		{"whitespace only", "   ", "identity"},
		{"absent coding only", "deflate", "identity"},
		{"identity explicit", "identity", "identity"},
		{"gzip bare", "gzip", "gzip"},
		{"gzip case", "GZIP", "gzip"},
		{"gzip with deflate", "gzip, deflate", "gzip"},
		{"deflate then gzip", "deflate, gzip", "gzip"},
		{"br wins over gzip", "gzip, deflate, br", "br"},
		{"br order swap", "br, gzip, deflate", "br"},
		{"br bare", "br", "br"},
		{"zstd wins over br", "gzip, deflate, br, zstd", "zstd"},
		{"zstd bare", "zstd", "zstd"},
		{"zstd after br", "br, zstd", "zstd"},
		{"zstd with identity suffix", "gzip, deflate, br, zstd, identity", "zstd"},
		{"gzip br pair", "gzip, br", "br"},
		{"br gzip pair swapped", "br, gzip", "br"},
		{"q excludes gzip", "gzip;q=0, deflate", "identity"},
		{"q excludes br", "br;q=0, gzip", "gzip"},
		{"q excludes zstd", "zstd;q=0, br", "br"},
		{"higher q wins", "br;q=0.5, gzip;q=1.0", "gzip"},
		{"equal q tie by rank", "zstd;q=1, br;q=1", "zstd"},
		{"weight on deflate ignored", "deflate;q=1, gzip;q=0.5", "gzip"},
		{"spaces around tokens", "  gzip , br  ", "br"},
		{"space in weight", "gzip; q=0.5, br; q=1", "br"},
		{"malformed weight falls back to 1", "gzip;q=banana, br;q=0.5", "gzip"},
		{"malformed everything", "!!!", "identity"},
		{"stray separators", ", ,,", "identity"},
		{"unknown coding ignored", "lzma, gzip", "gzip"},
		{"identity q=0 still identity", "gzip;q=0, identity;q=0", "identity"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, encodingBucket(tc.in))
		})
	}
}

// TestEncodingBucket_AllBrowserDialectsOneVariant pins the hit-ratio
// motivation: every realistic browser Accept-Encoding that negotiates
// br collapses to the same bucket, so one stored variant serves them
// all. Before bucketing these produced five distinct keys (br, br+zstd,
// gzip-only was already separate, etc.).
func TestEncodingBucket_AllBrowserDialectsOneVariant(t *testing.T) {
	t.Parallel()
	brDialects := []string{
		"gzip, deflate, br",
		"br, gzip, deflate",
		"deflate, gzip, br",
		"gzip, br",
		"gzip, deflate, br, zstd, identity", // zstd outranks br
	}
	seen := make(map[string]int)
	for _, d := range brDialects {
		seen[encodingBucket(d)]++
	}
	// Two buckets across these dialects: br for the first four, zstd
	// for the zstd-capable one. The point is that the first four
	// collapse to one; pre-bucketing they were four distinct keys.
	require.Equal(t, map[string]int{"br": 4, "zstd": 1}, seen)
}
