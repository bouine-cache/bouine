package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEncodingBucket covers the Accept-Encoding bucketing rules
// (docs/plans/accept-encoding-bucketing.md §3.1): the bucket is the
// negotiation outcome among {br, zstd, gzip}, not the token set.
// deflate, identity, and unknown tokens never contribute. Equal
// weights tie-break br > zstd > gzip — the sharing-maximizing order
// (see aeRank).
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
		{"br wins over zstd on tie", "gzip, deflate, br, zstd", "br"},
		{"zstd bare", "zstd", "zstd"},
		{"zstd after br", "br, zstd", "br"},
		{"br with zstd and identity suffix", "gzip, deflate, br, zstd, identity", "br"},
		{"gzip br pair", "gzip, br", "br"},
		{"br gzip pair swapped", "br, gzip", "br"},
		{"q excludes gzip", "gzip;q=0, deflate", "identity"},
		{"q excludes br", "br;q=0, gzip", "gzip"},
		{"q excludes zstd", "zstd;q=0, br", "br"},
		{"higher q wins", "br;q=0.5, gzip;q=1.0", "gzip"},
		{"higher q zstd over br", "br;q=0.5, zstd;q=1", "zstd"},
		{"equal q tie by rank", "zstd;q=1, br;q=1", "br"},
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
// motivation: every realistic browser Accept-Encoding that can accept
// br collapses to the same bucket, so one stored variant serves them
// all — including the zstd-spelling dialect, because equal weights
// prefer br (the sharing-maximizing tie-break; see aeRank).
// Pre-bucketing these five spellings were five distinct keys.
func TestEncodingBucket_AllBrowserDialectsOneVariant(t *testing.T) {
	t.Parallel()
	brDialects := []string{
		"gzip, deflate, br",
		"br, gzip, deflate",
		"deflate, gzip, br",
		"gzip, br",
		"gzip, deflate, br, zstd, identity",
	}
	seen := make(map[string]int)
	for _, d := range brDialects {
		seen[encodingBucket(d)]++
	}
	require.Equal(t, map[string]int{"br": 5}, seen,
		"all br-capable browser dialects must share one bucket, got %v", seen)
}
