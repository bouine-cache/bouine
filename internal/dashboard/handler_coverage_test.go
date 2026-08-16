package dashboard

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/observability"
)

func TestSortRouteStats(t *testing.T) {
	t.Parallel()
	stats := []observability.RouteStat{
		{Route: "b", Requests: 10},
		{Route: "a", Requests: 50},
		{Route: "c", Requests: 30},
	}
	sorted := sortRouteStats(stats)
	require.Equal(t, 3, len(sorted))
	assert.Equal(t, "a", sorted[0].Route)
	assert.Equal(t, "c", sorted[1].Route)
	assert.Equal(t, "b", sorted[2].Route)
}

func TestSortRouteStats_Empty(t *testing.T) {
	t.Parallel()
	sorted := sortRouteStats(nil)
	assert.Empty(t, sorted)
}

func TestSortURLStats(t *testing.T) {
	t.Parallel()
	stats := []observability.URLStat{
		{URL: "/b", Requests: 5},
		{URL: "/a", Requests: 20},
	}
	sorted := sortURLStats(stats)
	require.Equal(t, 2, len(sorted))
	assert.Equal(t, "/a", sorted[0].URL)
	assert.Equal(t, "/b", sorted[1].URL)
}

func TestSortURLStats_Empty(t *testing.T) {
	t.Parallel()
	sorted := sortURLStats(nil)
	assert.Empty(t, sorted)
}

func TestApdexScore(t *testing.T) {
	t.Parallel()
	t.Run("zero_total", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 0.0, apdexScore(observability.LatencyHistogram{}, 0))
	})
	t.Run("all_satisfied", func(t *testing.T) {
		t.Parallel()
		h := observability.LatencyHistogram{100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		score := apdexScore(h, 100)
		assert.Equal(t, 1.0, score)
	})
	t.Run("all_tolerating", func(t *testing.T) {
		t.Parallel()
		h := observability.LatencyHistogram{0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0}
		score := apdexScore(h, 100)
		assert.Equal(t, 0.5, score)
	})
	t.Run("mixed", func(t *testing.T) {
		t.Parallel()
		h := observability.LatencyHistogram{50, 0, 0, 0, 0, 0, 0, 50, 0, 0, 0}
		score := apdexScore(h, 100)
		assert.Equal(t, 0.75, score)
	})
}

func TestSLOBuckets(t *testing.T) {
	t.Parallel()
	t.Run("zero_total", func(t *testing.T) {
		t.Parallel()
		buckets := sloBuckets(observability.LatencyHistogram{}, 0)
		require.Equal(t, 3, len(buckets))
		for _, b := range buckets {
			assert.Equal(t, 0.0, b.Pct)
		}
	})
	t.Run("all_under_10ms", func(t *testing.T) {
		t.Parallel()
		h := observability.LatencyHistogram{100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		buckets := sloBuckets(h, 100)
		require.Equal(t, 3, len(buckets))
		assert.Equal(t, 100.0, buckets[0].Pct) // 10ms
		assert.Equal(t, 100.0, buckets[1].Pct) // 100ms
		assert.Equal(t, 100.0, buckets[2].Pct) // 1s
	})
	t.Run("some_over_10ms", func(t *testing.T) {
		t.Parallel()
		h := observability.LatencyHistogram{30, 0, 0, 0, 0, 0, 0, 70, 0, 0, 0}
		buckets := sloBuckets(h, 100)
		assert.Equal(t, 30.0, buckets[0].Pct)  // 10ms
		assert.Equal(t, 30.0, buckets[1].Pct)  // 100ms
		assert.Equal(t, 100.0, buckets[2].Pct) // 1s
	})
}

func TestValidateCacheURL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		url  string
		want string
	}{
		{"empty", "", "URL is required"},
		{"invalid", "ht\ttp://invalid", "invalid URL"},
		{"wrong_scheme", "ftp://example.com", "URL must begin with http:// or https://"},
		{"no_host", "http:///path", "URL must include a host"},
		{"valid_http", "http://example.com/path", ""},
		{"valid_https", "https://example.com/path", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := validateCacheURL(tt.url)
			if tt.want == "" {
				assert.Empty(t, got)
			} else {
				assert.Contains(t, got, tt.want)
			}
		})
	}
}

func TestValidateRegex(t *testing.T) {
	t.Parallel()
	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "", validateRegex("field", ""))
	})
	t.Run("valid", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "", validateRegex("field", "^/api/.*$"))
	})
	t.Run("invalid", func(t *testing.T) {
		t.Parallel()
		result := validateRegex("field", "[invalid")
		assert.Contains(t, result, "field")
		assert.Contains(t, result, "regex")
	})
}

func TestEncodeJSON(t *testing.T) {
	t.Parallel()
	data := map[string]int{"a": 1, "b": 2}
	b, err := EncodeJSON(data)
	require.NoError(t, err)
	assert.NotEmpty(t, b)
}

func TestEncodeJSON_Nil(t *testing.T) {
	t.Parallel()
	b, err := EncodeJSON(nil)
	require.NoError(t, err)
	assert.Equal(t, "null\n", string(b))
}
