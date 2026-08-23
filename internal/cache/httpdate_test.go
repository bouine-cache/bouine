package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// TestComputeAge_PrefersStoredOriginAge proves ComputeAge uses the pre-parsed
// obj.OriginAge field rather than re-parsing the Age header on every hit. The
// stored field and the header are set to DIFFERENT values so the stored field
// must win; if ComputeAge fell back to parsing the header it would return 5s.
func TestComputeAge_PrefersStoredOriginAge(t *testing.T) {
	t.Parallel()
	now := time.Now()
	obj := &api.Object{
		StoredAt:  now, // elapsed-since-store contributes 0
		OriginAge: 30 * time.Second,
		Header:    headerMap(header.Age, "5"),
	}
	got := ComputeAge(obj, now)
	require.Equal(t, 30*time.Second, got)
}

// TestComputeAge_FallsBackToHeader covers the warm-tier / legacy path where the
// transient OriginAge field is zero: ComputeAge must still recover the origin
// age by parsing the header, keeping the value identical to the old behaviour.
func TestComputeAge_FallsBackToHeader(t *testing.T) {
	t.Parallel()
	now := time.Now()
	obj := &api.Object{
		StoredAt: now,
		Header:   headerMap(header.Age, "42"),
	}
	got := ComputeAge(obj, now)
	require.Equal(t, 42*time.Second, got)
}

func TestHeuristicTTL(t *testing.T) {
	t.Parallel()
	t.Run("no_last_modified", func(t *testing.T) {
		t.Parallel()
		h := header.Map{}
		require.Equal(t, time.Duration(0), HeuristicTTL(h, time.Now()))
	})
	t.Run("invalid_last_modified", func(t *testing.T) {
		t.Parallel()
		h := headerMap(header.LastModified, "garbage")
		require.Equal(t, time.Duration(0), HeuristicTTL(h, time.Now()))
	})
	t.Run("with_date_header", func(t *testing.T) {
		t.Parallel()
		h := headerMap(header.Date, "Mon, 01 Jan 2024 00:00:00 GMT")
		h.Set(header.LastModified, "Mon, 01 Jan 2023 00:00:00 GMT")
		got := HeuristicTTL(h, time.Now())
		// 365 days / 10 = 36.5 days = 876h
		require.Equal(t, 876*time.Hour, got)
	})
	t.Run("without_date_uses_now", func(t *testing.T) {
		t.Parallel()
		now := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
		h := headerMap(header.LastModified, "Mon, 01 Jan 2024 00:00:00 GMT")
		got := HeuristicTTL(h, now)
		// ~152 days / 10
		require.Greater(t, got, time.Duration(0))
		require.Less(t, got, 365*24*time.Hour)
	})
	t.Run("age_le_zero", func(t *testing.T) {
		t.Parallel()
		h := headerMap(header.Date, "Mon, 01 Jan 2023 00:00:00 GMT")
		h.Set(header.LastModified, "Mon, 01 Jan 2024 00:00:00 GMT")
		require.Equal(t, time.Duration(0), HeuristicTTL(h, time.Now()))
	})
}

func TestParseOriginAge(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		h    header.Map
		want time.Duration
	}{
		{"empty", header.Map{}, 0},
		{"normal", headerMap(header.Age, "3600"), 3600 * time.Second},
		{"float_rejected", headerMap(header.Age, "7200.0"), 0},
		{"non_numeric", headerMap(header.Age, "abc"), 0},
		{"negative", headerMap(header.Age, "-10"), 0},
		{"clamped", headerMap(header.Age, "9999999999"), 2147483648 * time.Second},
		{"trailing_garbage", headerMap(header.Age, "3600;foo"), 3600 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := parseOriginAge(tt.h)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMergeHeaderValues(t *testing.T) {
	t.Parallel()
	t.Run("single_value", func(t *testing.T) {
		t.Parallel()
		h := headerMap(header.CacheControl, "max-age=60")
		assert.Equal(t, "max-age=60", mergeHeaderValues(h, header.CacheControl))
	})
	t.Run("multiple_values", func(t *testing.T) {
		t.Parallel()
		h := header.Map{}
		h.SetValues(header.CacheControl, []string{"max-age=60", "public"})
		assert.Equal(t, "max-age=60, public", mergeHeaderValues(h, header.CacheControl))
	})
	t.Run("absent", func(t *testing.T) {
		t.Parallel()
		h := header.Map{}
		assert.Equal(t, "", mergeHeaderValues(h, header.CacheControl))
	})
}

func TestParseHTTPDate(t *testing.T) {
	t.Parallel()
	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		require.True(t, parseHTTPDate("").IsZero())
	})
	t.Run("rfc1123", func(t *testing.T) {
		t.Parallel()
		d := parseHTTPDate("Mon, 01 Jan 2024 00:00:00 GMT")
		require.False(t, d.IsZero())
		assert.Equal(t, 2024, d.Year())
	})
	t.Run("rfc850", func(t *testing.T) {
		t.Parallel()
		d := parseHTTPDate("Monday, 01-Jan-24 00:00:00 GMT")
		require.False(t, d.IsZero())
	})
	t.Run("asctime", func(t *testing.T) {
		t.Parallel()
		d := parseHTTPDate("Mon Jan  1 00:00:00 2024")
		require.False(t, d.IsZero())
	})
	t.Run("single_digit_day", func(t *testing.T) {
		t.Parallel()
		d := parseHTTPDate("Mon, 2 Jan 2024 00:00:00 GMT")
		require.False(t, d.IsZero())
	})
	t.Run("lowercase_gmt", func(t *testing.T) {
		t.Parallel()
		d := parseHTTPDate("Mon, 01 Jan 2024 00:00:00 gmt")
		require.False(t, d.IsZero())
	})
	t.Run("double_space_rejected_with_comma", func(t *testing.T) {
		t.Parallel()
		d := parseHTTPDate("Mon,  01 Jan 2024 00:00:00 GMT")
		require.True(t, d.IsZero())
	})
	t.Run("invalid", func(t *testing.T) {
		t.Parallel()
		require.True(t, parseHTTPDate("garbage").IsZero())
	})
}

func TestValidHTTPTimeField(t *testing.T) {
	t.Parallel()
	t.Run("two_digit_hour", func(t *testing.T) {
		t.Parallel()
		assert.True(t, validHTTPTimeField("Mon, 01 Jan 2024 00:00:00 GMT"))
	})
	t.Run("one_digit_hour_rejected", func(t *testing.T) {
		t.Parallel()
		assert.False(t, validHTTPTimeField("Mon, 01 Jan 2024 0:00:00 GMT"))
	})
	t.Run("no_colon", func(t *testing.T) {
		t.Parallel()
		assert.True(t, validHTTPTimeField("no time here"))
	})
}

func TestNormalizeTZ(t *testing.T) {
	t.Parallel()
	t.Run("gmt", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "Mon, 01 Jan 2024 00:00:00 GMT", normalizeTZ("Mon, 01 Jan 2024 00:00:00 gmt"))
	})
	t.Run("Gmt", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "Mon, 01 Jan 2024 00:00:00 GMT", normalizeTZ("Mon, 01 Jan 2024 00:00:00 Gmt"))
	})
	t.Run("non_gmt_unchanged", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "Mon, 01 Jan 2024 00:00:00 PST", normalizeTZ("Mon, 01 Jan 2024 00:00:00 PST"))
	})
	t.Run("too_short", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "ab", normalizeTZ("ab"))
	})
}
