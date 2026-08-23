package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/header"
)

func TestFreshnessLifetime(t *testing.T) {
	t.Parallel()
	t.Run("cdn_cc_max_age", func(t *testing.T) {
		t.Parallel()
		getHdr := func(key string) string {
			if key == header.CDNCacheControl {
				return "max-age=120"
			}
			return ""
		}
		d, ok := FreshnessLifetime(Directives{}, getHdr)
		require.True(t, ok)
		assert.Equal(t, 120*time.Second, d)
	})
	t.Run("cdn_cc_no_store", func(t *testing.T) {
		t.Parallel()
		getHdr := func(key string) string {
			if key == header.CDNCacheControl {
				return "no-store"
			}
			return ""
		}
		d, ok := FreshnessLifetime(Directives{}, getHdr)
		require.True(t, ok)
		assert.Equal(t, time.Duration(0), d)
	})
	t.Run("cdn_cc_no_ttl", func(t *testing.T) {
		t.Parallel()
		getHdr := func(key string) string {
			if key == header.CDNCacheControl {
				return "public"
			}
			return ""
		}
		d, ok := FreshnessLifetime(Directives{}, getHdr)
		require.True(t, ok)
		assert.Equal(t, time.Duration(0), d)
	})
	t.Run("s_maxage", func(t *testing.T) {
		t.Parallel()
		respCC := Directives{SMaxAgeSet: true, SMaxAge: 30 * time.Second}
		d, ok := FreshnessLifetime(respCC, func(string) string { return "" })
		require.True(t, ok)
		assert.Equal(t, 30*time.Second, d)
	})
	t.Run("max_age", func(t *testing.T) {
		t.Parallel()
		respCC := Directives{MaxAgeSet: true, MaxAge: 60 * time.Second}
		d, ok := FreshnessLifetime(respCC, func(string) string { return "" })
		require.True(t, ok)
		assert.Equal(t, 60*time.Second, d)
	})
	t.Run("valid_expires", func(t *testing.T) {
		t.Parallel()
		getHdr := func(key string) string {
			switch key {
			case header.Expires:
				return "Mon, 01 Jan 2024 01:00:00 GMT"
			case header.Date:
				return "Mon, 01 Jan 2024 00:00:00 GMT"
			}
			return ""
		}
		d, ok := FreshnessLifetime(Directives{}, getHdr)
		require.True(t, ok)
		assert.Equal(t, time.Hour, d)
	})
	t.Run("invalid_expires", func(t *testing.T) {
		t.Parallel()
		getHdr := func(key string) string {
			switch key {
			case header.Expires:
				return "garbage"
			case header.Date:
				return "Mon, 01 Jan 2024 00:00:00 GMT"
			}
			return ""
		}
		_, ok := FreshnessLifetime(Directives{}, getHdr)
		require.False(t, ok)
	})
	t.Run("missing_date", func(t *testing.T) {
		t.Parallel()
		getHdr := func(key string) string {
			if key == header.Expires {
				return "Mon, 01 Jan 2024 01:00:00 GMT"
			}
			return ""
		}
		_, ok := FreshnessLifetime(Directives{}, getHdr)
		require.False(t, ok)
	})
	t.Run("no_freshness", func(t *testing.T) {
		t.Parallel()
		_, ok := FreshnessLifetime(Directives{}, func(string) string { return "" })
		require.False(t, ok)
	})
}

func TestFreshnessLifetimeH(t *testing.T) {
	t.Parallel()
	t.Run("cdn_cc_max_age", func(t *testing.T) {
		t.Parallel()
		h := header.Map{}
		h.Set(header.CDNCacheControl, "max-age=120")
		d, ok := FreshnessLifetimeH(Directives{}, h)
		require.True(t, ok)
		assert.Equal(t, 120*time.Second, d)
	})
	t.Run("cdn_cc_no_store", func(t *testing.T) {
		t.Parallel()
		h := header.Map{}
		h.Set(header.CDNCacheControl, "no-store")
		d, ok := FreshnessLifetimeH(Directives{}, h)
		require.True(t, ok)
		assert.Equal(t, time.Duration(0), d)
	})
	t.Run("s_maxage", func(t *testing.T) {
		t.Parallel()
		respCC := Directives{SMaxAgeSet: true, SMaxAge: 30 * time.Second}
		d, ok := FreshnessLifetimeH(respCC, header.Map{})
		require.True(t, ok)
		assert.Equal(t, 30*time.Second, d)
	})
	t.Run("multiple_expires_rejected", func(t *testing.T) {
		t.Parallel()
		h := header.Map{}
		h.SetValues(header.Expires, []string{"Mon, 01 Jan 2024 01:00:00 GMT", "Mon, 01 Jan 2024 02:00:00 GMT"})
		_, ok := FreshnessLifetimeH(Directives{}, h)
		require.False(t, ok)
	})
	t.Run("missing_date_uses_now", func(t *testing.T) {
		t.Parallel()
		h := headerMap(header.Expires, "Mon, 01 Jan 2024 00:00:00 GMT")
		d, ok := FreshnessLifetimeH(Directives{}, h)
		require.True(t, ok)
		// Should be negative since Expires is in the past relative to now.
		assert.True(t, d < 0 || d == 0)
	})
	t.Run("invalid_expires", func(t *testing.T) {
		t.Parallel()
		h := headerMap(header.Expires, "garbage")
		h.Set(header.Date, "Mon, 01 Jan 2024 00:00:00 GMT")
		_, ok := FreshnessLifetimeH(Directives{}, h)
		require.False(t, ok)
	})
}

func TestApplyBoolDirective(t *testing.T) {
	t.Parallel()
	tests := []struct {
		key   string
		check func(Directives) bool
	}{
		{"no-store", func(d Directives) bool { return d.NoStore }},
		{"no-cache", func(d Directives) bool { return d.NoCache }},
		{"private", func(d Directives) bool { return d.Private }},
		{"public", func(d Directives) bool { return d.Public }},
		{"must-revalidate", func(d Directives) bool { return d.MustRevalidate }},
		{"proxy-revalidate", func(d Directives) bool { return d.ProxyRevalidate }},
		{"immutable", func(d Directives) bool { return d.Immutable }},
		{"no-transform", func(d Directives) bool { return d.NoTransform }},
		{"only-if-cached", func(d Directives) bool { return d.OnlyIfCached }},
		{"must-understand", func(d Directives) bool { return d.MustUnderstand }},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			t.Parallel()
			var d Directives
			applyBoolDirective(&d, tt.key)
			assert.True(t, tt.check(d))
		})
	}
	t.Run("unknown_returns_false", func(t *testing.T) {
		t.Parallel()
		var d Directives
		assert.False(t, applyBoolDirective(&d, "unknown"))
	})
}

func TestApplyDurDirective(t *testing.T) {
	t.Parallel()
	t.Run("s_maxage", func(t *testing.T) {
		t.Parallel()
		var d Directives
		applyDurDirective(&d, "s-maxage", "30")
		assert.True(t, d.SMaxAgeSet)
		assert.Equal(t, 30*time.Second, d.SMaxAge)
	})
	t.Run("min_fresh", func(t *testing.T) {
		t.Parallel()
		var d Directives
		applyDurDirective(&d, "min-fresh", "15")
		assert.True(t, d.MinFreshSet)
		assert.Equal(t, 15*time.Second, d.MinFresh)
	})
	t.Run("stale_if_error", func(t *testing.T) {
		t.Parallel()
		var d Directives
		applyDurDirective(&d, "stale-if-error", "60")
		assert.True(t, d.StaleIfErrorSet)
		assert.Equal(t, 60*time.Second, d.StaleIfError)
	})
	t.Run("max_stale_no_value", func(t *testing.T) {
		t.Parallel()
		var d Directives
		applyDurDirective(&d, "max-stale", "")
		assert.True(t, d.MaxStaleSet)
		assert.True(t, d.MaxStale > 0)
	})
	t.Run("max_stale_with_value", func(t *testing.T) {
		t.Parallel()
		var d Directives
		applyDurDirective(&d, "max-stale", "100")
		assert.True(t, d.MaxStaleSet)
		assert.Equal(t, 100*time.Second, d.MaxStale)
	})
}

func TestParseDur(t *testing.T) {
	t.Parallel()
	t.Run("largest_among_duplicates_wins", func(t *testing.T) {
		t.Parallel()
		var dur time.Duration
		var set bool
		parseDur(&dur, &set, "30")
		parseDur(&dur, &set, "60")
		assert.True(t, set)
		assert.Equal(t, 60*time.Second, dur)
	})
	t.Run("non_numeric_ignored", func(t *testing.T) {
		t.Parallel()
		var dur time.Duration
		var set bool
		parseDur(&dur, &set, "abc")
		assert.False(t, set)
	})
}

func TestParseIntNoAlloc(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want int64
		ok   bool
	}{
		{"empty", "", 0, false},
		{"trailing_garbage", "100a", 100, true},
		{"float_truncated", "3600.0", 3600, true},
		{"pure_non_numeric", "abc", 0, false},
		{"normal", "60", 60, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			n, ok := parseIntNoAlloc(tt.in)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.want, n)
		})
	}
}

func TestEqFold(t *testing.T) {
	t.Parallel()
	t.Run("different_lengths", func(t *testing.T) {
		t.Parallel()
		assert.False(t, eqFold("abc", "ab"))
	})
	t.Run("case_insensitive_match", func(t *testing.T) {
		t.Parallel()
		assert.True(t, eqFold("Max-Age", "max-age"))
	})
	t.Run("mismatch", func(t *testing.T) {
		t.Parallel()
		assert.False(t, eqFold("max-age", "max-stale"))
	})
}

func TestApplyDirective_NoCacheFields(t *testing.T) {
	t.Parallel()
	var d Directives
	applyDirective(&d, "no-cache", "Set-Cookie, Content-Encoding")
	assert.Equal(t, "Set-Cookie, Content-Encoding", d.NoCacheFields)
	// no-cache with value should NOT set NoCache bool.
	assert.False(t, d.NoCache)
}

func TestParseCacheControl_QuotedValue(t *testing.T) {
	t.Parallel()
	d := ParseCacheControl(`no-cache="Set-Cookie, Content-Encoding"`)
	assert.Equal(t, "Set-Cookie, Content-Encoding", d.NoCacheFields)
}
