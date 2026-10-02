package header

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// The tokenizer tests moved from internal/cache/directives_test.go when
// the parser moved here (ADR-0053). They test the tokenizer through its
// public functions exactly as production code uses them.

func TestParseBoolDirective(t *testing.T) {
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
			d := ParseCacheControl(tt.key)
			assert.True(t, tt.check(d))
		})
	}
	t.Run("unknown_directive_ignored", func(t *testing.T) {
		t.Parallel()
		d := ParseCacheControl("unknown-directive")
		assert.Equal(t, Directives{}, d, "an unrecognized directive must leave Directives zero")
	})
}

func TestParseDurationDirectives(t *testing.T) {
	t.Parallel()
	t.Run("s_maxage", func(t *testing.T) {
		t.Parallel()
		d := ParseCacheControl("s-maxage=30")
		assert.True(t, d.SMaxAgeSet)
		assert.Equal(t, 30*time.Second, d.SMaxAge)
	})
	t.Run("min_fresh", func(t *testing.T) {
		t.Parallel()
		d := ParseCacheControl("min-fresh=15")
		assert.True(t, d.MinFreshSet)
		assert.Equal(t, 15*time.Second, d.MinFresh)
	})
	t.Run("stale_if_error", func(t *testing.T) {
		t.Parallel()
		d := ParseCacheControl("stale-if-error=60")
		assert.True(t, d.StaleIfErrorSet)
		assert.Equal(t, 60*time.Second, d.StaleIfError)
	})
	t.Run("max_stale_no_value", func(t *testing.T) {
		t.Parallel()
		d := ParseCacheControl("max-stale")
		assert.True(t, d.MaxStaleSet)
		assert.True(t, d.MaxStale > 0)
	})
	t.Run("max_stale_with_value", func(t *testing.T) {
		t.Parallel()
		d := ParseCacheControl("max-stale=100")
		assert.True(t, d.MaxStaleSet)
		assert.Equal(t, 100*time.Second, d.MaxStale)
	})
	t.Run("largest_among_duplicate_max_age_wins", func(t *testing.T) {
		t.Parallel()
		d := ParseCacheControl("max-age=30, max-age=60")
		assert.True(t, d.MaxAgeSet)
		assert.Equal(t, 60*time.Second, d.MaxAge)
	})
	t.Run("non_numeric_ignored", func(t *testing.T) {
		t.Parallel()
		d := ParseCacheControl("max-age=abc")
		assert.False(t, d.MaxAgeSet)
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
			n, ok := ParseIntNoAlloc(tt.in)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.want, n)
		})
	}
}

func TestEqFold(t *testing.T) {
	t.Parallel()
	t.Run("different_lengths", func(t *testing.T) {
		t.Parallel()
		assert.False(t, EqFold("abc", "ab"))
	})
	t.Run("case_insensitive_match", func(t *testing.T) {
		t.Parallel()
		assert.True(t, EqFold("Max-Age", "max-age"))
	})
	t.Run("mismatch", func(t *testing.T) {
		t.Parallel()
		assert.False(t, EqFold("max-age", "max-stale"))
	})
}

func TestParseCacheControl_NoCacheFields(t *testing.T) {
	t.Parallel()
	d := ParseCacheControl(`no-cache="Set-Cookie, Content-Encoding"`)
	assert.Equal(t, "Set-Cookie, Content-Encoding", d.NoCacheFields)
	assert.False(t, d.NoCache, "no-cache with a value must not set the bare NoCache bool")
}

func TestParseCacheControlBytes_MatchesStringVariant(t *testing.T) {
	t.Parallel()
	in := `public, max-age=600, no-cache="A, B"`
	assert.Equal(t, ParseCacheControl(in), ParseCacheControlBytes([]byte(in)),
		"the []byte tokenizer must produce the same Directives as the string one")
}
