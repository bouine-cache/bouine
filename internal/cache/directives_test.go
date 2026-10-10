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
		h.Set(header.Expires, "Mon, 01 Jan 2024 01:00:00 GMT, Mon, 01 Jan 2024 02:00:00 GMT")
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

func TestParseCacheControl_QuotedValue(t *testing.T) {
	t.Parallel()
	d := ParseCacheControl(`no-cache="Set-Cookie, Content-Encoding"`)
	assert.Equal(t, "Set-Cookie, Content-Encoding", d.NoCacheFields)
}
