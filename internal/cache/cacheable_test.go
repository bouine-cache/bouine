package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/header"
)

func TestIsCDNCCCharForbidden(t *testing.T) {
	t.Parallel()
	forbidden := []byte{'&', '@', '[', ']', '{', '}', '"'}
	for _, b := range forbidden {
		assert.True(t, isCDNCCCharForbidden(b), "byte %q should be forbidden", b)
	}
	safe := []byte{'a', '0', '-', '.', '/', ':', ',', ' ', '='}
	for _, b := range safe {
		assert.False(t, isCDNCCCharForbidden(b), "byte %q should be allowed", b)
	}
}

func TestHasMeaningfulCDNCCDirective(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		d    Directives
		want bool
	}{
		{"empty", Directives{}, false},
		{"max_age", Directives{MaxAgeSet: true}, true},
		{"s_maxage", Directives{SMaxAgeSet: true}, true},
		{"no_store", Directives{NoStore: true}, true},
		{"private", Directives{Private: true}, true},
		{"no_cache", Directives{NoCache: true}, true},
		{"other_only", Directives{Public: true, MustRevalidate: true}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, hasMeaningfulCDNCCDirective(tt.d))
		})
	}
}

func TestCDNCacheControl(t *testing.T) {
	t.Parallel()
	t.Run("absent", func(t *testing.T) {
		t.Parallel()
		_, ok := cdnCacheControl(header.Map{})
		require.False(t, ok)
	})
	t.Run("forbidden_char", func(t *testing.T) {
		t.Parallel()
		h := header.Map{}
		h.Set(header.CDNCacheControl, "max-age=60&foo")
		_, ok := cdnCacheControl(h)
		require.False(t, ok)
	})
	t.Run("no_meaningful_directive", func(t *testing.T) {
		t.Parallel()
		h := header.Map{}
		h.Set(header.CDNCacheControl, "public")
		_, ok := cdnCacheControl(h)
		require.False(t, ok)
	})
	t.Run("valid_max_age", func(t *testing.T) {
		t.Parallel()
		h := header.Map{}
		h.Set(header.CDNCacheControl, "max-age=60")
		d, ok := cdnCacheControl(h)
		require.True(t, ok)
		assert.True(t, d.MaxAgeSet)
	})
	t.Run("valid_s_maxage", func(t *testing.T) {
		t.Parallel()
		h := header.Map{}
		h.Set(header.CDNCacheControl, "s-maxage=30")
		d, ok := cdnCacheControl(h)
		require.True(t, ok)
		assert.True(t, d.SMaxAgeSet)
	})
}

func TestIsUnderstoodStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		status int
		want   bool
	}{
		{200, true}, {203, true}, {204, true}, {206, true},
		{300, true}, {301, true}, {302, true}, {303, true},
		{304, true}, {307, true}, {308, true},
		{400, true}, {401, true}, {403, true}, {404, true},
		{405, true}, {406, true}, {408, true}, {409, true},
		{410, true}, {411, true}, {412, true}, {413, true},
		{414, true}, {415, true}, {416, true},
		{500, true}, {501, true}, {502, true}, {503, true}, {504, true},
		{599, false}, {100, false}, {700, false}, {0, false},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, isUnderstoodStatus(tt.status), "status %d", tt.status)
	}
}

func TestNewParsedResponse(t *testing.T) {
	t.Parallel()
	t.Run("with_cdn_cc", func(t *testing.T) {
		t.Parallel()
		h := header.Map{}
		h.Set(header.CDNCacheControl, "max-age=60")
		p := newParsedResponse(200, header.Map{}, h)
		assert.True(t, p.hasCDN)
		assert.True(t, p.respCC.MaxAgeSet)
	})
	t.Run("without_cdn_cc", func(t *testing.T) {
		t.Parallel()
		h := headerMap(header.CacheControl, "max-age=30")
		p := newParsedResponse(200, header.Map{}, h)
		assert.False(t, p.hasCDN)
		assert.True(t, p.respCC.MaxAgeSet)
	})
}

func TestParsedResponse_IsCacheable(t *testing.T) {
	t.Parallel()
	t.Run("no_store_blocks", func(t *testing.T) {
		t.Parallel()
		h := headerMap(header.CacheControl, "no-store")
		p := newParsedResponse(200, header.Map{}, h)
		require.False(t, p.isCacheable(0))
	})
	t.Run("no_store_with_must_understand_understood", func(t *testing.T) {
		t.Parallel()
		h := headerMap(header.CacheControl, "no-store, must-understand, max-age=60")
		p := newParsedResponse(200, header.Map{}, h)
		require.True(t, p.isCacheable(0))
	})
	t.Run("no_store_with_must_understand_not_understood", func(t *testing.T) {
		t.Parallel()
		h := headerMap(header.CacheControl, "no-store, must-understand, max-age=60")
		p := newParsedResponse(599, header.Map{}, h)
		require.False(t, p.isCacheable(0))
	})
	t.Run("private_blocks", func(t *testing.T) {
		t.Parallel()
		h := headerMap(header.CacheControl, "private, max-age=60")
		p := newParsedResponse(200, header.Map{}, h)
		require.False(t, p.isCacheable(0))
	})
	t.Run("explicit_max_age", func(t *testing.T) {
		t.Parallel()
		h := headerMap(header.CacheControl, "max-age=60")
		p := newParsedResponse(200, header.Map{}, h)
		require.True(t, p.isCacheable(0))
	})
	t.Run("heuristic_with_last_modified", func(t *testing.T) {
		t.Parallel()
		h := headerMap(header.LastModified, "Mon, 01 Jan 2024 00:00:00 GMT")
		p := newParsedResponse(301, header.Map{}, h)
		require.True(t, p.isCacheable(0))
	})
	t.Run("negative_cacheable", func(t *testing.T) {
		t.Parallel()
		h := header.Map{}
		p := newParsedResponse(404, header.Map{}, h)
		require.True(t, p.isCacheable(30))
	})
}

func TestParsedResponse_IsCacheableWithDefault(t *testing.T) {
	t.Parallel()
	t.Run("default_ttl_for_heuristic_status", func(t *testing.T) {
		t.Parallel()
		h := header.Map{}
		p := newParsedResponse(200, header.Map{}, h)
		require.True(t, p.isCacheableWithDefault(0, 60))
	})
	t.Run("default_ttl_zero_blocks", func(t *testing.T) {
		t.Parallel()
		h := header.Map{}
		p := newParsedResponse(200, header.Map{}, h)
		require.False(t, p.isCacheableWithDefault(0, 0))
	})
	t.Run("no_store_blocks_default", func(t *testing.T) {
		t.Parallel()
		h := headerMap(header.CacheControl, "no-store")
		p := newParsedResponse(200, header.Map{}, h)
		require.False(t, p.isCacheableWithDefault(0, 60))
	})
	t.Run("non_heuristic_status_blocks_default", func(t *testing.T) {
		t.Parallel()
		h := header.Map{}
		p := newParsedResponse(502, header.Map{}, h)
		require.False(t, p.isCacheableWithDefault(0, 60))
	})
}
