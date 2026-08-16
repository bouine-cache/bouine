package cache

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestClientConditionalMatch(t *testing.T) {
	t.Parallel()
	t.Run("if_none_match_match", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set(header.IfNoneMatch, `"abc"`)
		obj := &api.Object{ETag: `"abc"`}
		require.True(t, ClientConditionalMatch(r, obj))
	})
	t.Run("if_none_match_mismatch", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set(header.IfNoneMatch, `"xyz"`)
		obj := &api.Object{ETag: `"abc"`}
		require.False(t, ClientConditionalMatch(r, obj))
	})
	t.Run("if_none_match_no_etag", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set(header.IfNoneMatch, `"abc"`)
		obj := &api.Object{}
		require.False(t, ClientConditionalMatch(r, obj))
	})
	t.Run("if_modified_since_last_modified_before", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set(header.IfModifiedSince, "Mon, 01 Jan 2024 00:00:00 GMT")
		obj := &api.Object{LastModified: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)}
		require.True(t, ClientConditionalMatch(r, obj))
	})
	t.Run("if_modified_since_last_modified_after", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set(header.IfModifiedSince, "Mon, 01 Jan 2023 00:00:00 GMT")
		obj := &api.Object{LastModified: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
		require.False(t, ClientConditionalMatch(r, obj))
	})
	t.Run("if_modified_since_date_fallback", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set(header.IfModifiedSince, "Mon, 01 Jan 2024 00:00:00 GMT")
		obj := &api.Object{
			Header: header.FromHTTP(http.Header{header.Date: {"Mon, 01 Jan 2023 00:00:00 GMT"}}),
		}
		require.True(t, ClientConditionalMatch(r, obj))
	})
	t.Run("if_modified_since_invalid_date", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set(header.IfModifiedSince, "garbage")
		obj := &api.Object{LastModified: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)}
		require.False(t, ClientConditionalMatch(r, obj))
	})
	t.Run("no_conditional_headers", func(t *testing.T) {
		t.Parallel()
		r := httptest.NewRequest("GET", "/", nil)
		obj := &api.Object{ETag: `"abc"`}
		require.False(t, ClientConditionalMatch(r, obj))
	})
}

func TestEtagMatch(t *testing.T) {
	t.Parallel()
	t.Run("wildcard", func(t *testing.T) {
		t.Parallel()
		require.True(t, etagMatch("*", `"abc"`))
	})
	t.Run("weak_match", func(t *testing.T) {
		t.Parallel()
		require.True(t, etagMatch(`W/"abc"`, `"abc"`))
	})
	t.Run("exact_match", func(t *testing.T) {
		t.Parallel()
		require.True(t, etagMatch(`"abc"`, `"abc"`))
	})
	t.Run("mismatch", func(t *testing.T) {
		t.Parallel()
		require.False(t, etagMatch(`"abc"`, `"xyz"`))
	})
	t.Run("multi_tag_list", func(t *testing.T) {
		t.Parallel()
		require.True(t, etagMatch(`"abc", "xyz"`, `"xyz"`))
	})
}

func TestMergeHeaders304(t *testing.T) {
	t.Parallel()
	t.Run("skips_content_headers", func(t *testing.T) {
		t.Parallel()
		stored := &api.Object{
			Header: header.FromHTTP(http.Header{
				header.ContentLength: {"100"},
				header.ContentType:   {"text/html"},
			}),
		}
		resp304 := http.Header{
			header.ContentLength:    {"200"},
			header.ContentType:      {"application/json"},
			header.ContentEncoding:  {"gzip"},
			header.SetCookie:        {"new=cookie"},
			header.TransferEncoding: {"chunked"},
			header.ETag:             {`"v2"`},
		}
		MergeHeaders304(stored, resp304)
		// Content-specific headers must NOT be overwritten.
		assert.Equal(t, "100", stored.Header.Get(header.ContentLength))
		// ContentType is NOT in the skip list, so it IS updated.
		assert.Equal(t, "application/json", stored.Header.Get(header.ContentType))
		// Other headers ARE updated.
		assert.Equal(t, `"v2"`, stored.Header.Get(header.ETag))
	})
	t.Run("updates_non_content_headers", func(t *testing.T) {
		t.Parallel()
		stored := &api.Object{
			Header: header.FromHTTP(http.Header{
				header.CacheControl: {"max-age=60"},
			}),
		}
		resp304 := http.Header{
			header.CacheControl: {"max-age=120"},
			header.LastModified: {"Mon, 01 Jan 2024 00:00:00 GMT"},
		}
		MergeHeaders304(stored, resp304)
		assert.Equal(t, "max-age=120", stored.Header.Get(header.CacheControl))
		assert.Equal(t, "Mon, 01 Jan 2024 00:00:00 GMT", stored.Header.Get(header.LastModified))
	})
}

func TestQuoteETag(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"already_quoted", `"abc"`, `"abc"`},
		{"weak_prefix", `W/"abc"`, `W/"abc"`},
		{"unquoted", "abc", `"abc"`},
		{"weak_lowercase", `w/"abc"`, `w/"abc"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, quoteETag(tt.in))
		})
	}
}
