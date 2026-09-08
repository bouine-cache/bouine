package cache

import (
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
		r := testCtx("GET", "http://example.com/")
		r.Request.Header.Set(header.IfNoneMatch, `"abc"`)
		obj := &api.Object{ETag: `"abc"`}
		require.True(t, ClientConditionalMatch(requestInfoFromCtx(r), obj))
	})
	t.Run("if_none_match_mismatch", func(t *testing.T) {
		t.Parallel()
		r := testCtx("GET", "http://example.com/")
		r.Request.Header.Set(header.IfNoneMatch, `"xyz"`)
		obj := &api.Object{ETag: `"abc"`}
		require.False(t, ClientConditionalMatch(requestInfoFromCtx(r), obj))
	})
	t.Run("if_none_match_no_etag", func(t *testing.T) {
		t.Parallel()
		r := testCtx("GET", "http://example.com/")
		r.Request.Header.Set(header.IfNoneMatch, `"abc"`)
		obj := &api.Object{}
		require.False(t, ClientConditionalMatch(requestInfoFromCtx(r), obj))
	})
	t.Run("if_modified_since_last_modified_before", func(t *testing.T) {
		t.Parallel()
		r := testCtx("GET", "http://example.com/")
		r.Request.Header.Set(header.IfModifiedSince, "Mon, 01 Jan 2024 00:00:00 GMT")
		obj := &api.Object{LastModified: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)}
		require.True(t, ClientConditionalMatch(requestInfoFromCtx(r), obj))
	})
	t.Run("if_modified_since_last_modified_after", func(t *testing.T) {
		t.Parallel()
		r := testCtx("GET", "http://example.com/")
		r.Request.Header.Set(header.IfModifiedSince, "Mon, 01 Jan 2023 00:00:00 GMT")
		obj := &api.Object{LastModified: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
		require.False(t, ClientConditionalMatch(requestInfoFromCtx(r), obj))
	})
	t.Run("if_modified_since_date_fallback", func(t *testing.T) {
		t.Parallel()
		r := testCtx("GET", "http://example.com/")
		r.Request.Header.Set(header.IfModifiedSince, "Mon, 01 Jan 2024 00:00:00 GMT")
		obj := &api.Object{
			Header: headerMap(header.Date, "Mon, 01 Jan 2023 00:00:00 GMT"),
		}
		require.True(t, ClientConditionalMatch(requestInfoFromCtx(r), obj))
	})
	t.Run("if_modified_since_invalid_date", func(t *testing.T) {
		t.Parallel()
		r := testCtx("GET", "http://example.com/")
		r.Request.Header.Set(header.IfModifiedSince, "garbage")
		obj := &api.Object{LastModified: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)}
		require.False(t, ClientConditionalMatch(requestInfoFromCtx(r), obj))
	})
	t.Run("no_conditional_headers", func(t *testing.T) {
		t.Parallel()
		r := testCtx("GET", "http://example.com/")
		obj := &api.Object{ETag: `"abc"`}
		require.False(t, ClientConditionalMatch(requestInfoFromCtx(r), obj))
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
			Header: func() header.Map {
				m := headerMap(header.ContentLength, "100")
				m.Set(header.ContentType, "text/html")
				return m
			}(),
		}
		resp304 := headerMap(header.ContentLength, "200")
		resp304.Set(header.ContentType, "application/json")
		resp304.Set(header.ContentEncoding, "gzip")
		resp304.Set(header.SetCookie, "new=cookie")
		resp304.Set(header.TransferEncoding, "chunked")
		resp304.Set(header.ETag, `"v2"`)
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
			Header: headerMap(header.CacheControl, "max-age=60"),
		}
		resp304 := headerMap(header.CacheControl, "max-age=120")
		resp304.Set(header.LastModified, "Mon, 01 Jan 2024 00:00:00 GMT")
		MergeHeaders304(stored, resp304)
		assert.Equal(t, "max-age=120", stored.Header.Get(header.CacheControl))
		assert.Equal(t, "Mon, 01 Jan 2024 00:00:00 GMT", stored.Header.Get(header.LastModified))
	})
	t.Run("replaces_multi_line_vary_wholesale", func(t *testing.T) {
		t.Parallel()
		storedLines := headerMap(header.Vary, "Accept-Encoding,Accept-Language")
		storedLines.AppendEntry(header.Vary, "BM-Market")
		stored := &api.Object{Header: storedLines}
		resp304Lines := headerMap(header.Vary, "Accept-Encoding")
		resp304Lines.AppendEntry(header.Vary, "BM-Market")
		MergeHeaders304(stored, resp304Lines)
		// The 304's Vary replaces the stored one line-for-line; per-line
		// Set would clobber the first slot and corrupt the stored list.
		assert.Equal(t, []string{"Accept-Encoding", "BM-Market"}, varyLines(stored.Header))
	})
	t.Run("keeps_stored_vary_when_304_omits_it", func(t *testing.T) {
		t.Parallel()
		storedLines := headerMap(header.Vary, "Accept-Encoding")
		storedLines.AppendEntry(header.Vary, "BM-Market")
		stored := &api.Object{Header: storedLines}
		MergeHeaders304(stored, headerMap(header.CacheControl, "max-age=120"))
		assert.Equal(t, []string{"Accept-Encoding", "BM-Market"}, varyLines(stored.Header))
	})
}

// varyLines collects the individual Vary field-line values in stored
// order, exposing multi-entry corruption that GetAll's join would hide.
func varyLines(m header.Map) []string {
	var lines []string
	m.Range(func(k, v string) bool {
		if k == header.Vary {
			lines = append(lines, v)
		}
		return true
	})
	return lines
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
