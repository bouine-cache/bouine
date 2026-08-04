package cache

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestServeRange_SingleRange(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.ContentType: {"text/plain"}}),
		Body:       []byte("Hello, World!"),
		BodySize:   13,
	}

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.Range, "bytes=0-4")
	rr := httptest.NewRecorder()

	ok := ServeRange(rr, r, obj, false, api.SourceHot)
	require.True(t, ok)
	require.Equal(t, http.StatusPartialContent, rr.Code)
	require.Equal(t, "Hello", rr.Body.String())
	cr := rr.Header().Get(header.ContentRange)
	require.Equal(t, "bytes 0-4/13", cr)
	{
		xc := rr.Header().Get(header.XCache)
		require.Equal(t, "HIT", xc)
	}
}

func TestServeRange_SuffixRange(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.Map{},
		Body:       []byte("abcdefghij"),
		BodySize:   10,
	}

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.Range, "bytes=-3")
	rr := httptest.NewRecorder()

	ok := ServeRange(rr, r, obj, false, api.SourceHot)
	require.True(t, ok)
	require.Equal(t, "hij", rr.Body.String())
}

func TestServeRange_OpenEnded(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.Map{},
		Body:       []byte("abcde"),
		BodySize:   5,
	}

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.Range, "bytes=3-")
	rr := httptest.NewRecorder()

	ok := ServeRange(rr, r, obj, false, api.SourceHot)
	require.True(t, ok)
	require.Equal(t, "de", rr.Body.String())
}

func TestServeRange_Unsatisfiable(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.Map{},
		Body:       []byte("ab"),
		BodySize:   2,
	}

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.Range, "bytes=5-10")
	rr := httptest.NewRecorder()

	ok := ServeRange(rr, r, obj, false, api.SourceHot)
	require.True(t, ok)
	require.Equal(t, http.StatusRequestedRangeNotSatisfiable, rr.Code)
}

func TestServeRange_NoRangeHeader(t *testing.T) {
	t.Parallel()
	obj := &api.Object{Body: []byte("x"), BodySize: 1}
	r := httptest.NewRequest("GET", "/", nil)
	rr := httptest.NewRecorder()

	ok := ServeRange(rr, r, obj, false, api.SourceHot)
	require.False(t, ok)
}

func TestServeRange_MultiRange(t *testing.T) {
	t.Parallel()
	obj := &api.Object{Body: []byte("abcde"), BodySize: 5}
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.Range, "bytes=0-1, 3-4")
	rr := httptest.NewRecorder()

	ok := ServeRange(rr, r, obj, false, api.SourceHot)
	require.True(t, ok)
	require.Equal(t, 206, rr.Code)
	ct := rr.Header().Get(header.ContentType)
	require.True(t, strings.HasPrefix(ct, "multipart/byteranges"))
	{
		xc := rr.Header().Get(header.XCache)
		require.Equal(t, "HIT", xc)
	}
}
