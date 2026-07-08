package cache

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
	if !ok {
		t.Fatal("expected range served")
	}
	if rr.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", rr.Code)
	}
	if rr.Body.String() != "Hello" {
		t.Fatalf("body = %q, want Hello", rr.Body.String())
	}
	cr := rr.Header().Get(header.ContentRange)
	if cr != "bytes 0-4/13" {
		t.Fatalf("Content-Range = %q", cr)
	}
	if xc := rr.Header().Get(header.XCache); xc != "HIT" {
		t.Fatalf("X-Cache = %q, want HIT", xc)
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
	if !ok {
		t.Fatal("expected range served")
	}
	if rr.Body.String() != "hij" {
		t.Fatalf("body = %q, want hij", rr.Body.String())
	}
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
	if !ok {
		t.Fatal("expected range served")
	}
	if rr.Body.String() != "de" {
		t.Fatalf("body = %q, want de", rr.Body.String())
	}
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
	if !ok {
		t.Fatal("expected range handled (416)")
	}
	if rr.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("status = %d, want 416", rr.Code)
	}
}

func TestServeRange_NoRangeHeader(t *testing.T) {
	t.Parallel()
	obj := &api.Object{Body: []byte("x"), BodySize: 1}
	r := httptest.NewRequest("GET", "/", nil)
	rr := httptest.NewRecorder()

	ok := ServeRange(rr, r, obj, false, api.SourceHot)
	if ok {
		t.Fatal("no Range header should return false")
	}
}

func TestServeRange_MultiRange(t *testing.T) {
	t.Parallel()
	obj := &api.Object{Body: []byte("abcde"), BodySize: 5}
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.Range, "bytes=0-1, 3-4")
	rr := httptest.NewRecorder()

	ok := ServeRange(rr, r, obj, false, api.SourceHot)
	if !ok {
		t.Fatal("multi-range should be served as multipart/byteranges")
	}
	if rr.Code != 206 {
		t.Fatalf("expected 206, got %d", rr.Code)
	}
	ct := rr.Header().Get(header.ContentType)
	if !strings.HasPrefix(ct, "multipart/byteranges") {
		t.Fatalf("expected multipart/byteranges Content-Type, got %q", ct)
	}
	if xc := rr.Header().Get(header.XCache); xc != "HIT" {
		t.Fatalf("X-Cache = %q, want HIT", xc)
	}
}
