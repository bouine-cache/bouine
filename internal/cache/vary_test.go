package cache

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/thylong/bouine/pkg/api"
)

func TestVariantKey_NoVary(t *testing.T) {
	primary := api.Key(100)
	got := VariantKey(primary, "", nil)
	if got != primary {
		t.Fatalf("no Vary should return primary key")
	}
}

func TestVariantKey_DifferentHeaders(t *testing.T) {
	primary := api.Key(100)
	h1 := http.Header{"Accept-Encoding": {"gzip"}}
	h2 := http.Header{"Accept-Encoding": {"br"}}
	k1 := VariantKey(primary, "Accept-Encoding", h1)
	k2 := VariantKey(primary, "Accept-Encoding", h2)
	if k1 == k2 {
		t.Fatal("different Accept-Encoding should produce different keys")
	}
	if k1 == primary || k2 == primary {
		t.Fatal("variant key should differ from primary")
	}
}

func TestVariantKey_SameHeaders(t *testing.T) {
	primary := api.Key(100)
	h := http.Header{"Accept-Encoding": {"gzip"}}
	k1 := VariantKey(primary, "Accept-Encoding", h)
	k2 := VariantKey(primary, "Accept-Encoding", h)
	if k1 != k2 {
		t.Fatal("same headers should produce same variant key")
	}
}

func TestVariantKey_VaryStar(t *testing.T) {
	primary := api.Key(100)
	h1 := http.Header{"Accept": {"text/html"}}
	h2 := http.Header{"Accept": {"application/json"}}
	k1 := VariantKey(primary, "*", h1)
	k2 := VariantKey(primary, "*", h2)
	if k1 == k2 {
		t.Fatal("Vary: * with different headers should differ")
	}
}

func TestServeRange_SingleRange(t *testing.T) {
	obj := &api.Object{
		StatusCode: 200,
		Header:     http.Header{"Content-Type": {"text/plain"}},
		Body:       []byte("Hello, World!"),
		BodySize:   13,
	}

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Range", "bytes=0-4")
	rr := httptest.NewRecorder()

	ok := ServeRange(rr, r, obj)
	if !ok {
		t.Fatal("expected range served")
	}
	if rr.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", rr.Code)
	}
	if rr.Body.String() != "Hello" {
		t.Fatalf("body = %q, want Hello", rr.Body.String())
	}
	cr := rr.Header().Get("Content-Range")
	if cr != "bytes 0-4/13" {
		t.Fatalf("Content-Range = %q", cr)
	}
}

func TestServeRange_SuffixRange(t *testing.T) {
	obj := &api.Object{
		StatusCode: 200,
		Header:     http.Header{},
		Body:       []byte("abcdefghij"),
		BodySize:   10,
	}

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Range", "bytes=-3")
	rr := httptest.NewRecorder()

	ok := ServeRange(rr, r, obj)
	if !ok {
		t.Fatal("expected range served")
	}
	if rr.Body.String() != "hij" {
		t.Fatalf("body = %q, want hij", rr.Body.String())
	}
}

func TestServeRange_OpenEnded(t *testing.T) {
	obj := &api.Object{
		StatusCode: 200,
		Header:     http.Header{},
		Body:       []byte("abcde"),
		BodySize:   5,
	}

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Range", "bytes=3-")
	rr := httptest.NewRecorder()

	ok := ServeRange(rr, r, obj)
	if !ok {
		t.Fatal("expected range served")
	}
	if rr.Body.String() != "de" {
		t.Fatalf("body = %q, want de", rr.Body.String())
	}
}

func TestServeRange_Unsatisfiable(t *testing.T) {
	obj := &api.Object{
		StatusCode: 200,
		Header:     http.Header{},
		Body:       []byte("ab"),
		BodySize:   2,
	}

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Range", "bytes=5-10")
	rr := httptest.NewRecorder()

	ok := ServeRange(rr, r, obj)
	if !ok {
		t.Fatal("expected range handled (416)")
	}
	if rr.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("status = %d, want 416", rr.Code)
	}
}

func TestServeRange_NoRangeHeader(t *testing.T) {
	obj := &api.Object{Body: []byte("x"), BodySize: 1}
	r := httptest.NewRequest("GET", "/", nil)
	rr := httptest.NewRecorder()

	ok := ServeRange(rr, r, obj)
	if ok {
		t.Fatal("no Range header should return false")
	}
}

func TestServeRange_MultiRange(t *testing.T) {
	obj := &api.Object{Body: []byte("abcde"), BodySize: 5}
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Range", "bytes=0-1, 3-4")
	rr := httptest.NewRecorder()

	ok := ServeRange(rr, r, obj)
	if !ok {
		t.Fatal("multi-range should be served as multipart/byteranges")
	}
	if rr.Code != 206 {
		t.Fatalf("expected 206, got %d", rr.Code)
	}
	ct := rr.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "multipart/byteranges") {
		t.Fatalf("expected multipart/byteranges Content-Type, got %q", ct)
	}
}

func TestHandler_VaryAwareStorage(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		w.Header().Set("Vary", "Accept-Encoding")
		w.Header().Set("Content-Encoding", r.Header.Get("Accept-Encoding"))
		w.Header().Set("ETag", `"v1"`)
		w.WriteHeader(200)
		_, _ = w.Write([]byte("body-" + r.Header.Get("Accept-Encoding")))
	})
	h := testHandler(t, upstream)

	// gzip request.
	r1 := httptest.NewRequest("GET", "http://example.com/vary", nil)
	r1.Header.Set("Accept-Encoding", "gzip")
	rr1 := httptest.NewRecorder()
	h.ServeHTTP(rr1, r1)
	if rr1.Header().Get("X-Cache") != "MISS" {
		t.Fatalf("gzip first: X-Cache = %q", rr1.Header().Get("X-Cache"))
	}

	// br request — different variant, should MISS.
	r2 := httptest.NewRequest("GET", "http://example.com/vary", nil)
	r2.Header.Set("Accept-Encoding", "br")
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, r2)
	if rr2.Header().Get("X-Cache") != "MISS" {
		t.Fatalf("br first: X-Cache = %q", rr2.Header().Get("X-Cache"))
	}

	// gzip again — should HIT.
	r3 := httptest.NewRequest("GET", "http://example.com/vary", nil)
	r3.Header.Set("Accept-Encoding", "gzip")
	rr3 := httptest.NewRecorder()
	h.ServeHTTP(rr3, r3)
	if rr3.Header().Get("X-Cache") != "HIT" {
		t.Fatalf("gzip second: X-Cache = %q, want HIT", rr3.Header().Get("X-Cache"))
	}
	if rr3.Body.String() != "body-gzip" {
		t.Fatalf("body = %q, want body-gzip", rr3.Body.String())
	}
}

func TestHandler_RangeOnCachedObject(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		w.Header().Set("ETag", `"full"`)
		w.WriteHeader(200)
		_, _ = w.Write([]byte("Hello, Range World!"))
	})
	h := testHandler(t, upstream)

	// Populate cache with full body.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/range", nil))
	if rr.Code != 200 {
		t.Fatalf("status = %d", rr.Code)
	}

	// Range request on cached object.
	rangeReq := httptest.NewRequest("GET", "http://example.com/range", nil)
	rangeReq.Header.Set("Range", "bytes=0-4")
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, rangeReq)
	if rr2.Code != http.StatusPartialContent {
		t.Fatalf("range status = %d, want 206", rr2.Code)
	}
	if rr2.Body.String() != "Hello" {
		t.Fatalf("range body = %q, want Hello", rr2.Body.String())
	}
}
