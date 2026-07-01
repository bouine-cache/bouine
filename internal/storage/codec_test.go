package storage

import (
	"bytes"
	"net/http"
	"testing"
	"time"

	"github.com/thylong/bouine/pkg/api"
	"github.com/thylong/bouine/pkg/header"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	orig := &api.Object{
		Key:        api.Key(0xDEADBEEFCAFE),
		VaryKey:    "accept-encoding=gzip",
		StatusCode: http.StatusOK,
		Header: http.Header{
			header.ContentType:  {"application/json"},
			header.CacheControl: {"public", "max-age=600"},
			header.Vary:         {"Accept-Encoding"},
		},
		Body:                 []byte("the quick brown fox jumps over the lazy dog"),
		BodySize:             43,
		StoredAt:             time.Unix(1_700_000_000, 123).UTC(),
		TTL:                  10 * time.Minute,
		StaleWhileRevalidate: 30 * time.Second,
		StaleIfError:         5 * time.Minute,
		ETag:                 `"abc123"`,
		LastModified:         time.Unix(1_699_000_000, 0).UTC(),
		SurrogateKeys:        []string{"product-42", "category-7"},
		Hits:                 99,
	}

	got, err := decodeObject(encodeObject(orig))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Key != orig.Key || got.VaryKey != orig.VaryKey || got.StatusCode != orig.StatusCode {
		t.Errorf("identity fields mismatch: %+v", got)
	}
	if !bytes.Equal(got.Body, orig.Body) || got.BodySize != int64(len(orig.Body)) {
		t.Errorf("body mismatch: got %q (size %d)", got.Body, got.BodySize)
	}
	if got.TTL != orig.TTL || got.StaleWhileRevalidate != orig.StaleWhileRevalidate || got.StaleIfError != orig.StaleIfError {
		t.Errorf("duration fields mismatch: %+v", got)
	}
	if !got.StoredAt.Equal(orig.StoredAt) || !got.LastModified.Equal(orig.LastModified) {
		t.Errorf("time fields mismatch: storedAt=%v lastMod=%v", got.StoredAt, got.LastModified)
	}
	if got.ETag != orig.ETag || got.Hits != orig.Hits {
		t.Errorf("etag/hits mismatch: %+v", got)
	}
	if len(got.SurrogateKeys) != 2 || got.SurrogateKeys[0] != "product-42" {
		t.Errorf("surrogate keys mismatch: %v", got.SurrogateKeys)
	}
	for k, want := range orig.Header {
		if g := got.Header[k]; len(g) != len(want) {
			t.Errorf("header %q: got %v want %v", k, g, want)
		}
	}
}

func TestEncodeDecodeZeroAndEmpty(t *testing.T) {
	// Zero LastModified, empty body, no surrogate keys, single header.
	orig := &api.Object{
		StatusCode: http.StatusNoContent,
		Header:     http.Header{header.XCache: {"MISS"}},
		StoredAt:   time.Unix(1_700_000_000, 0).UTC(),
	}
	got, err := decodeObject(encodeObject(orig))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.LastModified.IsZero() {
		t.Errorf("LastModified should round-trip as zero, got %v", got.LastModified)
	}
	if len(got.Body) != 0 || got.BodySize != 0 {
		t.Errorf("empty body expected, got %q (size %d)", got.Body, got.BodySize)
	}
	if got.SurrogateKeys != nil {
		t.Errorf("nil surrogate keys expected, got %v", got.SurrogateKeys)
	}
}

func TestDecodeRejectsCorruptAndLegacyJSON(t *testing.T) {
	cases := map[string][]byte{
		"empty":       {},
		"legacy json": []byte(`{"key":1,"status_code":200}`),
		"bad version": {0xFE, 0x01, 0x02},
		"truncated":   encodeObject(&api.Object{Header: http.Header{"A": {"b"}}, Body: []byte("xx")})[:4],
	}
	for name, blob := range cases {
		if _, err := decodeObject(blob); err == nil {
			t.Errorf("%s: expected decode error, got nil", name)
		}
	}
}
