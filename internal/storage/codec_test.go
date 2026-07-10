package storage

import (
	"bytes"
	"net/http"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	orig := &api.Object{
		Key:        api.Key(0xDEADBEEFCAFE),
		VaryKey:    "accept-encoding=gzip",
		StatusCode: http.StatusOK,
		Header: header.FromHTTP(http.Header{
			header.ContentType:  {"application/json"},
			header.CacheControl: {"public", "max-age=600"},
			header.Vary:         {"Accept-Encoding"},
		}),
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
	orig.Header.Range(func(k, want string) bool {
		if g := got.Header.Get(k); g != want {
			t.Errorf("header %q: got %q want %q", k, g, want)
		}
		return true
	})
}

func TestEncodeDecodeZeroAndEmpty(t *testing.T) {
	// Zero LastModified and zero StoredAt, empty body, no surrogate keys,
	// single header.
	orig := &api.Object{
		StatusCode: http.StatusNoContent,
		Header:     header.FromHTTP(http.Header{header.XCache: {"MISS"}}),
		// StoredAt left zero to verify the time.Time zero-value
		// round-trip (ADR-0015 risk).
	}
	got, err := decodeObject(encodeObject(orig))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.LastModified.IsZero() {
		t.Errorf("LastModified should round-trip as zero, got %v", got.LastModified)
	}
	if !got.StoredAt.IsZero() {
		t.Errorf("StoredAt should round-trip as zero, got %v", got.StoredAt)
	}
	if len(got.Body) != 0 || got.BodySize != 0 {
		t.Errorf("empty body expected, got %q (size %d)", got.Body, got.BodySize)
	}
	if got.SurrogateKeys != nil {
		t.Errorf("nil surrogate keys expected, got %v", got.SurrogateKeys)
	}
}

func TestEncodeIsDeterministicAcrossMapIteration(t *testing.T) {
	// FromHTTP inherits Go map iteration order (randomized per call), but
	// Range must sort entries by canonical key so encodeObject emits
	// identical bytes for logically identical objects. This matters for
	// anti-entropy checksums and any future content-addressing on the
	// warm tier.
	base := http.Header{
		header.ContentType:  {"application/json"},
		header.CacheControl: {"public, max-age=600"},
		header.Vary:         {"Accept-Encoding"},
		header.Age:          {"42"},
	}
	prev := encodeObject(&api.Object{
		StatusCode: http.StatusOK,
		Header:     header.FromHTTP(base),
		Body:       []byte("deterministic payload"),
	})
	for range 20 {
		blob := encodeObject(&api.Object{
			StatusCode: http.StatusOK,
			Header:     header.FromHTTP(base),
			Body:       []byte("deterministic payload"),
		})
		if !bytes.Equal(blob, prev) {
			t.Fatalf("encodeObject not deterministic\nprev=%x\ncurr=%x", prev, blob)
		}
		prev = blob
	}
}

func TestDecodeRejectsCorruptAndLegacyJSON(t *testing.T) {
	cases := map[string][]byte{
		"empty":       {},
		"legacy json": []byte(`{"key":1,"status_code":200}`),
		"bad version": {0xFE, 0x01, 0x02},
		"truncated":   encodeObject(&api.Object{Header: header.FromHTTP(http.Header{"A": {"b"}}), Body: []byte("xx")})[:4],
	}
	for name, blob := range cases {
		if _, err := decodeObject(blob); err == nil {
			t.Errorf("%s: expected decode error, got nil", name)
		}
	}
}
