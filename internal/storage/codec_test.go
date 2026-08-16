package storage

import (
	"bytes"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	orig := &api.Object{
		Key:        testkey.Key(0xDEADBEEFCAFE),
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
	require.NoError(t, err, "decode")

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
	require.NoError(t, err, "decode")
	assert.True(t, got.LastModified.IsZero())
	assert.True(t, got.StoredAt.IsZero())
	if len(got.Body) != 0 || got.BodySize != 0 {
		t.Errorf("empty body expected, got %q (size %d)", got.Body, got.BodySize)
	}
	assert.Nil(t, got.SurrogateKeys)
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
		require.True(t, bytes.Equal(blob, prev))
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
		_, err := decodeObject(blob)
		assert.NotNilf(t, err, "case %s", name)
	}
}
func TestEncodeDecodeObject_RoundTrip(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		Key:                  api.Key{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		VaryKey:              "variant1",
		StatusCode:           200,
		Header:               header.FromHTTP(map[string][]string{"Content-Type": {"text/html"}, "X-Custom": {"val"}}),
		Body:                 []byte("hello world"),
		BodySize:             11,
		StoredAt:             time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		LastModified:         time.Date(2023, 6, 15, 12, 0, 0, 0, time.UTC),
		TTL:                  60 * time.Second,
		StaleWhileRevalidate: 30 * time.Second,
		StaleIfError:         5 * time.Minute,
		Hits:                 42,
		ETag:                 `"v1"`,
		SurrogateKeys:        []string{"key1", "key2"},
	}
	encoded := EncodeObject(obj)
	require.NotEmpty(t, encoded)
	decoded, err := DecodeObject(encoded)
	require.NoError(t, err)
	require.NotNil(t, decoded)
	assert.Equal(t, obj.Key, decoded.Key)
	assert.Equal(t, obj.VaryKey, decoded.VaryKey)
	assert.Equal(t, obj.StatusCode, decoded.StatusCode)
	assert.Equal(t, obj.TTL, decoded.TTL)
	assert.Equal(t, obj.StaleWhileRevalidate, decoded.StaleWhileRevalidate)
	assert.Equal(t, obj.StaleIfError, decoded.StaleIfError)
	assert.Equal(t, obj.StoredAt, decoded.StoredAt)
	assert.Equal(t, obj.LastModified, decoded.LastModified)
	assert.Equal(t, obj.Hits, decoded.Hits)
	assert.Equal(t, obj.ETag, decoded.ETag)
	assert.Equal(t, obj.SurrogateKeys, decoded.SurrogateKeys)
	assert.True(t, bytes.Equal(obj.Body, decoded.Body))
	assert.Equal(t, obj.BodySize, decoded.BodySize)
	assert.Equal(t, "text/html", decoded.Header.Get("Content-Type"))
	assert.Equal(t, "val", decoded.Header.Get("X-Custom"))
}

func TestEncodeObjectInto_AppendToBuffer(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		Key:        api.Key{},
		StatusCode: 200,
		Header:     header.Map{},
		Body:       []byte("x"),
		BodySize:   1,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	buf := make([]byte, 0, 256)
	buf = append(buf, "prefix"...)
	encoded := EncodeObjectInto(obj, buf)
	assert.True(t, bytes.HasPrefix(encoded, []byte("prefix")))
}

func TestDecodeObject_CorruptBlob(t *testing.T) {
	t.Parallel()
	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		_, err := DecodeObject(nil)
		require.Error(t, err)
	})
	t.Run("wrong_version", func(t *testing.T) {
		t.Parallel()
		_, err := DecodeObject([]byte{0xFF})
		require.Error(t, err)
	})
	t.Run("truncated_key", func(t *testing.T) {
		t.Parallel()
		_, err := DecodeObject([]byte{3, 0x01, 0x02})
		require.Error(t, err)
	})
}

func TestDecodeObject_EmptyBody(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		Key:        api.Key{},
		StatusCode: 304,
		Header:     header.Map{},
		Body:       nil,
		BodySize:   0,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	encoded := EncodeObject(obj)
	decoded, err := DecodeObject(encoded)
	require.NoError(t, err)
	assert.Empty(t, decoded.Body)
	assert.Equal(t, int64(0), decoded.BodySize)
}

func TestDecodeObject_ZeroTimes(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		Key:        api.Key{},
		StatusCode: 200,
		Header:     header.Map{},
		Body:       []byte("x"),
		BodySize:   1,
		StoredAt:   time.Time{},
		TTL:        60 * time.Second,
	}
	encoded := EncodeObject(obj)
	decoded, err := DecodeObject(encoded)
	require.NoError(t, err)
	assert.True(t, decoded.StoredAt.IsZero())
	assert.True(t, decoded.LastModified.IsZero())
}
