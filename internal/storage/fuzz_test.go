package storage

import (
	"bytes"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// fuzzFixedTime is a deterministic timestamp for fuzz round-trip tests.
// Using a fixed time instead of time.Now() ensures reproducibility and
// complies with AGENTS.md §8: "no time.Now() in tests."
var fuzzFixedTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// FuzzCodecRoundTrip fuzzes the object encode/decode round-trip with
// arbitrary field values. DecodeObject(EncodeObject(obj)) must reproduce
// all serialized fields exactly. It also tests with zero times to
// exercise the time.Time zero-value round-trip path (ADR-0015 risk).
func FuzzCodecRoundTrip(f *testing.F) {
	f.Add(200, "accept-encoding=gzip", `"abc123"`, "public, max-age=600", "Hello, World!")
	f.Add(404, "", "", "", "")
	f.Add(301, "", `"weak"`, "no-cache", "")
	f.Add(500, "vary-key-with-special-chars", `"etag-with-\\"`, "private", "body with \x00 null bytes")
	f.Add(200, "", "", "", "large body that exceeds the stack buffer threshold to exercise the heap path and ensure the codec handles it correctly without any truncation or corruption in the round trip")
	f.Add(204, "", "", "", "")

	f.Fuzz(func(t *testing.T, statusCode int, varyKey, etag, cacheControl, body string) {
		// Full object with all fields populated.
		assertCodecRoundTrip(t, &api.Object{
			Key:        testkey.Key(0xDEADBEEFCAFE),
			VaryKey:    varyKey,
			StatusCode: statusCode,
			Header: header.FromHTTP(http.Header{
				header.CacheControl: {cacheControl},
				header.ContentType:  {"application/octet-stream"},
			}),
			Body:                 []byte(body),
			BodySize:             int64(len(body)),
			StoredAt:             fuzzFixedTime,
			TTL:                  10 * time.Minute,
			StaleWhileRevalidate: 30 * time.Second,
			StaleIfError:         5 * time.Minute,
			ETag:                 etag,
			LastModified:         fuzzFixedTime,
			SurrogateKeys:        []string{"product-42", "category-7"},
			Hits:                 99,
		})

		// Minimal object with zero times to exercise the zero-value
		// round-trip path (ADR-0015 risk).
		assertCodecRoundTrip(t, &api.Object{
			Key:        testkey.Key(0),
			StatusCode: statusCode,
			Header:     header.FromHTTP(http.Header{header.ContentType: {"text/plain"}}),
			Body:       []byte(body),
			BodySize:   int64(len(body)),
		})
	})
}

// FuzzDecodeObjectArbitrary fuzzes DecodeObject with arbitrary bytes to
// ensure it never panics on corrupt or malformed input. It must return
// either a valid object or an error — never a nil object with nil error.
func FuzzDecodeObjectArbitrary(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x03})
	f.Add([]byte{0x03, 0x01, 0x02, 0x03})
	f.Add([]byte{0xFE, 0x01, 0x02, 0x03})
	f.Add([]byte("not a binary blob at all"))
	f.Add([]byte{0x00})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	valid := EncodeObject(&api.Object{
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.ContentType: {"text/plain"}}),
		Body:       []byte("test"),
	})
	f.Add(valid)
	f.Add(valid[:4])
	f.Add(append(valid, 0xFF, 0xFF, 0xFF))

	f.Fuzz(func(t *testing.T, data []byte) {
		obj, err := DecodeObject(data)
		if err != nil {
			return
		}
		if obj == nil {
			t.Fatal("DecodeObject returned nil object with nil error")
		}
	})
}

// assertCodecRoundTrip encodes and decodes the object, then verifies all
// serialized fields match the original.
func assertCodecRoundTrip(t *testing.T, orig *api.Object) {
	t.Helper()
	encoded := EncodeObject(orig)
	decoded, err := DecodeObject(encoded)
	require.NoError(t, err, "decode failed")

	if decoded.Key != orig.Key {
		t.Errorf("Key mismatch: got %v want %v", decoded.Key, orig.Key)
	}
	if decoded.VaryKey != orig.VaryKey {
		t.Errorf("VaryKey mismatch: got %q want %q", decoded.VaryKey, orig.VaryKey)
	}
	if decoded.StatusCode != orig.StatusCode {
		t.Errorf("StatusCode mismatch: got %d want %d", decoded.StatusCode, orig.StatusCode)
	}
	if decoded.TTL != orig.TTL {
		t.Errorf("TTL mismatch: got %v want %v", decoded.TTL, orig.TTL)
	}
	if decoded.StaleWhileRevalidate != orig.StaleWhileRevalidate {
		t.Errorf("SWR mismatch: got %v want %v", decoded.StaleWhileRevalidate, orig.StaleWhileRevalidate)
	}
	if decoded.StaleIfError != orig.StaleIfError {
		t.Errorf("SIE mismatch: got %v want %v", decoded.StaleIfError, orig.StaleIfError)
	}
	if !decoded.StoredAt.Equal(orig.StoredAt) {
		t.Errorf("StoredAt mismatch: got %v want %v", decoded.StoredAt, orig.StoredAt)
	}
	if !decoded.LastModified.Equal(orig.LastModified) {
		t.Errorf("LastModified mismatch: got %v want %v", decoded.LastModified, orig.LastModified)
	}
	if decoded.ETag != orig.ETag {
		t.Errorf("ETag mismatch: got %q want %q", decoded.ETag, orig.ETag)
	}
	if decoded.Hits != orig.Hits {
		t.Errorf("Hits mismatch: got %d want %d", decoded.Hits, orig.Hits)
	}
	if !bytes.Equal(decoded.Body, orig.Body) {
		t.Errorf("Body mismatch: got %q want %q", decoded.Body, orig.Body)
	}
	if decoded.BodySize != orig.BodySize {
		t.Errorf("BodySize mismatch: got %d want %d", decoded.BodySize, orig.BodySize)
	}
	if len(decoded.SurrogateKeys) != len(orig.SurrogateKeys) {
		t.Errorf("SurrogateKeys length mismatch: got %d want %d", len(decoded.SurrogateKeys), len(orig.SurrogateKeys))
	} else {
		for i, sk := range orig.SurrogateKeys {
			if decoded.SurrogateKeys[i] != sk {
				t.Errorf("SurrogateKey[%d] mismatch: got %q want %q", i, decoded.SurrogateKeys[i], sk)
			}
		}
	}
	orig.Header.Range(func(k, want string) bool {
		if got := decoded.Header.Get(k); got != want {
			t.Errorf("header %q: got %q want %q", k, got, want)
		}
		return true
	})
}
