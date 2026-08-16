package storage

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

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
