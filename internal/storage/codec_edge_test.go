package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestObjReader_Uvarint_Corrupt(t *testing.T) {
	t.Parallel()
	// A buffer where uvarint cannot be decoded (incomplete) must set err.
	blob := []byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF}
	_, err := decodeObject(blob)
	require.Error(t, err)
}

func TestObjReader_Varint_Corrupt(t *testing.T) {
	t.Parallel()
	// Construct a valid-looking blob but truncate it so varint fails.
	obj := &api.Object{
		Key:        testkey.Key(1),
		StatusCode: 200,
		Header:     header.Map{},
		Body:       []byte("x"),
		BodySize:   1,
		StoredAt:   time.Unix(1, 0),
		TTL:        60 * time.Second,
	}
	blob := encodeObject(obj)
	// Truncate mid-StoredAt varint to force a varint read error.
	_, err := decodeObject(blob[:len(blob)-2])
	require.Error(t, err)
}

func TestObjReader_Count_Overflow(t *testing.T) {
	t.Parallel()
	// A blob that claims a huge header count (larger than remaining bytes).
	corrupt := make([]byte, 0, 32)
	corrupt = append(corrupt, 3)                   // version
	corrupt = append(corrupt, make([]byte, 16)...) // key
	corrupt = append(corrupt, 0xC8, 0x01)          // status 200
	corrupt = append(corrupt, 0)                   // varykey len 0
	corrupt = append(corrupt, 0)                   // body len 0
	corrupt = append(corrupt, 0)                   // stored_at present=0
	// TTL varint:
	corrupt = append(corrupt, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01)
	// SWR present=0, SIE present=0
	corrupt = append(corrupt, 0, 0)
	// ETag len=0, LastMod present=0, hits=0
	corrupt = append(corrupt, 0, 0, 0)
	// Header count: encode a value larger than remaining bytes.
	corrupt = append(corrupt, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F) // huge count
	_, err := decodeObject(corrupt)
	require.Error(t, err)
}

func TestObjReader_Bytes_OutOfBounds(t *testing.T) {
	t.Parallel()
	// Truncated blob where a bytes(n) call exceeds remaining data.
	blob := []byte{3, 0x01, 0x02, 0x03} // version + partial key
	_, err := decodeObject(blob)
	require.Error(t, err)
}

func TestDecodeObject_BadVersionByte(t *testing.T) {
	t.Parallel()
	_, err := decodeObject([]byte{0xFE, 0x00})
	require.Error(t, err)
}

func TestEncodeObjectInto_EmptyHeader(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		Key:        api.Key{},
		StatusCode: 200,
		Header:     header.Map{},
		Body:       nil,
		BodySize:   0,
		StoredAt:   time.Time{},
		TTL:        0,
	}
	encoded := EncodeObjectInto(obj, nil)
	decoded, err := DecodeObject(encoded)
	require.NoError(t, err)
	assert.Equal(t, obj.StatusCode, decoded.StatusCode)
	assert.True(t, decoded.StoredAt.IsZero())
	assert.True(t, decoded.LastModified.IsZero())
}
