package api

import (
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewKeyFromBytes(t *testing.T) {
	b := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	k := NewKeyFromBytes(b)
	require.Equal(t, Key(b), k)
}

func TestNewKeyFromUint64(t *testing.T) {
	k := NewKeyFromUint64(0xDEADBEEF)
	require.Equal(t, uint64(0xDEADBEEF), binary.LittleEndian.Uint64(k[:8]))
	// High half must be zero for the test/diagnostic constructor.
	require.Equal(t, uint64(0), binary.LittleEndian.Uint64(k[8:]))
}

func TestWithVaryXorsBothHalves(t *testing.T) {
	k := NewKeyFromBytes([16]byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
	})
	v := uint64(0xFFFFFFFFFFFFFFFF)
	got := k.WithVary(v)
	require.Equal(t, binary.LittleEndian.Uint64(k[:8])^v, binary.LittleEndian.Uint64(got[:8]))
	require.Equal(t, binary.LittleEndian.Uint64(k[8:])^v, binary.LittleEndian.Uint64(got[8:]))
}

func TestWithVaryZeroIsIdentity(t *testing.T) {
	k := NewKeyFromUint64(42)
	require.Equal(t, k, k.WithVary(0))
}

func TestIsZero(t *testing.T) {
	require.True(t, Key{}.IsZero())
	require.False(t, NewKeyFromUint64(1).IsZero())
}

func TestHexIs32Chars(t *testing.T) {
	k := NewKeyFromUint64(0x42)
	s := k.Hex()
	require.Len(t, s, 32)
	require.Equal(t, k.Hex(), k.String())
}

func TestSingleFlightKeyDistinguishesSuffix(t *testing.T) {
	k := NewKeyFromUint64(0xCAFEBABE)
	base := k.SingleFlightKey(0)
	reval := k.SingleFlightKey(1)
	require.NotEqual(t, base, reval)
	require.Len(t, base, 32)
}

func TestKeyJSONRoundTrip(t *testing.T) {
	k := NewKeyFromBytes([16]byte{
		0xEF, 0xBE, 0xAD, 0xDE, 0x00, 0x00, 0x00, 0x00,
		0x78, 0x56, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00,
	})
	out, err := json.Marshal(k)
	require.NoError(t, err)
	// [lo, hi] decimal array.
	require.Equal(t, `[3735928559,305419896]`, string(out))

	var got Key
	require.NoError(t, json.Unmarshal(out, &got))
	require.Equal(t, k, got)
}

func TestKeyUnmarshalJSONRejectsBadInput(t *testing.T) {
	var k Key
	err := json.Unmarshal([]byte(`"deadbeef"`), &k)
	require.Error(t, err)
	err = json.Unmarshal([]byte(`["a","b"]`), &k)
	require.Error(t, err)
}
