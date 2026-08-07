package api

import (
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// fromUint64 builds a Key with v in the high half and a zeroed low
// half. Local test helper — production tests use testkey.Key.
func fromUint64(v uint64) Key {
	var k Key
	binary.BigEndian.PutUint64(k[:8], v)
	return k
}

func TestNewKeyFromBytes(t *testing.T) {
	t.Parallel()
	b := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	k := NewKeyFromBytes(b)
	require.Equal(t, Key(b), k)
}

func TestHash64ReturnsHighHalf(t *testing.T) {
	t.Parallel()
	k := fromUint64(0xDEADBEEF)
	require.Equal(t, uint64(0xDEADBEEF), k.Hash64())
	require.Equal(t, uint64(0), binary.BigEndian.Uint64(k[8:]))
}

func TestWithVaryXorsBothHalves(t *testing.T) {
	t.Parallel()
	k := NewKeyFromBytes([16]byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
	})
	v := uint64(0xFFFFFFFFFFFFFFFF)
	got := k.WithVary(v)
	require.Equal(t, k.Hash64()^v, got.Hash64())
	require.Equal(t, binary.BigEndian.Uint64(k[8:])^v, binary.BigEndian.Uint64(got[8:]))
}

func TestWithVaryZeroIsIdentity(t *testing.T) {
	t.Parallel()
	k := fromUint64(42)
	require.Equal(t, k, k.WithVary(0))
}

func TestIsZero(t *testing.T) {
	t.Parallel()
	require.True(t, Key{}.IsZero())
	require.False(t, fromUint64(1).IsZero())
}

func TestHexIs32Chars(t *testing.T) {
	t.Parallel()
	k := fromUint64(0x42)
	s := k.Hex()
	require.Len(t, s, 32)
	require.Equal(t, k.Hex(), k.String())
}

func TestSingleFlightKeyDistinguishesSuffix(t *testing.T) {
	t.Parallel()
	k := fromUint64(0xCAFEBABE)
	base := k.SingleFlightKey(0)
	reval := k.SingleFlightKey(1)
	require.NotEqual(t, base, reval)
	require.Len(t, base, 32)
}

func TestKeyJSONRoundTrip(t *testing.T) {
	t.Parallel()
	k := NewKeyFromBytes([16]byte{
		0x00, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, // hi = 0xDEADBEEF
		0x00, 0x00, 0x00, 0x00, 0x12, 0x34, 0x56, 0x78, // lo = 0x12345678
	})
	out, err := json.Marshal(k)
	require.NoError(t, err)
	require.Equal(t, `[3735928559,305419896]`, string(out))

	var got Key
	require.NoError(t, json.Unmarshal(out, &got))
	require.Equal(t, k, got)
}

func TestKeyUnmarshalJSONRejectsBadInput(t *testing.T) {
	t.Parallel()
	var k Key
	err := json.Unmarshal([]byte(`"deadbeef"`), &k)
	require.Error(t, err)
	err = json.Unmarshal([]byte(`["a","b"]`), &k)
	require.Error(t, err)
}
