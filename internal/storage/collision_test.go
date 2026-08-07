package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

// TestHotStore_KeysWithSameFirstHalfAreDistinct pins the design property
// that the hot-tier map is keyed by the full 16-byte [api.Key], not by a
// uint64 derivation of its first half. Two keys that share k[:8] (which
// is what shard selection, ring hashing, and sampling use) but differ in
// k[8:] MUST be distinct map entries — otherwise a regression to a
// uint64-keyed map would reintroduce the wrong-body class of bug that
// issue #51 fixed.
//
// This is a map-semantics pin, not a hash-collision proof. It guards
// against someone "optimizing" the index back to map[uint64] by hashing
// only the first half.
func TestHotStore_KeysWithSameFirstHalfAreDistinct(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	ctx := context.Background()

	// Construct two keys that share the first 8 bytes but differ in
	// the second 8. Under the old uint64 key these would be identical.
	key1 := api.Key{}
	key1[7] = 0x42  // first 8 bytes = [0,0,0,0,0,0,0,0x42]
	key1[15] = 0xAA // second 8 bytes differ

	key2 := api.Key{}
	key2[7] = 0x42  // same first 8 bytes
	key2[15] = 0xBB // different second 8 bytes

	require.NotEqual(t, key1, key2)
	require.Equal(t, key1.Hash64(), key2.Hash64(), "first halves must match")

	// Put with key1.
	err := s.Put(ctx, key1, obj(key1, 100))
	require.NoError(t, err)

	// Get with key2 must miss — not return key1's object.
	got, _, err := s.Get(ctx, key2)
	require.NoError(t, err)
	require.Nil(t, got, "key2 must not return key1's object")

	// Get with key1 must hit.
	got, _, err = s.Get(ctx, key1)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, key1, got.Key)
}

// TestNewKey_SecondHalfIndependentOfFirst verifies that testkey.Hash
// (which mirrors cache.NewKey) produces a second half that is an
// independent function of the input, not a copy of the first half. If
// the seed constant were ever set to 0 (or dropped), both halves would
// be identical and the 128-bit key would collapse to 64 bits of real
// collision resistance — silently reintroducing issue #51. This test
// catches that regression.
func TestNewKey_SecondHalfIndependentOfFirst(t *testing.T) {
	t.Parallel()
	inputs := [][]byte{
		[]byte("https://example.com/|/|a=1|GET"),
		[]byte("https://example.com/|/|b=2|GET"),
		[]byte("https://example.com/path|/|q=hello world|GET"),
		[]byte("x"),
	}
	for _, in := range inputs {
		k := testkey.Hash(in)
		require.NotEqual(t, k.Hash64(), 0, "first half must be non-zero for non-empty input: %q", in)
		// The second half must differ from the first for a non-trivial
		// input — otherwise the seed did nothing and the two halves
		// are the same hash, giving 64-bit (not 128-bit) collision
		// resistance.
		require.NotEqualf(t, k.Hash64(), uint64le(k[8:]),
			"both halves identical for %q — seed %d did not produce an independent second hash",
			in, api.Key2Seed)
	}
}

// uint64le reads 8 bytes as a little-endian uint64.
func uint64le(b []byte) uint64 {
	_ = b[7]
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}
