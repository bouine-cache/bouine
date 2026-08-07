package storage

import (
	"context"
	"encoding/binary"
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

// TestNewKey_HighAndLowHalvesDiffer verifies that testkey.Hash (which
// mirrors cache.NewKey) produces a low half that differs from the high
// half for non-trivial inputs. XXH128 computes two independent 64-bit
// halves (high and low); if a bug caused them to be identical, the
// 128-bit key would collapse to 64 bits of real collision resistance —
// silently reintroducing issue #51. This test catches that regression.
func TestNewKey_HighAndLowHalvesDiffer(t *testing.T) {
	t.Parallel()
	inputs := [][]byte{
		[]byte("https://example.com/|/|a=1|GET"),
		[]byte("https://example.com/|/|b=2|GET"),
		[]byte("https://example.com/path|/|q=hello world|GET"),
		[]byte("x"),
	}
	for _, in := range inputs {
		k := testkey.Hash(in)
		hi := k.Hash64()
		lo := binary.BigEndian.Uint64(k[8:])
		require.NotEqual(t, hi, 0, "high half must be non-zero for non-empty input: %q", in)
		require.NotEqualf(t, hi, lo,
			"high and low halves identical for %q — XXH128 produced a degenerate hash",
			in)
	}
}
