package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
)

// TestHotStore_128BitKeyPreventsCollision verifies that two keys sharing
// the same first 8 bytes (which would collide under the old uint64 key)
// are distinct in the hot store. The second 8 bytes of the 128-bit key
// make the map lookup itself a full collision check.
func TestHotStore_128BitKeyPreventsCollision(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	ctx := context.Background()

	// Construct two keys that share the first 8 bytes but differ in
	// the second 8. Under the old uint64 key these would be identical;
	// with [16]byte they are distinct.
	key1 := api.Key{}
	key1[7] = 0x42  // first 8 bytes = [0,0,0,0,0,0,0,0x42]
	key1[15] = 0xAA // second 8 bytes differ

	key2 := api.Key{}
	key2[7] = 0x42  // same first 8 bytes
	key2[15] = 0xBB // different second 8 bytes

	require.NotEqual(t, key1, key2)

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
