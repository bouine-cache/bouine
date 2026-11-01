package storage

import (
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

// TestHotStore_PutOwnedAliasesTransferredBody pins the ownership contract
// of PutOwned with the slab disabled (the default): the transferred body
// IS the stored body — no second heap copy. The fill paths rely on this:
// their fetch already builds an exact-size, cache-owned copy, and cloning
// it again inside the store held 84.7% of inuse heap (479.86MB) on the
// rc1 5krps profile.
func TestHotStore_PutOwnedAliasesTransferredBody(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})

	k := testkey.Hash([]byte("putowned-aliases"))
	o := obj(k, 0)
	body := []byte("transferred body bytes")
	o.Body = body
	o.BodySize = int64(len(body))

	require.NoError(t, s.PutOwned(context.Background(), k, o))

	got, src, err := s.Get(context.Background(), k)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, api.SourceHot, src)
	require.Equal(t, body, got.Body)
	require.True(t, &got.Body[0] == &body[0],
		"with the slab off, PutOwned must alias the transferred body, not clone it")
}

// TestHotStore_PutOwnedWithSlabStoresSlabMemory pins the slab interaction
// on Linux: the stored body must be slab memory, never the transferred
// slice — eviction returns slab regions to the free list while flight
// followers may still hold the transferred bytes, so aliasing them into
// the stored entry would be a use-after-free under eviction. Elsewhere
// the allocator is a no-op and PutOwned aliases by design.
func TestHotStore_PutOwnedWithSlabStoresSlabMemory(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("slab allocator is Linux-only; the no-op elsewhere aliases by design")
	}
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4, Slab: true})

	k := testkey.Hash([]byte("putowned-slab"))
	o := obj(k, 0)
	body := []byte("slab must own the stored bytes")
	o.Body = body
	o.BodySize = int64(len(body))

	require.NoError(t, s.PutOwned(context.Background(), k, o))

	got, _, err := s.Get(context.Background(), k)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, body, got.Body)
	require.False(t, &got.Body[0] == &body[0],
		"with the slab on, the stored body must be slab memory, not the transferred slice")
}
