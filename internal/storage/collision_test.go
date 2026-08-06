package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// TestCollisionDetection_HotStoreRejectsKey2Mismatch verifies that
// HotStore.Get returns a miss (not wrong content) when two entries
// collide on the primary 64-bit hash but differ on the guard hash
// (issue #51).
func TestCollisionDetection_HotStoreRejectsKey2Mismatch(t *testing.T) {
	t.Parallel()
	store := NewHotStore(HotConfig{MaxBytes: 1 << 20})
	ctx := context.Background()

	// Store an entry with a specific guard.
	key1 := api.KeyFromPrimary(42).WithGuard(111)
	obj1 := &api.Object{
		Key:        key1,
		StatusCode: 200,
		Header:     header.FromHTTP(nil),
		Body:       []byte("body-A"),
		BodySize:   6,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	require.NoError(t, store.Put(ctx, key1, obj1))

	// Lookup with a different guard must return a miss, not body-A.
	got, _, err := store.Get(ctx, api.KeyFromPrimary(42).WithGuard(222))
	require.NoError(t, err)
	assert.Nil(t, got, "collision must return miss, not wrong body")

	// Lookup with the correct guard returns body-A.
	got, _, err = store.Get(ctx, key1)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "body-A", string(got.Body))
}

// TestCollisionDetection_HotStoreOverwritesCollidingEntry verifies
// that storing a second entry with the same primary hash but different
// guard overwrites the first (ping-pong degradation, not wrong content).
func TestCollisionDetection_HotStoreOverwritesCollidingEntry(t *testing.T) {
	t.Parallel()
	store := NewHotStore(HotConfig{MaxBytes: 1 << 20})
	ctx := context.Background()

	key1 := api.KeyFromPrimary(99).WithGuard(111)
	obj1 := &api.Object{
		Key:        key1,
		StatusCode: 200,
		Header:     header.FromHTTP(nil),
		Body:       []byte("body-A"),
		BodySize:   6,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	key2 := api.KeyFromPrimary(99).WithGuard(222)
	obj2 := &api.Object{
		Key:        key2,
		StatusCode: 200,
		Header:     header.FromHTTP(nil),
		Body:       []byte("body-B"),
		BodySize:   6,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}

	require.NoError(t, store.Put(ctx, key1, obj1))
	require.NoError(t, store.Put(ctx, key2, obj2))

	// obj2 overwrote obj1; only guard=222 can find it.
	got, _, err := store.Get(ctx, key2)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "body-B", string(got.Body))

	// guard=111 now misses (entry was replaced).
	got, _, err = store.Get(ctx, key1)
	require.NoError(t, err)
	assert.Nil(t, got)
}
