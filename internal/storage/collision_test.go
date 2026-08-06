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
// collide on the primary 64-bit hash but differ on the secondary hash
// (issue #51).
func TestCollisionDetection_HotStoreRejectsKey2Mismatch(t *testing.T) {
	t.Parallel()
	store := NewHotStore(HotConfig{MaxBytes: 1 << 20})
	ctx := context.Background()

	// Simulate a collision: same primary hash, different Hash2.
	primary := api.KeyFromPrimary(42)
	obj1 := &api.Object{
		Key:        api.KeyFromPrimary(42).WithGuard(111),
		StatusCode: 200,
		Header:     header.FromHTTP(nil),
		Body:       []byte("body-A"),
		BodySize:   6,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	require.NoError(t, store.Put(ctx, primary, obj1))

	// Lookup with a different Hash2 must return a miss, not body-A.
	got, _, err := store.Get(ctx, api.KeyFromPrimary(42).WithGuard(222))
	require.NoError(t, err)
	assert.Nil(t, got, "collision must return miss, not wrong body")

	// Lookup with the correct Hash2 returns body-A.
	got, _, err = store.Get(ctx, api.KeyFromPrimary(42).WithGuard(111))
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "body-A", string(got.Body))
}

// TestCollisionDetection_HotStoreOverwritesCollidingEntry verifies
// that storing a second entry with the same primary hash but different
// Hash2 overwrites the first (ping-pong degradation, not wrong content).
func TestCollisionDetection_HotStoreOverwritesCollidingEntry(t *testing.T) {
	t.Parallel()
	store := NewHotStore(HotConfig{MaxBytes: 1 << 20})
	ctx := context.Background()

	primary := api.KeyFromPrimary(99)
	obj1 := &api.Object{
		Key:        api.KeyFromPrimary(99).WithGuard(111),
		StatusCode: 200,
		Header:     header.FromHTTP(nil),
		Body:       []byte("body-A"),
		BodySize:   6,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	obj2 := &api.Object{
		Key:        api.KeyFromPrimary(99).WithGuard(222),
		StatusCode: 200,
		Header:     header.FromHTTP(nil),
		Body:       []byte("body-B"),
		BodySize:   6,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}

	require.NoError(t, store.Put(ctx, primary, obj1))
	require.NoError(t, store.Put(ctx, primary, obj2))

	// obj2 overwrote obj1; only Hash2=222 can find it.
	got, _, err := store.Get(ctx, api.KeyFromPrimary(99).WithGuard(222))
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "body-B", string(got.Body))

	// Hash2=111 now misses (entry was replaced).
	got, _, err = store.Get(ctx, api.KeyFromPrimary(99).WithGuard(111))
	require.NoError(t, err)
	assert.Nil(t, got)
}
