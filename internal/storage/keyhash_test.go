package storage

import (
	"github.com/cespare/xxhash/v2"

	"github.com/bouine-cache/bouine/pkg/api"
)

// key2Seed is the seed for the collision-guard hash. It mirrors
// internal/cache.key2Seed so that keys produced by keyHash are
// structurally identical to keys produced by cache.NewKey for the same
// canonical bytes. Duplicating one constant is cheaper than a layering
// violation (storage cannot import cache).
const key2Seed uint64 = 0x626f75696e6532

// keyHash computes a full cache key (primary + guard) from a byte slice.
// It is the storage-package test analogue of cache.NewKey: two
// independent xxhash64 calls over the same bytes, the second with a
// different seed. Use this instead of api.KeyFromPrimary so tests
// exercise the guard-verification path with a non-zero guard.
func keyHash(b []byte) api.Key {
	g := xxhash.NewWithSeed(key2Seed)
	_, _ = g.Write(b)
	return api.NewKeyFromHashes(xxhash.Sum64(b), g.Sum64())
}
