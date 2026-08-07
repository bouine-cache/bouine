// Package testkey provides factories for constructing [api.Key] values
// in tests. [Key] builds a key from a plain integer (low half only,
// zero high half — distinct but not collision-resistant). [Hash] builds
// a real dual-xxhash64 key from bytes, mirroring [cache.NewKey] for
// tests that need realistic keys without importing the cache layer.
// Production key construction MUST go through [cache.NewKey].
package testkey

import (
	"encoding/binary"

	"github.com/cespare/xxhash/v2"

	"github.com/bouine-cache/bouine/pkg/api"
)

// key2Seed is the seed for the second xxhash64 half. Duplicated from
// internal/cache.key2Seed because testutil cannot import cache.
const key2Seed uint64 = 0x626f75696e6532 // "bouine2"

// Key builds an [api.Key] with n in the low half and a zeroed high half.
// Accepts uint64 so callers can pass loop counters and hash values
// without a cast; untyped int constants are converted automatically.
func Key(n uint64) api.Key {
	var k api.Key
	binary.LittleEndian.PutUint64(k[:8], n)
	return k
}

// Hash computes a 128-bit [api.Key] from b using the same dual-xxhash64
// scheme as [cache.NewKey]. Use this when tests need realistic keys
// (e.g. shard distribution, eviction with distinct string-derived keys)
// rather than integer-labelled keys from [Key].
func Hash(b []byte) api.Key {
	var k api.Key
	binary.LittleEndian.PutUint64(k[:8], xxhash.Sum64(b))
	g := xxhash.NewWithSeed(key2Seed)
	_, _ = g.Write(b)
	binary.LittleEndian.PutUint64(k[8:], g.Sum64())
	return k
}
