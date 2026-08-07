// Package testkey provides factories for constructing [api.Key] values
// in tests. [Key] builds a key from a plain integer (high half only,
// zero low half — distinct but not collision-resistant). [Hash] builds
// a real XXH128 key from bytes, mirroring [cache.NewKey] for tests that
// need realistic keys without importing the cache layer.
// Production key construction MUST go through [cache.NewKey].
package testkey

import (
	"encoding/binary"

	"github.com/bouine-cache/xxhash/v3"

	"github.com/bouine-cache/bouine/pkg/api"
)

// Key builds an [api.Key] with n in the high half and a zeroed low half.
// Accepts uint64 so callers can pass loop counters and hash values
// without a cast; untyped int constants are converted automatically.
func Key(n uint64) api.Key {
	var k api.Key
	binary.BigEndian.PutUint64(k[:8], n)
	return k
}

// Hash computes a 128-bit [api.Key] from b using the same XXH128 hash as
// [cache.NewKey]. Use this when tests need realistic keys (e.g. shard
// distribution, eviction with distinct string-derived keys) rather than
// integer-labelled keys from [Key].
func Hash(b []byte) api.Key {
	h := xxhash.Sum128(b)
	return api.NewKeyFromBytes(h.Bytes())
}
