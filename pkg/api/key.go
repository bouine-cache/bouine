package api

import (
	"encoding/json"
	"log/slog"
	"strconv"
)

// Key is the canonical cache key. It carries two independent 64-bit
// hashes of the normalized request attributes (scheme + host + path +
// query + method): the primary hash is the map index; the guard hash is
// the collision guard verified on every Get (issue #51). Together they
// provide 128-bit collision resistance (birthday bound ~2^64 objects).
//
// The two hashes are encapsulated: callers must not reach in and read or
// construct them directly. Use cache.NewKey / cache.BuildKey to build a
// key from a canonical request; NewKeyFromHashes is the low-level
// constructor for the hashing layer, and KeyFromPrimary reconstructs a
// primary-only key (guard 0) for the cases where the guard is genuinely
// unavailable (see its doc). The With* methods derive related keys.
//
// The zero-value Key (both hashes 0) represents an unset/invalid key.
type Key struct {
	hash  uint64
	hash2 uint64
}

// NewKeyFromHashes builds a Key from two precomputed hashes: primary is
// the map-index hash, guard is the collision-guard hash. The two inputs
// must be statistically independent (e.g. two xxhash64 calls with
// different seeds). This constructor is for the hashing layer; ordinary
// callers should obtain keys from cache.BuildKey / cache.NewKey.
func NewKeyFromHashes(primary, guard uint64) Key {
	return Key{hash: primary, hash2: guard}
}

// KeyFromPrimary reconstructs a Key from a stored primary hash with a
// zero guard. The resulting key will MISS on any Get that verifies the
// guard (guard 0 ≠ stored guard). Use only when the guard is genuinely
// unavailable: warm-tier key unions (warm stores uint64 primaries, not
// full keys), v1 backward-compat decoders (no guard field on the wire),
// purge events (which carry only the primary), and eviction callbacks
// (which receive a uint64 from the warm tier). For decoders that have
// both hashes, use NewKeyFromHashes instead.
func KeyFromPrimary(h uint64) Key { return Key{hash: h} }

// Primary returns the primary map-index hash. This is the only accessor
// for the value used to shard and index the hot-tier map; storage is the
// sole legitimate consumer.
func (k Key) Primary() uint64 { return k.hash }

// Guard returns the collision-guard hash. Storage verifies this against
// the stored entry's guard on every Get to detect primary-hash
// collisions (issue #51).
func (k Key) Guard() uint64 { return k.hash2 }

// WithGuard returns a copy of k with the guard replaced. Used by
// decoders that read the guard from bytes after the primary.
func (k Key) WithGuard(g uint64) Key { return Key{hash: k.hash, hash2: g} }

// WithVary returns a variant key derived from k by XORing both hashes
// with varyHash. This keeps the variant's collision guard independent
// from the primary's, so a vary-driven variant cannot collide with a
// different primary's variant.
func (k Key) WithVary(varyHash uint64) Key {
	return Key{hash: k.hash ^ varyHash, hash2: k.hash2 ^ varyHash}
}

// SingleFlightKey returns a stable string for singleflight deduplication
// of concurrent fetches for k. suffix is XORed into the primary before
// formatting so distinct dedup buckets (e.g. fetch vs revalidation) can
// share the same cache key without colliding. Pass 0 for a plain fetch.
func (k Key) SingleFlightKey(suffix uint64) string {
	return strconv.FormatUint(k.hash^suffix, 36) + ":" + strconv.FormatUint(k.hash2, 36)
}

// SameGuard reports whether k and other share the same guard hash.
// Used by the warm-tier load path to detect a collision without
// comparing the primary (the primary already matched the map index).
func (k Key) SameGuard(other Key) bool { return k.hash2 == other.hash2 }

// IsZero reports whether the key is the zero value (both hashes 0).
func (k Key) IsZero() bool { return k.hash == 0 && k.hash2 == 0 }

// LogValue renders the key as a lowercase hex string in slog output so
// it matches the xxhash64 hex form used in admin API responses, storage
// paths, and runbook examples. This allocates one string per log record,
// which is acceptable: the hit-path access log is sampled (1:100) and
// miss/error paths are not hot.
func (k Key) LogValue() slog.Value { return slog.StringValue(k.Hex()) }

// Hex returns the lowercase hex representation of the primary hash.
// Intended for admin API responses, log output, and runbook examples.
func (k Key) Hex() string { return strconv.FormatUint(k.hash, 16) }

// String returns the lowercase hex representation of the primary hash,
// satisfying fmt.Stringer.
func (k Key) String() string { return k.Hex() }

// MarshalJSON serialises the key as a two-element JSON array
// [hash, hash2] to preserve backward compatibility with admin API
// consumers that parse the key as a JSON number: the first element is
// the primary hash (same value as before), the second is the
// collision guard.
func (k Key) MarshalJSON() ([]byte, error) {
	return []byte("[" + strconv.FormatUint(k.hash, 10) + "," + strconv.FormatUint(k.hash2, 10) + "]"), nil
}

// UnmarshalJSON deserialises the key from a JSON array [hash, hash2].
// For backward compatibility, a bare JSON number is interpreted as the
// primary hash with a zero guard.
func (k *Key) UnmarshalJSON(data []byte) error {
	s := string(data)
	if len(s) > 0 && s[0] == '[' {
		var arr [2]uint64
		if err := json.Unmarshal(data, &arr); err != nil {
			return err
		}
		k.hash = arr[0]
		k.hash2 = arr[1]
		return nil
	}
	var v uint64
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	k.hash = v
	k.hash2 = 0
	return nil
}
