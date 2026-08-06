package api

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
)

// Key is the canonical 128-bit cache key: two independent xxhash64
// digests of the canonical request bytes (scheme + host + path + query
// + method + Vary headers), packed little-endian into 16 bytes. The
// first 8 bytes (k[:8]) are the primary xxhash64; the second 8 bytes
// (k[8:]) are an xxhash64 computed with a fixed seed. The full 16-byte
// array is the map key, so a single map lookup is a 128-bit collision
// check — no separate guard field, no guard verification, no
// Primary/Guard accessor split. Birthday bound is ~2^64 objects.
//
// The first 8 bytes are a uniform hash; ring ownership, shard
// selection, and log sampling derive from k[:8] rather than hashing
// all 16 bytes (a second hash call on the hot path would buy nothing
// in distribution).
//
// Unstable until the 128-bit key migration lands across the cluster
// wire formats (codec v3, WAL v3, peer fetch v2, snapshot v2, purge v2).
type Key [16]byte

// NewKeyFromBytes constructs a Key from a raw 16-byte array. Intended
// for decoders that read the key off the wire or off disk. Callers that
// need a key from canonical request bytes should use cache.NewKey,
// which computes both xxhash64 halves.
func NewKeyFromBytes(b [16]byte) Key { return Key(b) }

// NewKeyFromUint64 builds a Key with v in the low half and a zeroed
// high half. Intended for tests and diagnostics where distinctness
// matters but 128-bit collision resistance does not. Production key
// construction MUST go through cache.NewKey.
func NewKeyFromUint64(v uint64) Key {
	var k Key
	binary.LittleEndian.PutUint64(k[:8], v)
	return k
}

// WithVary returns a variant key derived by XORing varyHash into both
// 8-byte halves. Because both halves are independent xxhash64 values,
// mixing both by the 64-bit vary hash preserves the 128-bit collision
// resistance. The vary dimension itself remains 64-bit (a single
// xxhash64 of the Vary header set), unchanged from the prior uint64
// key design.
func (k Key) WithVary(varyHash uint64) Key {
	p := k.Hash64() ^ varyHash
	g := binary.LittleEndian.Uint64(k[8:]) ^ varyHash
	var result Key
	binary.LittleEndian.PutUint64(result[:8], p)
	binary.LittleEndian.PutUint64(result[8:], g)
	return result
}

// SingleFlightKey returns a string key for request collapsing. The
// suffix is XORed into the first 8 bytes so revalidation singleflight
// (suffix != 0) is distinguished from fetch singleflight (suffix == 0)
// without hashing the full key. The result is the 32-char hex of the
// (possibly suffix-mixed) key.
func (k Key) SingleFlightKey(suffix uint64) string {
	var x Key
	binary.LittleEndian.PutUint64(x[:8], k.Hash64()^suffix)
	copy(x[8:], k[8:])
	return hex.EncodeToString(x[:])
}

// IsZero reports whether the key is the all-zero value (the zero Key
// returned by BuildKey on empty input). Used to gate sampling and
// access logging on the miss path.
func (k Key) IsZero() bool {
	for i := range k {
		if k[i] != 0 {
			return false
		}
	}
	return true
}

// Hash64 returns the first 8 bytes as a little-endian uint64 — the
// primary xxhash64 half. It is a uniform hash suitable for shard
// selection, consistent-hash ring ownership, and log sampling without
// re-hashing the full 16 bytes. Callers that need the full 128-bit
// collision check MUST use the Key directly as a map key, not Hash64.
func (k Key) Hash64() uint64 { return binary.LittleEndian.Uint64(k[:8]) }

// Hex returns the 32-char lowercase hex of all 16 key bytes.
func (k Key) Hex() string { return hex.EncodeToString(k[:]) }

// String returns the 32-char lowercase hex, satisfying fmt.Stringer.
func (k Key) String() string { return k.Hex() }

// LogValue renders the key as its 32-char hex in slog output. This
// allocates one string per log record, which is acceptable: the
// hit-path access log is sampled (1:100) and miss/error paths are not
// hot. Operator grep patterns change from a 16-char hex to a 32-char
// hex; see docs/runbook for the migration note.
func (k Key) LogValue() slog.Value { return slog.StringValue(k.String()) }

// MarshalJSON emits the key as a 2-element decimal JSON array
// [lo, hi] where lo and hi are the little-endian uint64 halves. The
// array form (rather than a bare number) makes the 128-bit width
// explicit and unambiguous in admin API output.
func (k Key) MarshalJSON() ([]byte, error) {
	lo := k.Hash64()
	hi := binary.LittleEndian.Uint64(k[8:])
	return json.Marshal([2]uint64{lo, hi})
}

// UnmarshalJSON accepts only the 2-element decimal array form produced
// by MarshalJSON. Nothing emits the old bare-number format, so there is
// no compat path.
func (k *Key) UnmarshalJSON(data []byte) error {
	var pair [2]uint64
	if err := json.Unmarshal(data, &pair); err != nil {
		return fmt.Errorf("api.Key: invalid key array: %w", err)
	}
	binary.LittleEndian.PutUint64(k[:8], pair[0])
	binary.LittleEndian.PutUint64(k[8:], pair[1])
	return nil
}
