// Package testkey provides a factory for constructing [api.Key] values
// from plain integers in tests. The high half is always zeroed, so
// distinctness is preserved but 128-bit collision resistance is not —
// production key construction MUST go through [cache.NewKey].
package testkey

import (
	"encoding/binary"

	"github.com/bouine-cache/bouine/pkg/api"
)

// Key builds an [api.Key] with n in the low half and a zeroed high half.
// Accepts uint64 so callers can pass loop counters and hash values
// without a cast; untyped int constants are converted automatically.
func Key(n uint64) api.Key {
	var k api.Key
	binary.LittleEndian.PutUint64(k[:8], n)
	return k
}
