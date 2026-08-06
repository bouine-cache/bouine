package cache

import (
	"testing"

	"github.com/bouine-cache/bouine/pkg/api"
)

// canonicalKey is a representative canonical buffer (~30 bytes) matching
// the typical scheme|host|path|query|method length produced by BuildKey
// for a short URL.
var canonicalKey = []byte("https|example.com|/path|q=1|GET")

// BenchmarkNewKey measures the default constructor (option A): one
// xxhash.Sum64 for the primary plus one xxhash.NewWithSeed Digest for
// the guard. Escape analysis stack-allocates the Digest, so this is
// zero-allocation. This is the path wired into BuildKey.
func BenchmarkNewKey(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		_ = NewKey(canonicalKey)
	}
}

// BenchmarkNewKeyZeroAlloc measures the alternative constructor
// (option B): two xxhash.Sum64 calls over a stack buffer (the guard
// input is canonical||seed). Zero allocations on the common path
// (canonical ≤ 512 bytes). Not wired into BuildKey; see NewKeyZeroAlloc
// godoc for the migration caveat (different guard hash values).
func BenchmarkNewKeyZeroAlloc(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		_ = NewKeyZeroAlloc(canonicalKey)
	}
}

// TestNewKeyDistinctHashes confirms the two constructors produce
// distinct guard values (they are different functions and must not be
// mixed within a deployment) but the same primary.
func TestNewKeyDistinctHashes(t *testing.T) {
	t.Parallel()
	a := NewKey(canonicalKey)
	z := NewKeyZeroAlloc(canonicalKey)
	if a.Primary() != z.Primary() {
		t.Fatalf("primary mismatch: %d vs %d", a.Primary(), z.Primary())
	}
	if a.Guard() == z.Guard() {
		t.Fatalf("guards must differ between constructors: both %d", a.Guard())
	}
	if a.IsZero() {
		t.Fatal("NewKey produced a zero key")
	}
}

// TestNewKeyFromHashesRoundTrip confirms NewKeyFromHashes + WithGuard
// reconstructs a key whose accessors return the same values.
func TestNewKeyFromHashesRoundTrip(t *testing.T) {
	t.Parallel()
	k := api.NewKeyFromHashes(42, 111)
	if k.Primary() != 42 {
		t.Fatalf("Primary: got %d want 42", k.Primary())
	}
	if k.Guard() != 111 {
		t.Fatalf("Guard: got %d want 111", k.Guard())
	}
}

// TestWithVary confirms both hashes are XORed with the vary hash.
func TestWithVary(t *testing.T) {
	t.Parallel()
	k := api.KeyFromPrimary(0x0a).WithGuard(0x0b)
	v := k.WithVary(0x0f)
	if v.Primary() != (0x0a^0x0f) || v.Guard() != (0x0b^0x0f) {
		t.Fatalf("WithVary: primary=%d guard=%d", v.Primary(), v.Guard())
	}
}

// TestSameGuard confirms the warm-tier collision check helper.
func TestSameGuard(t *testing.T) {
	t.Parallel()
	a := api.KeyFromPrimary(1).WithGuard(7)
	b := api.KeyFromPrimary(2).WithGuard(7)
	c := api.KeyFromPrimary(1).WithGuard(8)
	if !a.SameGuard(b) {
		t.Fatal("a and b share guard 7")
	}
	if a.SameGuard(c) {
		t.Fatal("a and c have different guards")
	}
}
