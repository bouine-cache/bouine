package cache

import (
	"testing"

	"github.com/bouine-cache/bouine/pkg/api"
)

// canonicalKey is a representative canonical buffer (~30 bytes) matching
// the typical scheme|host|path|query|method length produced by BuildKey
// for a short URL.
var canonicalKey = []byte("https|example.com|/path|q=1|GET")

// BenchmarkNewKey measures the constructor: one xxhash.Sum64 for the
// primary plus one xxhash.NewWithSeed Digest for the guard. Escape
// analysis stack-allocates the Digest, so this is zero-allocation.
// This is the path wired into BuildKey.
func BenchmarkNewKey(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		_ = NewKey(canonicalKey)
	}
}

// TestNewKeyNonZero confirms NewKey produces a non-zero key with a
// guard distinct from the primary (the two hashes are independent).
func TestNewKeyNonZero(t *testing.T) {
	t.Parallel()
	k := NewKey(canonicalKey)
	if k.IsZero() {
		t.Fatal("NewKey produced a zero key")
	}
	if k.Primary() == k.Guard() {
		t.Fatalf("primary and guard must differ: both %d", k.Primary())
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
