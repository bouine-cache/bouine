package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
)

// fuzzFixedTime is a deterministic timestamp for fuzz round-trip tests.
// Using a fixed time instead of time.Now() ensures reproducibility and
// complies with AGENTS.md §8: "no time.Now() in tests."
var fuzzFixedTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// FuzzPurgeCodecRoundTrip fuzzes the purge event encode/decode round-trip
// for both gossip and HTTP transport frames. Encoding then decoding must
// reproduce the original event exactly.
func FuzzPurgeCodecRoundTrip(f *testing.F) {
	f.Add("", "node-1", uint64(0))
	f.Add("accept-encoding=gzip", "node-1", uint64(42))
	f.Add("", "", uint64(1))
	f.Add("vary-key-with-special-chars", "node-with-long-name", uint64(18446744073709551615))

	f.Fuzz(func(t *testing.T, varyKey, issuer string, seq uint64) {
		orig := api.PurgeEvent{
			Key:      api.Key{},
			VaryKey:  varyKey,
			Issuer:   issuer,
			IssuedAt: fuzzFixedTime,
			Seq:      seq,
		}

		// Gossip round-trip. Encode can fail if strings exceed 64 KiB.
		gossipBytes, err := EncodePurgeGossip(orig)
		if err != nil {
			t.Skipf("encode gossip: %v", err)
		}
		gotGossip, err := DecodePurgeGossip(gossipBytes)
		require.NoError(t, err, "decode gossip")
		assertPurgeEqual(t, gotGossip, orig)

		// HTTP round-trip.
		httpBytes, err := EncodePurgeHTTP(orig)
		if err != nil {
			t.Skipf("encode http: %v", err)
		}
		gotHTTP, err := DecodePurgeHTTP(httpBytes)
		require.NoError(t, err, "decode http")
		assertPurgeEqual(t, gotHTTP, orig)
	})
}

// FuzzBanCodecRoundTrip fuzzes the ban event encode/decode round-trip for
// both gossip and HTTP transport frames. Encoding then decoding must
// reproduce the original event exactly.
func FuzzBanCodecRoundTrip(f *testing.F) {
	f.Add("example\\.com", "/api/.*", "", "node-1", uint64(0))
	f.Add("", "", "product-42", "node-2", uint64(99))
	f.Add("host.*", "", "", "", uint64(1))
	f.Add("", "", "", "", uint64(18446744073709551615))

	f.Fuzz(func(t *testing.T, hostRegex, pathRegex, surrogateKey, issuer string, seq uint64) {
		orig := api.BanEvent{
			Predicate: api.BanExpr{
				HostRegex:    hostRegex,
				PathRegex:    pathRegex,
				SurrogateKey: surrogateKey,
				CreatedAt:    fuzzFixedTime,
			},
			Issuer:   issuer,
			IssuedAt: fuzzFixedTime,
			Seq:      seq,
		}

		// Gossip round-trip. Encode can fail if strings exceed 64 KiB.
		gossipBytes, err := EncodeBanGossip(orig)
		if err != nil {
			t.Skipf("encode gossip: %v", err)
		}
		gotGossip, err := DecodeBanGossip(gossipBytes)
		require.NoError(t, err, "decode gossip")
		assertBanEqual(t, gotGossip, orig)

		// HTTP round-trip.
		httpBytes, err := EncodeBanHTTP(orig)
		if err != nil {
			t.Skipf("encode http: %v", err)
		}
		gotHTTP, err := DecodeBanHTTP(httpBytes)
		require.NoError(t, err, "decode http")
		assertBanEqual(t, gotHTTP, orig)
	})
}

// FuzzDecodeArbitrary fuzzes all four cluster decode functions with
// arbitrary bytes to ensure none of them panic on corrupt or malformed
// input. Panic safety is the only property under test — cluster decoders
// return value types (not pointers), so there is no nil-with-nil-error
// invariant to check.
func FuzzDecodeArbitrary(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x42})
	f.Add([]byte{0x42, 0x03})
	f.Add([]byte{0x42, 0x03, 0x01})
	f.Add([]byte{0x42, 0x03, 0x02})
	f.Add([]byte{0x42, 0x00, 0x01})
	f.Add([]byte{0x00, 0x00, 0x00})
	f.Add([]byte("not a binary frame"))
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	validPurge, _ := EncodePurgeGossip(api.PurgeEvent{
		Key:      api.Key{},
		Issuer:   "node-1",
		IssuedAt: fuzzFixedTime,
		Seq:      1,
	})
	f.Add(validPurge)
	f.Add(validPurge[:4])
	f.Add(append(validPurge, 0xFF, 0xFF))

	validBan, _ := EncodeBanGossip(api.BanEvent{
		Predicate: api.BanExpr{HostRegex: "example.com"},
		Issuer:    "node-1",
		IssuedAt:  fuzzFixedTime,
		Seq:       1,
	})
	f.Add(validBan)
	f.Add(validBan[:4])

	f.Fuzz(func(t *testing.T, data []byte) {
		// All four decode functions must never panic on arbitrary
		// input. Unlike the storage codec, cluster decoders return
		// value types (not pointers), so there is no nil-with-nil-error
		// invariant to check — the panic safety is the only property.
		DecodePurgeGossip(data)
		DecodePurgeHTTP(data)
		DecodeBanGossip(data)
		DecodeBanHTTP(data)
	})
}

// assertPurgeEqual compares two PurgeEvents, using time.Equal for time
// fields because the cluster codec encodes time as UnixNano and decodes
// via time.Unix, which returns local time — the instant is preserved but
// the location differs from the UTC original.
func assertPurgeEqual(t *testing.T, got, want api.PurgeEvent) {
	t.Helper()
	if got.Key != want.Key {
		t.Errorf("Key mismatch: got %v want %v", got.Key, want.Key)
	}
	if got.VaryKey != want.VaryKey {
		t.Errorf("VaryKey mismatch: got %q want %q", got.VaryKey, want.VaryKey)
	}
	if got.Issuer != want.Issuer {
		t.Errorf("Issuer mismatch: got %q want %q", got.Issuer, want.Issuer)
	}
	if !got.IssuedAt.Equal(want.IssuedAt) {
		t.Errorf("IssuedAt mismatch: got %v want %v", got.IssuedAt, want.IssuedAt)
	}
	if got.Seq != want.Seq {
		t.Errorf("Seq mismatch: got %d want %d", got.Seq, want.Seq)
	}
}

// assertBanEqual compares two BanEvents with the same time.Equal semantics
// as assertPurgeEqual.
func assertBanEqual(t *testing.T, got, want api.BanEvent) {
	t.Helper()
	if got.Predicate.HostRegex != want.Predicate.HostRegex {
		t.Errorf("HostRegex mismatch: got %q want %q", got.Predicate.HostRegex, want.Predicate.HostRegex)
	}
	if got.Predicate.PathRegex != want.Predicate.PathRegex {
		t.Errorf("PathRegex mismatch: got %q want %q", got.Predicate.PathRegex, want.Predicate.PathRegex)
	}
	if got.Predicate.SurrogateKey != want.Predicate.SurrogateKey {
		t.Errorf("SurrogateKey mismatch: got %q want %q", got.Predicate.SurrogateKey, want.Predicate.SurrogateKey)
	}
	if !got.Predicate.CreatedAt.Equal(want.Predicate.CreatedAt) {
		t.Errorf("CreatedAt mismatch: got %v want %v", got.Predicate.CreatedAt, want.Predicate.CreatedAt)
	}
	if got.Issuer != want.Issuer {
		t.Errorf("Issuer mismatch: got %q want %q", got.Issuer, want.Issuer)
	}
	if !got.IssuedAt.Equal(want.IssuedAt) {
		t.Errorf("IssuedAt mismatch: got %v want %v", got.IssuedAt, want.IssuedAt)
	}
	if got.Seq != want.Seq {
		t.Errorf("Seq mismatch: got %d want %d", got.Seq, want.Seq)
	}
}
