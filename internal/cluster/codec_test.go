package cluster

import (
	"testing"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
)

func TestEncodeDecodeKeySet_RoundTrip(t *testing.T) {
	t.Parallel()
	keys := []api.Key{1, 42, 255, api.Key(1) << 40, api.Key(^uint64(0))}
	buf, err := EncodeKeySet("node-7", keys)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	name, decoded, err := DecodeKeySet(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if name != "node-7" {
		t.Errorf("name = %q", name)
	}
	if len(decoded) != len(keys) {
		t.Fatalf("len = %d, want %d", len(decoded), len(keys))
	}
	for i, k := range keys {
		if decoded[i] != k {
			t.Errorf("[%d] = %d, want %d", i, decoded[i], k)
		}
	}
}

func TestEncodeKeySet_EmptyKeys(t *testing.T) {
	t.Parallel()
	buf, err := EncodeKeySet("solo", nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	name, keys, err := DecodeKeySet(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if name != "solo" {
		t.Errorf("name = %q", name)
	}
	if len(keys) != 0 {
		t.Fatalf("len = %d, want 0", len(keys))
	}
}

func TestEncodeKeySet_EmptyNodeName(t *testing.T) {
	t.Parallel()
	buf, err := EncodeKeySet("", []api.Key{1})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	name, keys, err := DecodeKeySet(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if name != "" {
		t.Errorf("name = %q", name)
	}
	if len(keys) != 1 || keys[0] != 1 {
		t.Fatalf("keys = %v", keys)
	}
}

func TestDecodeKeySet_BadMagic(t *testing.T) {
	t.Parallel()
	buf, _ := EncodeKeySet("x", []api.Key{1})
	buf[0] = 0
	_, _, err := DecodeKeySet(buf)
	if err != errBadMagic {
		t.Fatalf("err = %v, want errBadMagic", err)
	}
}

func TestDecodeKeySet_ShortFrame(t *testing.T) {
	t.Parallel()
	_, _, err := DecodeKeySet([]byte{binaryMagic})
	if err != errShortFrame {
		t.Fatalf("err = %v, want errShortFrame", err)
	}
}

func TestEncodeDecodePurge_RoundTrip(t *testing.T) {
	t.Parallel()
	evt := api.PurgeEvent{
		Key:      api.Key(0xDEADBEEF),
		VaryKey:  "variant-1",
		Issuer:   "node-0",
		IssuedAt: time.Unix(0, 1234567890),
		Seq:      42,
	}
	// gossip
	buf, err := EncodePurgeGossip(evt)
	if err != nil {
		t.Fatalf("encode gossip: %v", err)
	}
	got, err := DecodePurgeGossip(buf)
	if err != nil {
		t.Fatalf("decode gossip: %v", err)
	}
	assertPurgeEqual(t, got, evt)
	// HTTP
	hbuf, err := EncodePurgeHTTP(evt)
	if err != nil {
		t.Fatalf("encode http: %v", err)
	}
	got2, err := DecodePurgeHTTP(hbuf)
	if err != nil {
		t.Fatalf("decode http: %v", err)
	}
	assertPurgeEqual(t, got2, evt)
}

func TestEncodeDecodePurge_EmptyStrings(t *testing.T) {
	t.Parallel()
	evt := api.PurgeEvent{
		Key:      1,
		VaryKey:  "",
		Issuer:   "",
		IssuedAt: time.Unix(0, 1),
		Seq:      0,
	}
	buf, err := EncodePurgeGossip(evt)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodePurgeGossip(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	assertPurgeEqual(t, got, evt)
}

func TestDecodePurge_ShortFrame(t *testing.T) {
	t.Parallel()
	_, err := DecodePurgeGossip([]byte{binaryMagic, binaryVersion, msgTypePurge})
	if err != errShortFrame {
		t.Fatalf("err = %v, want errShortFrame", err)
	}
}

func TestDecodePurge_WrongMsgType(t *testing.T) {
	t.Parallel()
	evt := api.BanEvent{Issuer: "x"}
	buf, _ := EncodeBanGossip(evt)
	_, err := DecodePurgeGossip(buf)
	if err == nil {
		t.Fatal("expected error decoding ban as purge")
	}
}

func TestEncodeDecodeBan_RoundTrip(t *testing.T) {
	t.Parallel()
	evt := api.BanEvent{
		Predicate: api.BanExpr{
			HostRegex:    "example\\.com",
			PathRegex:    "/api/.*",
			SurrogateKey: "sk-1",
			CreatedAt:    time.Unix(0, 999999),
		},
		Issuer:   "node-9",
		IssuedAt: time.Unix(0, 888888),
		Seq:      7,
	}
	// gossip
	buf, err := EncodeBanGossip(evt)
	if err != nil {
		t.Fatalf("encode gossip: %v", err)
	}
	got, err := DecodeBanGossip(buf)
	if err != nil {
		t.Fatalf("decode gossip: %v", err)
	}
	assertBanEqual(t, got, evt)
	// HTTP
	hbuf, err := EncodeBanHTTP(evt)
	if err != nil {
		t.Fatalf("encode http: %v", err)
	}
	got2, err := DecodeBanHTTP(hbuf)
	if err != nil {
		t.Fatalf("decode http: %v", err)
	}
	assertBanEqual(t, got2, evt)
}

func TestEncodeDecodeBan_EmptyPredicate(t *testing.T) {
	t.Parallel()
	evt := api.BanEvent{
		Issuer:   "node-0",
		IssuedAt: time.Unix(0, 1),
		Seq:      1,
	}
	buf, err := EncodeBanGossip(evt)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeBanGossip(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	assertBanEqual(t, got, evt)
}

func TestIsBinaryFrame(t *testing.T) {
	t.Parallel()
	if IsBinaryFrame([]byte("{}")) {
		t.Fatal("JSON should not be binary frame")
	}
	if IsBinaryFrame(nil) {
		t.Fatal("nil should not be binary frame")
	}
	buf, _ := EncodePurgeGossip(api.PurgeEvent{Key: 1})
	if !IsBinaryFrame(buf) {
		t.Fatal("binary purge should be binary frame")
	}
	if GossipMsgType(buf) != msgTypePurge {
		t.Fatalf("msgType = %d, want %d", GossipMsgType(buf), msgTypePurge)
	}
}

// --- helpers ---

func assertPurgeEqual(t *testing.T, got, want api.PurgeEvent) {
	t.Helper()
	if got.Key != want.Key {
		t.Errorf("Key = %d, want %d", got.Key, want.Key)
	}
	if got.VaryKey != want.VaryKey {
		t.Errorf("VaryKey = %q, want %q", got.VaryKey, want.VaryKey)
	}
	if got.Issuer != want.Issuer {
		t.Errorf("Issuer = %q, want %q", got.Issuer, want.Issuer)
	}
	if !got.IssuedAt.Equal(want.IssuedAt) {
		t.Errorf("IssuedAt = %v, want %v", got.IssuedAt, want.IssuedAt)
	}
	if got.Seq != want.Seq {
		t.Errorf("Seq = %d, want %d", got.Seq, want.Seq)
	}
}

func assertBanEqual(t *testing.T, got, want api.BanEvent) {
	t.Helper()
	if got.Predicate.HostRegex != want.Predicate.HostRegex {
		t.Errorf("HostRegex = %q, want %q", got.Predicate.HostRegex, want.Predicate.HostRegex)
	}
	if got.Predicate.PathRegex != want.Predicate.PathRegex {
		t.Errorf("PathRegex = %q, want %q", got.Predicate.PathRegex, want.Predicate.PathRegex)
	}
	if got.Predicate.SurrogateKey != want.Predicate.SurrogateKey {
		t.Errorf("SurrogateKey = %q, want %q", got.Predicate.SurrogateKey, want.Predicate.SurrogateKey)
	}
	if !got.Predicate.CreatedAt.Equal(want.Predicate.CreatedAt) {
		t.Errorf("CreatedAt = %v, want %v", got.Predicate.CreatedAt, want.Predicate.CreatedAt)
	}
	if got.Issuer != want.Issuer {
		t.Errorf("Issuer = %q, want %q", got.Issuer, want.Issuer)
	}
	if !got.IssuedAt.Equal(want.IssuedAt) {
		t.Errorf("IssuedAt = %v, want %v", got.IssuedAt, want.IssuedAt)
	}
	if got.Seq != want.Seq {
		t.Errorf("Seq = %d, want %d", got.Seq, want.Seq)
	}
}
