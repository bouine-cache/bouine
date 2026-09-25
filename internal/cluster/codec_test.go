package cluster

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

func TestEncodeDecodePurge_RoundTrip(t *testing.T) {
	t.Parallel()
	evt := api.PurgeEvent{
		Key:      testkey.Key(0xDEADBEEF),
		VaryKey:  "variant-1",
		Issuer:   "node-0",
		IssuedAt: time.Unix(0, 1234567890),
		Seq:      42,
	}
	// gossip
	buf, err := EncodePurgeGossip(evt)
	require.NoError(t, err, "encode gossip")
	got, err := DecodePurgeGossip(buf)
	require.NoError(t, err, "decode gossip")
	got.IssuedAt = got.IssuedAt.UTC()
	evt.IssuedAt = evt.IssuedAt.UTC()
	require.Equal(t, evt, got)
	// HTTP
	hbuf, err := EncodePurgeHTTP(evt)
	require.NoError(t, err, "encode http")
	got2, err := DecodePurgeHTTP(hbuf)
	require.NoError(t, err, "decode http")
	got2.IssuedAt = got2.IssuedAt.UTC()
	evt.IssuedAt = evt.IssuedAt.UTC()
	require.Equal(t, evt, got2)
}

func TestEncodeDecodePurge_EmptyStrings(t *testing.T) {
	t.Parallel()
	evt := api.PurgeEvent{
		Key:      testkey.Key(1),
		VaryKey:  "",
		Issuer:   "",
		IssuedAt: time.Unix(0, 1),
		Seq:      0,
	}
	buf, err := EncodePurgeGossip(evt)
	require.NoError(t, err, "encode")
	got, err := DecodePurgeGossip(buf)
	require.NoError(t, err, "decode")
	got.IssuedAt = got.IssuedAt.UTC()
	evt.IssuedAt = evt.IssuedAt.UTC()
	require.Equal(t, evt, got)
}

func TestDecodePurge_ShortFrame(t *testing.T) {
	t.Parallel()
	_, err := DecodePurgeGossip([]byte{binaryMagic, binaryVersion, msgTypePurge})
	require.Equal(t, errShortFrame, err)
}

func TestDecodePurge_WrongMsgType(t *testing.T) {
	t.Parallel()
	evt := api.BanEvent{Issuer: "x"}
	buf, _ := EncodeBanGossip(evt)
	_, err := DecodePurgeGossip(buf)
	require.Error(t, err)
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
	require.NoError(t, err, "encode gossip")
	got, err := DecodeBanGossip(buf)
	require.NoError(t, err, "decode gossip")
	got.Predicate.CreatedAt = got.Predicate.CreatedAt.UTC()
	got.IssuedAt = got.IssuedAt.UTC()
	evt.Predicate.CreatedAt = evt.Predicate.CreatedAt.UTC()
	evt.IssuedAt = evt.IssuedAt.UTC()
	require.Equal(t, evt, got)
	// HTTP
	hbuf, err := EncodeBanHTTP(evt)
	require.NoError(t, err, "encode http")
	got2, err := DecodeBanHTTP(hbuf)
	require.NoError(t, err, "decode http")
	got2.Predicate.CreatedAt = got2.Predicate.CreatedAt.UTC()
	got2.IssuedAt = got2.IssuedAt.UTC()
	evt.Predicate.CreatedAt = evt.Predicate.CreatedAt.UTC()
	evt.IssuedAt = evt.IssuedAt.UTC()
	require.Equal(t, evt, got2)
}

func TestEncodeDecodeBan_EmptyPredicate(t *testing.T) {
	t.Parallel()
	evt := api.BanEvent{
		Issuer:   "node-0",
		IssuedAt: time.Unix(0, 1),
		Seq:      1,
	}
	buf, err := EncodeBanGossip(evt)
	require.NoError(t, err, "encode")
	got, err := DecodeBanGossip(buf)
	require.NoError(t, err, "decode")
	got.Predicate.CreatedAt = got.Predicate.CreatedAt.UTC()
	got.IssuedAt = got.IssuedAt.UTC()
	evt.Predicate.CreatedAt = evt.Predicate.CreatedAt.UTC()
	evt.IssuedAt = evt.IssuedAt.UTC()
	require.Equal(t, evt, got)
}

func TestIsBinaryFrame(t *testing.T) {
	t.Parallel()
	require.False(t, IsBinaryFrame([]byte("{}")))
	require.False(t, IsBinaryFrame(nil))
	buf, _ := EncodePurgeGossip(api.PurgeEvent{Key: testkey.Key(1)})
	require.True(t, IsBinaryFrame(buf))
	require.Equal(t, msgTypePurge, GossipMsgType(buf))
}

func TestEncodeDecodeRefresh_RoundTrip(t *testing.T) {
	t.Parallel()
	evt := api.RefreshEvent{
		Key:      testkey.Key(0xCAFEBABE),
		Issuer:   "node-refresh",
		IssuedAt: time.Unix(0, 1234567890),
		Seq:      99,
	}
	// gossip
	buf, err := EncodeRefreshGossip(evt)
	require.NoError(t, err, "encode gossip")
	got, err := DecodeRefreshGossip(buf)
	require.NoError(t, err, "decode gossip")
	got.IssuedAt = got.IssuedAt.UTC()
	evt.IssuedAt = evt.IssuedAt.UTC()
	require.Equal(t, evt, got)
	// HTTP
	hbuf, err := EncodeRefreshHTTP(evt)
	require.NoError(t, err, "encode http")
	got2, err := DecodeRefreshHTTP(hbuf)
	require.NoError(t, err, "decode http")
	got2.IssuedAt = got2.IssuedAt.UTC()
	evt.IssuedAt = evt.IssuedAt.UTC()
	require.Equal(t, evt, got2)
}

func TestEncodeDecodeRefresh_EmptyStrings(t *testing.T) {
	t.Parallel()
	evt := api.RefreshEvent{
		Key:      testkey.Key(1),
		Issuer:   "",
		IssuedAt: time.Unix(0, 1),
		Seq:      0,
	}
	buf, err := EncodeRefreshGossip(evt)
	require.NoError(t, err, "encode")
	got, err := DecodeRefreshGossip(buf)
	require.NoError(t, err, "decode")
	got.IssuedAt = got.IssuedAt.UTC()
	evt.IssuedAt = evt.IssuedAt.UTC()
	require.Equal(t, evt, got)
}

func TestDecodeRefresh_ShortFrame(t *testing.T) {
	t.Parallel()
	_, err := DecodeRefreshGossip([]byte{binaryMagic, binaryVersion, msgTypeRefresh})
	require.Equal(t, errShortFrame, err)
}

func TestDecodeRefresh_WrongMsgType(t *testing.T) {
	t.Parallel()
	evt := api.PurgeEvent{Key: testkey.Key(1)}
	buf, _ := EncodePurgeGossip(evt)
	_, err := DecodeRefreshGossip(buf)
	require.Error(t, err)
}

func TestDecodeRefreshHTTP_BadMagic(t *testing.T) {
	t.Parallel()
	_, err := DecodeRefreshHTTP([]byte{0x00, binaryVersion})
	require.Equal(t, errBadMagic, err)
}

func TestDecodeRefreshHTTP_UnsupportedVersion(t *testing.T) {
	t.Parallel()
	_, err := DecodeRefreshHTTP([]byte{binaryMagic, 0xFF})
	require.Error(t, err)
}

func TestGossipMsgType_Refresh(t *testing.T) {
	t.Parallel()
	buf, _ := EncodeRefreshGossip(api.RefreshEvent{Key: testkey.Key(1)})
	require.True(t, IsBinaryFrame(buf))
	require.Equal(t, msgTypeRefresh, GossipMsgType(buf))
}

// TestMsgTypes_NonZero pins the encodeFrame/decodeFrame sentinel: 0
// means "HTTP frame, no msgType byte", so no gossip msgType may be 0 —
// a zero-valued type would silently emit HTTP-framed bytes on the
// gossip channel.
func TestMsgTypes_NonZero(t *testing.T) {
	t.Parallel()
	for _, mt := range []byte{msgTypePurge, msgTypeBan, msgTypeRefresh, msgTypePurgeBatch, msgTypeRefreshBatch} {
		require.NotZero(t, mt, "msgType 0 is reserved for HTTP frames")
	}
}

func TestPeerInfoMeta_RoundTrip(t *testing.T) {
	t.Parallel()
	info := api.PeerInfo{Name: "n1", Addr: "127.0.0.1:1", Weight: 2, JoinedAt: time.Now()}
	b, err := EncodePeerInfoMeta(info)
	require.NoError(t, err)
	require.Equal(t, binaryMagic, b[0])
	require.Equal(t, binaryVersion, b[1])

	got, err := DecodePeerInfoMeta(b)
	require.NoError(t, err)
	require.True(t, info.JoinedAt.Equal(got.JoinedAt))
	info.JoinedAt, got.JoinedAt = time.Time{}, time.Time{}
	require.Equal(t, info, got)
}

func TestPeerInfoMeta_RejectsBadMagic(t *testing.T) {
	t.Parallel()
	_, err := DecodePeerInfoMeta([]byte(`{"name":"n1"}`))
	require.ErrorIs(t, err, errBadMagic)
}

func TestPeerInfoMeta_RejectsUnknownVersion(t *testing.T) {
	t.Parallel()
	b, err := EncodePeerInfoMeta(api.PeerInfo{Name: "n1"})
	require.NoError(t, err)
	b[1] = binaryVersion + 1
	_, err = DecodePeerInfoMeta(b)
	require.ErrorIs(t, err, errUnsupportedVer)
}

func TestRingDigestState_RoundTrip(t *testing.T) {
	t.Parallel()
	digest := api.RingDigest{Hash: 0xDEADBEEFCAFEF00D, Size: 3, Version: 42}
	got, err := DecodeRingDigestState(EncodeRingDigestState(digest))
	require.NoError(t, err)
	require.Equal(t, digest, got)
}

func BenchmarkCodec_NodeMeta(b *testing.B) {
	info := api.PeerInfo{
		Name: "bench-node", Addr: "10.0.0.1:8080", AdminAddr: "10.0.0.1:8081",
		DataAddr: "10.0.0.1:8082", Version: "0.5.21", Weight: 1,
		JoinedAt: time.Now(),
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := EncodePeerInfoMeta(info); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCodec_LocalState(b *testing.B) {
	digest := api.RingDigest{Hash: 0xDEADBEEFCAFEF00D, Size: 3, Version: 42}
	b.ReportAllocs()
	for b.Loop() {
		EncodeRingDigestState(digest)
	}
}

func BenchmarkCodec_DecodeMeta(b *testing.B) {
	info := api.PeerInfo{
		Name: "bench-node", Addr: "10.0.0.1:8080", AdminAddr: "10.0.0.1:8081",
		DataAddr: "10.0.0.1:8082", Version: "0.5.21", Weight: 1,
	}
	buf, err := EncodePeerInfoMeta(info)
	require.NoError(b, err)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := DecodePeerInfoMeta(buf); err != nil {
			b.Fatal(err)
		}
	}
}

func TestPeerInfoMeta_RejectsOversizedString(t *testing.T) {
	t.Parallel()
	info := api.PeerInfo{Name: "n1", Version: strings.Repeat("v", maxStringLen+1)}
	_, err := EncodePeerInfoMeta(info)
	require.ErrorIs(t, err, errStringTooLong)
}
