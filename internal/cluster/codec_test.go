package cluster

import (
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
