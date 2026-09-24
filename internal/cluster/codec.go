package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
)

const (
	binaryMagic   byte = 0x42
	binaryVersion byte = 3
	binaryHdrLen       = 2 // magic + version
	gossipHdrLen       = 3 // magic + version + msgType
	maxStringLen       = 65535
)

const (
	msgTypePurge        byte = 1
	msgTypeBan          byte = 2
	msgTypeRefresh      byte = 3
	msgTypePurgeBatch   byte = 4
	msgTypeRefreshBatch byte = 5
)

var (
	errShortFrame     = errors.New("cluster: short binary frame")
	errBadMagic       = errors.New("cluster: bad magic byte")
	errUnsupportedVer = errors.New("cluster: unsupported binary version")
	errStringTooLong  = errors.New("cluster: string length exceeds 64 KiB")
)

func encodeTime(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func decodeTime(nano int64) time.Time {
	if nano == 0 {
		return time.Time{}
	}
	return time.Unix(0, nano)
}

func putString(buf []byte, offset int, s string) (int, error) {
	if len(s) > maxStringLen {
		return offset, errStringTooLong
	}
	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(s))) //nolint:gosec // bounded by maxStringLen check above
	offset += 2
	copy(buf[offset:], s)
	return offset + len(s), nil
}

func readString(buf []byte, offset int) (string, int, error) {
	if offset+2 > len(buf) {
		return "", offset, errShortFrame
	}
	n := int(binary.LittleEndian.Uint16(buf[offset:]))
	offset += 2
	if offset+n > len(buf) {
		return "", offset, errShortFrame
	}
	return string(buf[offset : offset+n]), offset + n, nil
}

// ---- generic framing ----
//
// Every invalidation frame is: magic, version, [msgType], payload.
// Gossip frames carry the msgType byte (the gossip channel delivers
// multiple event kinds); HTTP peer frames omit it (one endpoint per
// kind). encodeFrame/decodeFrame implement the shared guard once.

// encodeFrame writes magic+version (+msgType when msgType > 0) and the
// payload produced by put into a buffer sized total.
func encodeFrame(total int, msgType byte, put func(buf []byte, off int) (int, error)) ([]byte, error) {
	buf := make([]byte, total)
	buf[0] = binaryMagic
	buf[1] = binaryVersion
	off := binaryHdrLen
	if msgType != 0 {
		buf[2] = msgType
		off = gossipHdrLen
	}
	off, err := put(buf, off)
	if err != nil {
		return nil, err
	}
	return buf[:off], nil
}

// decodeFrame validates magic, version, and — when wantType > 0 — the
// msgType byte, then decodes the payload at the frame-header offset.
func decodeFrame(buf []byte, wantType byte, decode func(buf []byte, off int) error) error {
	hdrLen := binaryHdrLen
	if wantType != 0 {
		hdrLen = gossipHdrLen
	}
	if len(buf) < hdrLen {
		return errShortFrame
	}
	if buf[0] != binaryMagic {
		return errBadMagic
	}
	if buf[1] != binaryVersion {
		return fmt.Errorf("%w: got %d", errUnsupportedVer, buf[1])
	}
	if wantType != 0 && buf[2] != wantType {
		return fmt.Errorf("cluster: wrong msgType %d (want %d)", buf[2], wantType)
	}
	return decode(buf, hdrLen)
}

// frameLen computes the total frame size for an HTTP (no msgType byte)
// or gossip frame around a payload of payloadLen bytes.
func frameLen(gossip bool, payloadLen int) int {
	if gossip {
		return gossipHdrLen + payloadLen
	}
	return binaryHdrLen + payloadLen
}

func purgePayloadLen(evt api.PurgeEvent) int {
	return 16 + 2 + len(evt.VaryKey) + 2 + len(evt.Issuer) + 8 + 8
}

func putPurgePayload(buf []byte, off int, evt api.PurgeEvent) (int, error) {
	copy(buf[off:off+16], evt.Key[:])
	off += 16
	var err error
	off, err = putString(buf, off, evt.VaryKey)
	if err != nil {
		return off, err
	}
	off, err = putString(buf, off, evt.Issuer)
	if err != nil {
		return off, err
	}
	binary.LittleEndian.PutUint64(buf[off:], uint64(encodeTime(evt.IssuedAt))) //nolint:gosec // wire format: int64 cast for LE encoding
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], evt.Seq)
	return off + 8, nil
}

func decodePurgePayload(buf []byte, off int) (api.PurgeEvent, error) {
	var evt api.PurgeEvent
	if off+16 > len(buf) {
		return evt, errShortFrame
	}
	copy(evt.Key[:], buf[off:off+16])
	off += 16
	var err error
	evt.VaryKey, off, err = readString(buf, off)
	if err != nil {
		return evt, err
	}
	evt.Issuer, off, err = readString(buf, off)
	if err != nil {
		return evt, err
	}
	if off+16 > len(buf) {
		return evt, errShortFrame
	}
	evt.IssuedAt = decodeTime(int64(binary.LittleEndian.Uint64(buf[off:]))) //nolint:gosec // wire format: uint64→int64 round-trip
	off += 8
	evt.Seq = binary.LittleEndian.Uint64(buf[off:])
	return evt, nil
}

// EncodePurgeGossip serializes a PurgeEvent as a gossip frame.
func EncodePurgeGossip(evt api.PurgeEvent) ([]byte, error) {
	return encodeFrame(frameLen(true, purgePayloadLen(evt)), msgTypePurge,
		func(buf []byte, off int) (int, error) { return putPurgePayload(buf, off, evt) })
}

// EncodePurgeHTTP serializes a PurgeEvent for the HTTP peer-purge endpoint.
func EncodePurgeHTTP(evt api.PurgeEvent) ([]byte, error) {
	return encodeFrame(frameLen(false, purgePayloadLen(evt)), 0,
		func(buf []byte, off int) (int, error) { return putPurgePayload(buf, off, evt) })
}

// DecodePurgeGossip decodes a PurgeEvent from a gossip frame.
func DecodePurgeGossip(buf []byte) (api.PurgeEvent, error) {
	var evt api.PurgeEvent
	err := decodeFrame(buf, msgTypePurge, func(buf []byte, off int) error {
		var err error
		evt, err = decodePurgePayload(buf, off)
		return err
	})
	return evt, err
}

// DecodePurgeHTTP decodes a PurgeEvent from an HTTP peer-purge body.
func DecodePurgeHTTP(buf []byte) (api.PurgeEvent, error) {
	var evt api.PurgeEvent
	err := decodeFrame(buf, 0, func(buf []byte, off int) error {
		var err error
		evt, err = decodePurgePayload(buf, off)
		return err
	})
	return evt, err
}

func banPayloadLen(evt api.BanEvent) int {
	return 2 + len(evt.Predicate.HostRegex) +
		2 + len(evt.Predicate.PathRegex) +
		2 + len(evt.Predicate.SurrogateKey) +
		8 + // CreatedAt
		2 + len(evt.Issuer) +
		8 + // IssuedAt
		8 // Seq
}

func putBanPayload(buf []byte, off int, evt api.BanEvent) (int, error) {
	var err error
	off, err = putString(buf, off, evt.Predicate.HostRegex)
	if err != nil {
		return off, err
	}
	off, err = putString(buf, off, evt.Predicate.PathRegex)
	if err != nil {
		return off, err
	}
	off, err = putString(buf, off, evt.Predicate.SurrogateKey)
	if err != nil {
		return off, err
	}
	binary.LittleEndian.PutUint64(buf[off:], uint64(encodeTime(evt.Predicate.CreatedAt))) //nolint:gosec // wire format: int64 cast
	off += 8
	off, err = putString(buf, off, evt.Issuer)
	if err != nil {
		return off, err
	}
	binary.LittleEndian.PutUint64(buf[off:], uint64(encodeTime(evt.IssuedAt))) //nolint:gosec // wire format: int64 cast
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], evt.Seq)
	return off + 8, nil
}

func decodeBanPayload(buf []byte, off int) (api.BanEvent, error) {
	var evt api.BanEvent
	var err error
	evt.Predicate.HostRegex, off, err = readString(buf, off)
	if err != nil {
		return evt, err
	}
	evt.Predicate.PathRegex, off, err = readString(buf, off)
	if err != nil {
		return evt, err
	}
	evt.Predicate.SurrogateKey, off, err = readString(buf, off)
	if err != nil {
		return evt, err
	}
	if off+8 > len(buf) {
		return evt, errShortFrame
	}
	evt.Predicate.CreatedAt = decodeTime(int64(binary.LittleEndian.Uint64(buf[off:]))) //nolint:gosec // wire format: uint64→int64 round-trip
	off += 8
	evt.Issuer, off, err = readString(buf, off)
	if err != nil {
		return evt, err
	}
	if off+16 > len(buf) {
		return evt, errShortFrame
	}
	evt.IssuedAt = decodeTime(int64(binary.LittleEndian.Uint64(buf[off:]))) //nolint:gosec // wire format: uint64→int64 round-trip
	off += 8
	evt.Seq = binary.LittleEndian.Uint64(buf[off:])
	return evt, nil
}

// EncodeBanGossip serializes a BanEvent as a gossip frame.
func EncodeBanGossip(evt api.BanEvent) ([]byte, error) {
	return encodeFrame(frameLen(true, banPayloadLen(evt)), msgTypeBan,
		func(buf []byte, off int) (int, error) { return putBanPayload(buf, off, evt) })
}

// EncodeBanHTTP serializes a BanEvent for the HTTP peer-ban endpoint.
func EncodeBanHTTP(evt api.BanEvent) ([]byte, error) {
	return encodeFrame(frameLen(false, banPayloadLen(evt)), 0,
		func(buf []byte, off int) (int, error) { return putBanPayload(buf, off, evt) })
}

// DecodeBanGossip decodes a BanEvent from a gossip frame.
func DecodeBanGossip(buf []byte) (api.BanEvent, error) {
	var evt api.BanEvent
	err := decodeFrame(buf, msgTypeBan, func(buf []byte, off int) error {
		var err error
		evt, err = decodeBanPayload(buf, off)
		return err
	})
	return evt, err
}

// DecodeBanHTTP decodes a BanEvent from an HTTP peer-ban body.
func DecodeBanHTTP(buf []byte) (api.BanEvent, error) {
	var evt api.BanEvent
	err := decodeFrame(buf, 0, func(buf []byte, off int) error {
		var err error
		evt, err = decodeBanPayload(buf, off)
		return err
	})
	return evt, err
}

func refreshPayloadLen(evt api.RefreshEvent) int {
	return 16 + 2 + len(evt.Issuer) + 8 + 8
}

func putRefreshPayload(buf []byte, off int, evt api.RefreshEvent) (int, error) {
	copy(buf[off:off+16], evt.Key[:])
	off += 16
	var err error
	off, err = putString(buf, off, evt.Issuer)
	if err != nil {
		return off, err
	}
	binary.LittleEndian.PutUint64(buf[off:], uint64(encodeTime(evt.IssuedAt))) //nolint:gosec // wire format: int64 cast
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], evt.Seq)
	return off + 8, nil
}

func decodeRefreshPayload(buf []byte, off int) (api.RefreshEvent, error) {
	var evt api.RefreshEvent
	if off+16 > len(buf) {
		return evt, errShortFrame
	}
	copy(evt.Key[:], buf[off:off+16])
	off += 16
	var err error
	evt.Issuer, off, err = readString(buf, off)
	if err != nil {
		return evt, err
	}
	if off+16 > len(buf) {
		return evt, errShortFrame
	}
	evt.IssuedAt = decodeTime(int64(binary.LittleEndian.Uint64(buf[off:]))) //nolint:gosec // wire format: uint64→int64 round-trip
	off += 8
	evt.Seq = binary.LittleEndian.Uint64(buf[off:])
	return evt, nil
}

// EncodeRefreshGossip serializes a RefreshEvent as a gossip frame.
func EncodeRefreshGossip(evt api.RefreshEvent) ([]byte, error) {
	return encodeFrame(frameLen(true, refreshPayloadLen(evt)), msgTypeRefresh,
		func(buf []byte, off int) (int, error) { return putRefreshPayload(buf, off, evt) })
}

// EncodeRefreshHTTP serializes a RefreshEvent for the HTTP peer-refresh endpoint.
func EncodeRefreshHTTP(evt api.RefreshEvent) ([]byte, error) {
	return encodeFrame(frameLen(false, refreshPayloadLen(evt)), 0,
		func(buf []byte, off int) (int, error) { return putRefreshPayload(buf, off, evt) })
}

// DecodeRefreshGossip decodes a RefreshEvent from a gossip frame.
func DecodeRefreshGossip(buf []byte) (api.RefreshEvent, error) {
	var evt api.RefreshEvent
	err := decodeFrame(buf, msgTypeRefresh, func(buf []byte, off int) error {
		var err error
		evt, err = decodeRefreshPayload(buf, off)
		return err
	})
	return evt, err
}

// DecodeRefreshHTTP decodes a RefreshEvent from an HTTP peer-refresh body.
func DecodeRefreshHTTP(buf []byte) (api.RefreshEvent, error) {
	var evt api.RefreshEvent
	err := decodeFrame(buf, 0, func(buf []byte, off int) error {
		var err error
		evt, err = decodeRefreshPayload(buf, off)
		return err
	})
	return evt, err
}

// IsBinaryFrame reports whether msg starts with the binary magic byte.
func IsBinaryFrame(msg []byte) bool {
	return len(msg) > 0 && msg[0] == binaryMagic
}

// GossipMsgType returns the msgType byte from a binary gossip frame.
func GossipMsgType(msg []byte) byte {
	if !IsBinaryFrame(msg) || len(msg) < gossipHdrLen {
		return 0
	}
	return msg[2]
}

// ---- meta / push-pull state frames (binary, no msgType byte) ----
//
// Meta carries PeerInfo (memberlist node metadata), state carries
// RingDigest (push/pull sync). Both are framed with binaryMagic +
// binaryVersion; the channel implies the payload type, so no msgType
// byte is spent.

// peerInfoStrings returns the frame's string fields in wire order.
func peerInfoStrings(info api.PeerInfo) []string {
	return []string{info.Name, info.Addr, info.AdminAddr, info.DataAddr, info.Version}
}

func peerInfoPayloadLen(info api.PeerInfo) int {
	return 8 + // JoinedAt (unix nanos, 0 = zero)
		5*2 + // five length-prefixed strings
		len(info.Name) + len(info.Addr) + len(info.AdminAddr) +
		len(info.DataAddr) + len(info.Version) +
		8 // Weight (float64)
}

func putPeerInfoPayload(buf []byte, off int, info api.PeerInfo) int {
	binary.LittleEndian.PutUint64(buf[off:], uint64(encodeTime(info.JoinedAt))) //nolint:gosec // wire format: uint64→int64 round-trip
	off += 8
	for _, s := range peerInfoStrings(info) {
		// String lengths are validated by EncodePeerInfoMeta; the
		// buffer is sized to peerInfoPayloadLen, so putString cannot
		// fail here.
		off, _ = putString(buf, off, s)
	}
	binary.LittleEndian.PutUint64(buf[off:], math.Float64bits(info.Weight))
	return off + 8
}

func readPeerInfoPayload(buf []byte, off int) (api.PeerInfo, int, error) {
	var info api.PeerInfo
	if off+8 > len(buf) {
		return info, off, errShortFrame
	}
	info.JoinedAt = decodeTime(int64(binary.LittleEndian.Uint64(buf[off:]))) //nolint:gosec // wire format: uint64→int64 round-trip
	off += 8
	var err error
	for _, dst := range []*string{&info.Name, &info.Addr, &info.AdminAddr, &info.DataAddr, &info.Version} {
		*dst, off, err = readString(buf, off)
		if err != nil {
			return info, off, err
		}
	}
	if off+8 > len(buf) {
		return info, off, errShortFrame
	}
	info.Weight = math.Float64frombits(binary.LittleEndian.Uint64(buf[off:]))
	return info, off + 8, nil
}

// EncodePeerInfoMeta serializes PeerInfo as the memberlist node meta
// frame (binaryMagic + version + payload).
func EncodePeerInfoMeta(info api.PeerInfo) ([]byte, error) {
	for _, s := range peerInfoStrings(info) {
		if len(s) > maxStringLen {
			return nil, errStringTooLong
		}
	}
	buf := make([]byte, binaryHdrLen+peerInfoPayloadLen(info))
	buf[0] = binaryMagic
	buf[1] = binaryVersion
	putPeerInfoPayload(buf, binaryHdrLen, info)
	return buf, nil
}

// DecodePeerInfoMeta decodes a PeerInfo from a memberlist meta frame.
func DecodePeerInfoMeta(buf []byte) (api.PeerInfo, error) {
	if len(buf) < binaryHdrLen {
		return api.PeerInfo{}, errShortFrame
	}
	if buf[0] != binaryMagic {
		return api.PeerInfo{}, errBadMagic
	}
	if buf[1] != binaryVersion {
		return api.PeerInfo{}, fmt.Errorf("%w: got %d", errUnsupportedVer, buf[1])
	}
	info, _, err := readPeerInfoPayload(buf, binaryHdrLen)
	return info, err
}

func encodeRingDigestPayload(buf []byte, off int, d api.RingDigest) int {
	binary.LittleEndian.PutUint64(buf[off:], d.Hash)
	binary.LittleEndian.PutUint64(buf[off+8:], uint64(d.Size)) //nolint:gosec // wire format: int size for LE encoding
	binary.LittleEndian.PutUint64(buf[off+16:], d.Version)
	return off + 24
}

// EncodeRingDigestState serializes the ring digest as the memberlist
// push/pull state frame (binaryMagic + version + payload).
func EncodeRingDigestState(d api.RingDigest) []byte {
	buf := make([]byte, binaryHdrLen+24)
	buf[0] = binaryMagic
	buf[1] = binaryVersion
	encodeRingDigestPayload(buf, binaryHdrLen, d)
	return buf
}

// DecodeRingDigestState decodes the ring digest from a push/pull state
// frame.
func DecodeRingDigestState(buf []byte) (api.RingDigest, error) {
	if len(buf) < binaryHdrLen+24 {
		return api.RingDigest{}, errShortFrame
	}
	if buf[0] != binaryMagic {
		return api.RingDigest{}, errBadMagic
	}
	if buf[1] != binaryVersion {
		return api.RingDigest{}, fmt.Errorf("%w: got %d", errUnsupportedVer, buf[1])
	}
	off := binaryHdrLen
	return api.RingDigest{
		Hash:    binary.LittleEndian.Uint64(buf[off:]),
		Size:    int(binary.LittleEndian.Uint64(buf[off+8:])), //nolint:gosec // wire format: int size for LE encoding
		Version: binary.LittleEndian.Uint64(buf[off+16:]),
	}, nil
}
