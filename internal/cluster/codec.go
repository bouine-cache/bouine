package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
)

const (
	binaryMagic   byte = 0x42
	binaryVersion byte = 2
	binaryHdrLen       = 2 // magic + version
	gossipHdrLen       = 3 // magic + version + msgType
	maxStringLen       = 65535
)

const (
	msgTypePurge byte = 1
	msgTypeBan   byte = 2
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

// EncodeKeySet serializes a KeySet into a single pre-sized byte slice.
func EncodeKeySet(nodeName string, keys []api.Key) ([]byte, error) {
	if len(nodeName) > maxStringLen {
		return nil, errStringTooLong
	}
	hdrLen := binaryHdrLen + 2 + len(nodeName) + 4
	total := hdrLen + len(keys)*8
	buf := make([]byte, total)
	buf[0] = binaryMagic
	buf[1] = binaryVersion
	off := 2
	binary.LittleEndian.PutUint16(buf[off:], uint16(len(nodeName))) //nolint:gosec // bounded by maxStringLen check above
	off += 2
	copy(buf[off:], nodeName)
	off += len(nodeName)
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(keys))) //nolint:gosec // key count bounded by available memory
	off += 4
	for _, k := range keys {
		binary.LittleEndian.PutUint64(buf[off:], uint64(k))
		off += 8
	}
	return buf, nil
}

// DecodeKeySet parses a binary KeySet from buf.
func DecodeKeySet(buf []byte) (nodeName string, keys []api.Key, err error) {
	if len(buf) < binaryHdrLen {
		return "", nil, errShortFrame
	}
	if buf[0] != binaryMagic {
		return "", nil, errBadMagic
	}
	if buf[1] != binaryVersion {
		return "", nil, fmt.Errorf("%w: got %d", errUnsupportedVer, buf[1])
	}
	off := binaryHdrLen
	nodeName, off, err = readString(buf, off)
	if err != nil {
		return "", nil, err
	}
	if off+4 > len(buf) {
		return "", nil, errShortFrame
	}
	count := int(binary.LittleEndian.Uint32(buf[off:]))
	off += 4
	need := count * 8
	if off+need > len(buf) {
		return "", nil, errShortFrame
	}
	keys = make([]api.Key, count)
	for i := range count {
		keys[i] = api.Key(binary.LittleEndian.Uint64(buf[off:]))
		off += 8
	}
	return nodeName, keys, nil
}

func purgePayloadLen(evt api.PurgeEvent) int {
	return 8 + 2 + len(evt.VaryKey) + 2 + len(evt.Issuer) + 8 + 8
}

func putPurgePayload(buf []byte, off int, evt api.PurgeEvent) (int, error) {
	binary.LittleEndian.PutUint64(buf[off:], uint64(evt.Key))
	off += 8
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
	if off+8 > len(buf) {
		return evt, errShortFrame
	}
	evt.Key = api.Key(binary.LittleEndian.Uint64(buf[off:]))
	off += 8
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
	total := gossipHdrLen + purgePayloadLen(evt)
	buf := make([]byte, total)
	buf[0] = binaryMagic
	buf[1] = binaryVersion
	buf[2] = msgTypePurge
	off, err := putPurgePayload(buf, gossipHdrLen, evt)
	if err != nil {
		return nil, err
	}
	return buf[:off], nil
}

// EncodePurgeHTTP serializes a PurgeEvent for the HTTP peer-purge endpoint.
func EncodePurgeHTTP(evt api.PurgeEvent) ([]byte, error) {
	total := binaryHdrLen + purgePayloadLen(evt)
	buf := make([]byte, total)
	buf[0] = binaryMagic
	buf[1] = binaryVersion
	off, err := putPurgePayload(buf, binaryHdrLen, evt)
	if err != nil {
		return nil, err
	}
	return buf[:off], nil
}

// DecodePurgeGossip decodes a PurgeEvent from a gossip frame.
func DecodePurgeGossip(buf []byte) (api.PurgeEvent, error) {
	if len(buf) < gossipHdrLen {
		return api.PurgeEvent{}, errShortFrame
	}
	if buf[0] != binaryMagic {
		return api.PurgeEvent{}, errBadMagic
	}
	if buf[1] != binaryVersion {
		return api.PurgeEvent{}, fmt.Errorf("%w: got %d", errUnsupportedVer, buf[1])
	}
	if buf[2] != msgTypePurge {
		return api.PurgeEvent{}, fmt.Errorf("cluster: wrong msgType %d for purge", buf[2])
	}
	return decodePurgePayload(buf, gossipHdrLen)
}

// DecodePurgeHTTP decodes a PurgeEvent from an HTTP peer-purge body.
func DecodePurgeHTTP(buf []byte) (api.PurgeEvent, error) {
	if len(buf) < binaryHdrLen {
		return api.PurgeEvent{}, errShortFrame
	}
	if buf[0] != binaryMagic {
		return api.PurgeEvent{}, errBadMagic
	}
	if buf[1] != binaryVersion {
		return api.PurgeEvent{}, fmt.Errorf("%w: got %d", errUnsupportedVer, buf[1])
	}
	return decodePurgePayload(buf, binaryHdrLen)
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
	total := gossipHdrLen + banPayloadLen(evt)
	buf := make([]byte, total)
	buf[0] = binaryMagic
	buf[1] = binaryVersion
	buf[2] = msgTypeBan
	off, err := putBanPayload(buf, gossipHdrLen, evt)
	if err != nil {
		return nil, err
	}
	return buf[:off], nil
}

// EncodeBanHTTP serializes a BanEvent for the HTTP peer-ban endpoint.
func EncodeBanHTTP(evt api.BanEvent) ([]byte, error) {
	total := binaryHdrLen + banPayloadLen(evt)
	buf := make([]byte, total)
	buf[0] = binaryMagic
	buf[1] = binaryVersion
	off, err := putBanPayload(buf, binaryHdrLen, evt)
	if err != nil {
		return nil, err
	}
	return buf[:off], nil
}

// DecodeBanGossip decodes a BanEvent from a gossip frame.
func DecodeBanGossip(buf []byte) (api.BanEvent, error) {
	if len(buf) < gossipHdrLen {
		return api.BanEvent{}, errShortFrame
	}
	if buf[0] != binaryMagic {
		return api.BanEvent{}, errBadMagic
	}
	if buf[1] != binaryVersion {
		return api.BanEvent{}, fmt.Errorf("%w: got %d", errUnsupportedVer, buf[1])
	}
	if buf[2] != msgTypeBan {
		return api.BanEvent{}, fmt.Errorf("cluster: wrong msgType %d for ban", buf[2])
	}
	return decodeBanPayload(buf, gossipHdrLen)
}

// DecodeBanHTTP decodes a BanEvent from an HTTP peer-ban body.
func DecodeBanHTTP(buf []byte) (api.BanEvent, error) {
	if len(buf) < binaryHdrLen {
		return api.BanEvent{}, errShortFrame
	}
	if buf[0] != binaryMagic {
		return api.BanEvent{}, errBadMagic
	}
	if buf[1] != binaryVersion {
		return api.BanEvent{}, fmt.Errorf("%w: got %d", errUnsupportedVer, buf[1])
	}
	return decodeBanPayload(buf, binaryHdrLen)
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
