package cluster

import (
	"encoding/binary"
	"fmt"

	"github.com/bouine-cache/bouine/pkg/api"
)

// batchMaxEvents bounds the number of events in one batch frame. The
// broadcaster flushes at broadcastBatchSize; the cap guards decode
// paths against hostile or corrupted frames.
const batchMaxEvents = 4096

// batchCountLen is the width of the batch event count field.
const batchCountLen = 4

// batchCodec holds the per-event table entries for one batch kind.
// All four batch encoders/decoders (purge/refresh × gossip/HTTP) share
// this skeleton via encodeFrame/decodeFrame; only the payload put/
// decode/bounded-decode functions differ.
type batchCodec[E any] struct {
	payloadLen func(E) int
	put        func(buf []byte, off int, evt E) (int, error)
	decodeOne  func(buf []byte, off int) (E, int, error) // bounded: returns offset past the event
	kind       string                                    // human-readable name for decode-error messages
	msgType    byte                                      // 0 for HTTP frames (no msgType byte)
	gossip     bool                                      // true for gossip frames (msgType byte present)
}

// encode serializes evts into a single batch frame: magic, version,
// [msgType], uint32 count, then per-event payloads back to back.
func (c batchCodec[E]) encode(evts []E) ([]byte, error) {
	total := frameLen(c.gossip, batchCountLen)
	for i := range evts {
		total += c.payloadLen(evts[i])
	}
	return encodeFrame(total, c.msgType, func(buf []byte, o int) (int, error) {
		binary.LittleEndian.PutUint32(buf[o:], uint32(len(evts))) //nolint:gosec // count bounded by batchMaxEvents check in decode
		o += batchCountLen
		var err error
		for i := range evts {
			o, err = c.put(buf, o, evts[i])
			if err != nil {
				return o, err
			}
		}
		return o, nil
	})
}

// decode parses a batch frame into its events, enforcing the count cap
// and rejecting trailing bytes.
func (c batchCodec[E]) decode(buf []byte) ([]E, error) {
	var evts []E
	err := decodeFrame(buf, c.msgType, func(buf []byte, off int) error {
		if off+batchCountLen > len(buf) {
			return errShortFrame
		}
		count := int(binary.LittleEndian.Uint32(buf[off:])) //nolint:gosec // width fixed by batchCountLen
		if count > batchMaxEvents {
			return fmt.Errorf("cluster: %s count %d exceeds %d", c.kind, count, batchMaxEvents)
		}
		evts = make([]E, 0, count)
		off += batchCountLen
		for range count {
			evt, next, err := c.decodeOne(buf, off)
			if err != nil {
				return err
			}
			evts = append(evts, evt)
			off = next
		}
		if off != len(buf) {
			return fmt.Errorf("cluster: %s has %d trailing bytes", c.kind, len(buf)-off)
		}
		return nil
	})
	return evts, err
}

// purgeBatchGossip / purgeBatchHTTP are the table entries for purge
// batches; refresh follows the same pattern.
var (
	purgeBatchGossip = batchCodec[api.PurgeEvent]{
		msgType: msgTypePurgeBatch, gossip: true, kind: "purge batch",
		payloadLen: purgePayloadLen,
		put:        putPurgePayload,
		decodeOne:  decodePurgePayloadBounded,
	}
	purgeBatchHTTP = batchCodec[api.PurgeEvent]{
		msgType: 0, gossip: false, kind: "purge batch",
		payloadLen: purgePayloadLen,
		put:        putPurgePayload,
		decodeOne:  decodePurgePayloadBounded,
	}
	refreshBatchGossip = batchCodec[api.RefreshEvent]{
		msgType: msgTypeRefreshBatch, gossip: true, kind: "refresh batch",
		payloadLen: refreshPayloadLen,
		put:        putRefreshPayload,
		decodeOne:  decodeRefreshPayloadBounded,
	}
	refreshBatchHTTP = batchCodec[api.RefreshEvent]{
		msgType: 0, gossip: false, kind: "refresh batch",
		payloadLen: refreshPayloadLen,
		put:        putRefreshPayload,
		decodeOne:  decodeRefreshPayloadBounded,
	}
)

// EncodePurgeBatchGossip serializes a batch of PurgeEvents as a single
// gossip frame. See ADR-0044.
func EncodePurgeBatchGossip(evts []api.PurgeEvent) ([]byte, error) {
	return purgeBatchGossip.encode(evts)
}

// DecodePurgeBatchGossip decodes a batch gossip frame into its events.
func DecodePurgeBatchGossip(buf []byte) ([]api.PurgeEvent, error) {
	return purgeBatchGossip.decode(buf)
}

// EncodePurgeBatchHTTP serializes a batch of PurgeEvents for the HTTP
// peer batch endpoint: magic, version, uint32 count, payloads.
func EncodePurgeBatchHTTP(evts []api.PurgeEvent) ([]byte, error) {
	return purgeBatchHTTP.encode(evts)
}

// DecodePurgeBatchHTTP decodes a batch HTTP body into its events.
func DecodePurgeBatchHTTP(buf []byte) ([]api.PurgeEvent, error) {
	return purgeBatchHTTP.decode(buf)
}

// EncodeRefreshBatchGossip serializes a batch of RefreshEvents as a
// single gossip frame.
func EncodeRefreshBatchGossip(evts []api.RefreshEvent) ([]byte, error) {
	return refreshBatchGossip.encode(evts)
}

// DecodeRefreshBatchGossip decodes a batch refresh gossip frame.
func DecodeRefreshBatchGossip(buf []byte) ([]api.RefreshEvent, error) {
	return refreshBatchGossip.decode(buf)
}

// EncodeRefreshBatchHTTP serializes a batch of RefreshEvents for the
// HTTP peer batch endpoint.
func EncodeRefreshBatchHTTP(evts []api.RefreshEvent) ([]byte, error) {
	return refreshBatchHTTP.encode(evts)
}

// DecodeRefreshBatchHTTP decodes a batch refresh HTTP body.
func DecodeRefreshBatchHTTP(buf []byte) ([]api.RefreshEvent, error) {
	return refreshBatchHTTP.decode(buf)
}

// decodePurgePayloadBounded decodes one purge payload at off and
// returns the event plus the offset just past it.
func decodePurgePayloadBounded(buf []byte, off int) (api.PurgeEvent, int, error) {
	evt, err := decodePurgePayload(buf, off)
	if err != nil {
		return api.PurgeEvent{}, off, err
	}
	// decodePurgePayload consumes at least the 16-byte key, two length
	// fields, and the 16-byte tail — recompute the consumed width.
	consumed := 16
	if off+consumed > len(buf) {
		return api.PurgeEvent{}, off, errShortFrame
	}
	vkLen := int(binary.LittleEndian.Uint16(buf[off+16:]))
	consumed += 2 + vkLen
	if off+consumed+2 > len(buf) {
		return api.PurgeEvent{}, off, errShortFrame
	}
	issuerLen := int(binary.LittleEndian.Uint16(buf[off+consumed:]))
	consumed += 2 + issuerLen + 16
	if off+consumed > len(buf) {
		return api.PurgeEvent{}, off, errShortFrame
	}
	return evt, off + consumed, nil
}

// decodeRefreshPayloadBounded decodes one refresh payload at off and
// returns the event plus the offset just past it.
func decodeRefreshPayloadBounded(buf []byte, off int) (api.RefreshEvent, int, error) {
	evt, err := decodeRefreshPayload(buf, off)
	if err != nil {
		return api.RefreshEvent{}, off, err
	}
	if off+16+2 > len(buf) {
		return api.RefreshEvent{}, off, errShortFrame
	}
	issuerLen := int(binary.LittleEndian.Uint16(buf[off+16:]))
	consumed := 16 + 2 + issuerLen + 16
	if off+consumed > len(buf) {
		return api.RefreshEvent{}, off, errShortFrame
	}
	return evt, off + consumed, nil
}
