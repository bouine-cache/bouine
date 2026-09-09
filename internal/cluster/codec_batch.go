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

// EncodePurgeBatchGossip serializes a batch of PurgeEvents as a single
// gossip frame: magic, version, msgTypePurgeBatch, uint32 count, then
// the per-event payloads back to back. See ADR-0044.
func EncodePurgeBatchGossip(evts []api.PurgeEvent) ([]byte, error) {
	total := gossipHdrLen + batchCountLen
	for _, evt := range evts {
		total += purgePayloadLen(evt)
	}
	buf := make([]byte, total)
	buf[0] = binaryMagic
	buf[1] = binaryVersion
	buf[2] = msgTypePurgeBatch
	binary.LittleEndian.PutUint32(buf[gossipHdrLen:], uint32(len(evts))) //nolint:gosec // count bounded by batchMaxEvents check below
	off := gossipHdrLen + batchCountLen
	for _, evt := range evts {
		var err error
		off, err = putPurgePayload(buf, off, evt)
		if err != nil {
			return nil, err
		}
	}
	return buf[:off], nil
}

// DecodePurgeBatchGossip decodes a batch gossip frame into its events.
func DecodePurgeBatchGossip(buf []byte) ([]api.PurgeEvent, error) {
	if len(buf) < gossipHdrLen+batchCountLen {
		return nil, errShortFrame
	}
	if buf[0] != binaryMagic {
		return nil, errBadMagic
	}
	if buf[1] != binaryVersion {
		return nil, fmt.Errorf("%w: got %d", errUnsupportedVer, buf[1])
	}
	if buf[2] != msgTypePurgeBatch {
		return nil, fmt.Errorf("cluster: wrong msgType %d for purge batch", buf[2])
	}
	count := int(binary.LittleEndian.Uint32(buf[gossipHdrLen:])) //nolint:gosec // width fixed by batchCountLen
	if count > batchMaxEvents {
		return nil, fmt.Errorf("cluster: purge batch count %d exceeds %d", count, batchMaxEvents)
	}
	evts := make([]api.PurgeEvent, 0, count)
	off := gossipHdrLen + batchCountLen
	for range count {
		evt, next, err := decodePurgePayloadBounded(buf, off)
		if err != nil {
			return nil, err
		}
		evts = append(evts, evt)
		off = next
	}
	if off != len(buf) {
		return nil, fmt.Errorf("cluster: purge batch has %d trailing bytes", len(buf)-off)
	}
	return evts, nil
}

// EncodePurgeBatchHTTP serializes a batch of PurgeEvents for the HTTP
// peer batch endpoint: magic, version, uint32 count, payloads.
func EncodePurgeBatchHTTP(evts []api.PurgeEvent) ([]byte, error) {
	total := binaryHdrLen + batchCountLen
	for _, evt := range evts {
		total += purgePayloadLen(evt)
	}
	buf := make([]byte, total)
	buf[0] = binaryMagic
	buf[1] = binaryVersion
	binary.LittleEndian.PutUint32(buf[binaryHdrLen:], uint32(len(evts))) //nolint:gosec // count bounded by batchMaxEvents check below
	off := binaryHdrLen + batchCountLen
	for _, evt := range evts {
		var err error
		off, err = putPurgePayload(buf, off, evt)
		if err != nil {
			return nil, err
		}
	}
	return buf[:off], nil
}

// DecodePurgeBatchHTTP decodes a batch HTTP body into its events.
func DecodePurgeBatchHTTP(buf []byte) ([]api.PurgeEvent, error) {
	if len(buf) < binaryHdrLen+batchCountLen {
		return nil, errShortFrame
	}
	if buf[0] != binaryMagic {
		return nil, errBadMagic
	}
	if buf[1] != binaryVersion {
		return nil, fmt.Errorf("%w: got %d", errUnsupportedVer, buf[1])
	}
	count := int(binary.LittleEndian.Uint32(buf[binaryHdrLen:])) //nolint:gosec // width fixed by batchCountLen
	if count > batchMaxEvents {
		return nil, fmt.Errorf("cluster: purge batch count %d exceeds %d", count, batchMaxEvents)
	}
	evts := make([]api.PurgeEvent, 0, count)
	off := binaryHdrLen + batchCountLen
	for range count {
		evt, next, err := decodePurgePayloadBounded(buf, off)
		if err != nil {
			return nil, err
		}
		evts = append(evts, evt)
		off = next
	}
	if off != len(buf) {
		return nil, fmt.Errorf("cluster: purge batch has %d trailing bytes", len(buf)-off)
	}
	return evts, nil
}

// EncodeRefreshBatchGossip serializes a batch of RefreshEvents as a
// single gossip frame.
func EncodeRefreshBatchGossip(evts []api.RefreshEvent) ([]byte, error) {
	total := gossipHdrLen + batchCountLen
	for _, evt := range evts {
		total += refreshPayloadLen(evt)
	}
	buf := make([]byte, total)
	buf[0] = binaryMagic
	buf[1] = binaryVersion
	buf[2] = msgTypeRefreshBatch
	binary.LittleEndian.PutUint32(buf[gossipHdrLen:], uint32(len(evts))) //nolint:gosec // count bounded by batchMaxEvents check below
	off := gossipHdrLen + batchCountLen
	for _, evt := range evts {
		var err error
		off, err = putRefreshPayload(buf, off, evt)
		if err != nil {
			return nil, err
		}
	}
	return buf[:off], nil
}

// DecodeRefreshBatchGossip decodes a batch refresh gossip frame.
func DecodeRefreshBatchGossip(buf []byte) ([]api.RefreshEvent, error) {
	if len(buf) < gossipHdrLen+batchCountLen {
		return nil, errShortFrame
	}
	if buf[0] != binaryMagic {
		return nil, errBadMagic
	}
	if buf[1] != binaryVersion {
		return nil, fmt.Errorf("%w: got %d", errUnsupportedVer, buf[1])
	}
	if buf[2] != msgTypeRefreshBatch {
		return nil, fmt.Errorf("cluster: wrong msgType %d for refresh batch", buf[2])
	}
	count := int(binary.LittleEndian.Uint32(buf[gossipHdrLen:])) //nolint:gosec // width fixed by batchCountLen
	if count > batchMaxEvents {
		return nil, fmt.Errorf("cluster: refresh batch count %d exceeds %d", count, batchMaxEvents)
	}
	evts := make([]api.RefreshEvent, 0, count)
	off := gossipHdrLen + batchCountLen
	for range count {
		evt, next, err := decodeRefreshPayloadBounded(buf, off)
		if err != nil {
			return nil, err
		}
		evts = append(evts, evt)
		off = next
	}
	if off != len(buf) {
		return nil, fmt.Errorf("cluster: refresh batch has %d trailing bytes", len(buf)-off)
	}
	return evts, nil
}

// EncodeRefreshBatchHTTP serializes a batch of RefreshEvents for the
// HTTP peer batch endpoint.
func EncodeRefreshBatchHTTP(evts []api.RefreshEvent) ([]byte, error) {
	total := binaryHdrLen + batchCountLen
	for _, evt := range evts {
		total += refreshPayloadLen(evt)
	}
	buf := make([]byte, total)
	buf[0] = binaryMagic
	buf[1] = binaryVersion
	binary.LittleEndian.PutUint32(buf[binaryHdrLen:], uint32(len(evts))) //nolint:gosec // count bounded by batchMaxEvents check below
	off := binaryHdrLen + batchCountLen
	for _, evt := range evts {
		var err error
		off, err = putRefreshPayload(buf, off, evt)
		if err != nil {
			return nil, err
		}
	}
	return buf[:off], nil
}

// DecodeRefreshBatchHTTP decodes a batch refresh HTTP body.
func DecodeRefreshBatchHTTP(buf []byte) ([]api.RefreshEvent, error) {
	if len(buf) < binaryHdrLen+batchCountLen {
		return nil, errShortFrame
	}
	if buf[0] != binaryMagic {
		return nil, errBadMagic
	}
	if buf[1] != binaryVersion {
		return nil, fmt.Errorf("%w: got %d", errUnsupportedVer, buf[1])
	}
	count := int(binary.LittleEndian.Uint32(buf[binaryHdrLen:])) //nolint:gosec // width fixed by batchCountLen
	if count > batchMaxEvents {
		return nil, fmt.Errorf("cluster: refresh batch count %d exceeds %d", count, batchMaxEvents)
	}
	evts := make([]api.RefreshEvent, 0, count)
	off := binaryHdrLen + batchCountLen
	for range count {
		evt, next, err := decodeRefreshPayloadBounded(buf, off)
		if err != nil {
			return nil, err
		}
		evts = append(evts, evt)
		off = next
	}
	if off != len(buf) {
		return nil, fmt.Errorf("cluster: refresh batch has %d trailing bytes", len(buf)-off)
	}
	return evts, nil
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
