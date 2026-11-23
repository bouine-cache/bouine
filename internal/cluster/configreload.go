package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/bouine-cache/bouine/pkg/api"
)

// Config-reload fan-out codec (issue #951, phase 2): the reload
// payload is a generation counter plus a config hash — never the file
// contents, which keeps secrets out of the gossip channel — so it
// rides JSON like the hot-set frame. The receiving endpoint applies
// through the same reload path the admin API uses and never
// re-broadcasts, so the fan-out cannot loop.

const configReloadHashMaxLen = 64 // hex SHA-256

// EncodeConfigReloadHTTP serializes a config-reload event for the
// POST /v1/peer/config/reload fan-out.
func EncodeConfigReloadHTTP(evt api.ConfigReloadEvent) ([]byte, error) {
	if len(evt.ConfigSHA256) > configReloadHashMaxLen {
		return nil, fmt.Errorf("cluster: config reload hash exceeds %d bytes", configReloadHashMaxLen)
	}
	return json.Marshal(evt)
}

// DecodeConfigReloadHTTP parses a POST /v1/peer/config/reload body.
// Unknown fields are rejected so a newer sender cannot smuggle fields
// an older receiver would silently ignore.
func DecodeConfigReloadHTTP(body []byte) (api.ConfigReloadEvent, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var evt api.ConfigReloadEvent
	if err := dec.Decode(&evt); err != nil {
		return api.ConfigReloadEvent{}, fmt.Errorf("cluster: config reload frame: %w", err)
	}
	if err := dec.Decode(new(json.RawMessage)); err == nil {
		return api.ConfigReloadEvent{}, fmt.Errorf("cluster: config reload frame: trailing data")
	}
	return evt, nil
}

// EncodeConfigReloadAck serializes the peer's ack body.
func EncodeConfigReloadAck(ack api.ConfigReloadAck) ([]byte, error) {
	return json.Marshal(ack)
}

// DecodeConfigReloadAck parses the fan-out caller's view of a peer ack.
func DecodeConfigReloadAck(body []byte) (api.ConfigReloadAck, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var ack api.ConfigReloadAck
	if err := dec.Decode(&ack); err != nil {
		return api.ConfigReloadAck{}, fmt.Errorf("cluster: config reload ack: %w", err)
	}
	return ack, nil
}
