package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/bouine-cache/bouine/pkg/api"
)

// Hot-config fan-out codec (issue #815): unlike purge/ban/refresh,
// runtime knob sets are rare operator actions with two short string
// fields, so they ride JSON instead of the binary invalidation frames.
// The receiving endpoint applies them through the same knob registry
// the admin API uses (validation, normalization, audit) and never
// re-broadcasts, so the fan-out cannot loop.

const hotSetValueMaxLen = maxStringLen

// EncodeHotSetHTTP serializes a hot-config set event for the
// POST /v1/peer/config/hot fan-out.
func EncodeHotSetHTTP(evt api.HotSetEvent) ([]byte, error) {
	if len(evt.Key) > hotSetValueMaxLen {
		return nil, fmt.Errorf("cluster: hot set key exceeds %d bytes", hotSetValueMaxLen)
	}
	if len(evt.Value) > hotSetValueMaxLen {
		return nil, fmt.Errorf("cluster: hot set value exceeds %d bytes", hotSetValueMaxLen)
	}
	return json.Marshal(evt)
}

// DecodeHotSetHTTP parses a POST /v1/peer/config/hot body. Unknown
// fields are rejected so a newer sender cannot smuggle fields an
// older receiver would silently ignore.
func DecodeHotSetHTTP(body []byte) (api.HotSetEvent, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var evt api.HotSetEvent
	if err := dec.Decode(&evt); err != nil {
		return api.HotSetEvent{}, fmt.Errorf("cluster: hot set frame: %w", err)
	}
	// Reject trailing garbage after the object, matching the strictness
	// of the binary frames.
	if err := dec.Decode(new(json.RawMessage)); err == nil {
		return api.HotSetEvent{}, fmt.Errorf("cluster: hot set frame: trailing data")
	}
	return evt, nil
}
