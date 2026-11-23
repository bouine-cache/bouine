package bouineapi

import (
	"context"
)

// PeerReloadAck mirrors admin.PeerReloadAck.
type PeerReloadAck struct {
	Peer   string `json:"peer"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// ConfigReloadChange mirrors the admin applied-knob entries (issue #817).
// Wire-compatible with admin.ConfigReloadChange; add fields, never remove.
type ConfigReloadChange struct {
	Key string `json:"key"`
	Old string `json:"old"`
	New string `json:"new"`
}

// ConfigReloadResult mirrors the admin POST /v1/config/reload report.
// Phase 2 (issue #951) added Gen, Result and Peers — additive only.
// Anything in NeedsRestart requires a restart to take effect — the
// operator must never believe a field applied when it did not.
//
//nolint:govet // fieldalignment: wire/DTO type, JSON field order is the contract
type ConfigReloadResult struct {
	// Peers lists per-peer fan-out acks (phase 2).
	Peers []PeerReloadAck `json:"peers,omitempty"`
	// Applied lists the hot-config knobs whose live value changed.
	Applied []ConfigReloadChange `json:"applied,omitempty"`
	// Unchanged lists the hot-config knobs already at the file value.
	Unchanged []string `json:"unchanged,omitempty"`
	// NeedsRestart lists config paths that could not be applied to
	// the running daemon (cold-only fields, or knobs that refused),
	// each with the reason.
	NeedsRestart []string `json:"needs_restart,omitempty"`
	// Gen is the config generation after the reload (issue #951).
	Gen uint64 `json:"gen,omitempty"`
	// Path is the config file that was (re)loaded.
	Path string `json:"path"`
	// Result is applied_all / applied_local_only / applied (phase 2).
	Result string `json:"result,omitempty"`
}

// ConfigReload asks the daemon to re-parse and apply a config file:
// the hot-appliable subset takes effect immediately, everything else
// is reported as needing a restart. An empty path reloads the config
// file the daemon was started with.
func (c *Client) ConfigReload(ctx context.Context, path string) (*ConfigReloadResult, error) {
	var out ConfigReloadResult
	body := struct {
		Path string `json:"path,omitempty"`
	}{Path: path}
	if err := c.post(ctx, "/v1/config/reload", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
