package admin

import (
	"errors"
)

// Config-reload wire types for POST /v1/config/reload (issue #817,
// phase 1): re-parse and validate a config file, apply the
// hot-appliable subset atomically, and report what could not be
// applied.
//
// Unstable: phase 2 (route table + pool target swap with cluster
// fan-out, issue #951) extends the applied set under the same
// endpoint; Gen/Result/Peers were added in phase 2.

// ErrReloadDisabled is returned when the daemon runs without
// experimental.config_reload; the endpoint answers 503 with it so
// operators get the opt-in instructions instead of a bare 404.
var ErrReloadDisabled = errors.New("config reload is disabled: set experimental.config_reload to enable it")

// ConfigReloadRequest is the POST /v1/config/reload body. An empty
// path reloads the config file the daemon was started with.
type ConfigReloadRequest struct {
	Path string `json:"path,omitempty"`
}

// ConfigReloadChange is one applied knob: key, the value it replaced,
// and the normalized value now running.
type ConfigReloadChange struct {
	Key string `json:"key"`
	Old string `json:"old"`
	New string `json:"new"`
}

// PeerReloadAck is one peer's answer to the config-reload fan-out.
type PeerReloadAck struct {
	Peer   string `json:"peer"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// ConfigReloadResult is the POST /v1/config/reload report. The
// operator must never believe a field applied when it did not:
// anything in NeedsRestart requires a restart to take effect.
//
//nolint:govet // fieldalignment: wire/DTO type, JSON field order is the contract
type ConfigReloadResult struct {
	// Peers lists each live peer's ack status for the fan-out
	// (applied/current/stale_config/error), empty on single-node.
	Peers []PeerReloadAck `json:"peers,omitempty"`
	// Applied lists the hot-config knobs whose live value changed,
	// with old and new values.
	Applied []ConfigReloadChange `json:"applied,omitempty"`
	// Unchanged lists the hot-config knobs already at the file value
	// (runtime drift the file reverts counts as applied, not
	// unchanged).
	Unchanged []string `json:"unchanged,omitempty"`
	// NeedsRestart lists config paths whose change could not be
	// applied to the running daemon: cold-only fields (listeners,
	// TLS, storage, cluster, connect.*, health.active.*, static.root,
	// ...) and knobs that failed to apply, each with the reason. In
	// phase 2 (issue #951) routes and pool membership/targets are hot
	// and never appear here.
	NeedsRestart []string `json:"needs_restart,omitempty"`
	// Path is the config file that was (re)loaded.
	Path string `json:"path"`
	// Gen is the config generation after the reload (boot = 1, each
	// applied reload increments; 0 when rejected). Exposed so the
	// fleet-convergence check (generation skew alert, issue #951
	// decision 13) can compare peers.
	Gen uint64 `json:"gen,omitempty"`
	// Result is applied_all (key-form change acked by every live
	// peer), applied_local_only (non-key-form change; peers may lag),
	// or applied; rejected and noop report through other fields.
	Result string `json:"result,omitempty"`
}
