package admin

import (
	"errors"
)

// Config-reload wire types for POST /v1/config/reload (issue #817,
// phase 1): re-parse and validate a config file, apply the
// hot-appliable subset atomically, and report what could not be
// applied.
//
// Unstable: phase 2 (route table + pool target swap) extends the
// applied set under the same endpoint.

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

// ConfigReloadResult is the POST /v1/config/reload report. The
// operator must never believe a field applied when it did not:
// anything in NeedsRestart requires a restart to take effect.
type ConfigReloadResult struct {
	// Path is the config file that was (re)loaded.
	Path string `json:"path"`
	// Applied lists the hot-config knobs whose live value changed,
	// with old and new values.
	Applied []ConfigReloadChange `json:"applied,omitempty"`
	// Unchanged lists the hot-config knobs already at the file value
	// (runtime drift the file reverts counts as applied, not
	// unchanged).
	Unchanged []string `json:"unchanged,omitempty"`
	// NeedsRestart lists config paths whose change could not be
	// applied to the running daemon: cold-only fields (listeners,
	// TLS, storage, cluster, routes, pool membership, ...) and knobs
	// that failed to apply, each with the reason.
	NeedsRestart []string `json:"needs_restart,omitempty"`
}
