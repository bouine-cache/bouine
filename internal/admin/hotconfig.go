package admin

// Hot-config wire types for GET /v1/config/hot and
// POST /v1/config/hot (issue #815): the runtime-tunable knob surface
// behind `bouine config get/set/diff`.
//
// Unstable: the knob set grows additively as more components gain
// hot-apply support.

// HotKnob describes one runtime-tunable config knob.
type HotKnob struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	// Source is "file" when the running value matches an explicit
	// boot-config value, "default" when it matches the effective value
	// the file left to built-in defaults, or "runtime" when it was
	// changed through this surface and diverges - the drift
	// `bouine config diff` reports.
	Source string `json:"source"`
	// FileValue is the effective boot value: what a restart restores.
	// For a knob the file leaves at zero this is the built-in default
	// (e.g. 24h0m0s for cluster.ban_ttl), not "0".
	FileValue   string `json:"file_value,omitempty"`
	Default     string `json:"default,omitempty"`
	Description string `json:"description,omitempty"`
}

// HotSetRequest is the POST /v1/config/hot body.
type HotSetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	// DryRun validates the value and reports the would-be change
	// without applying it (bouine config set --dry-run).
	DryRun bool `json:"dry_run,omitempty"`
}

// HotSetResult is the POST /v1/config/hot response.
//
//nolint:govet // fieldalignment: field order mirrors the wire JSON order
type HotSetResult struct {
	Key      string `json:"key"`
	OldValue string `json:"old_value"`
	NewValue string `json:"new_value"`
	// Applied is false exactly when DryRun was requested.
	Applied bool `json:"applied"`
	// Warning is the ephemeral-change notice the CLI must surface:
	// a runtime override is lost at the next restart.
	Warning string `json:"warning,omitempty"`
}

// HotSetWarning is the canonical not-persisted notice.
const HotSetWarning = "not persisted; add to your YAML before the next deploy"
