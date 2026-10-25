package bouineapi

import (
	"context"
)

// HotKnob mirrors the admin GET /v1/config/hot entries (issue #815).
// Wire-compatible with admin.HotKnob; add fields, never remove.
//
// Unstable until the admin-surface epic settles.
type HotKnob struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	// Source is "file", "default", or "runtime" (a live override
	// that diverges from the boot config and is lost at restart).
	Source string `json:"source"`
	// FileValue is the effective boot value: what a restart restores.
	FileValue   string `json:"file_value,omitempty"`
	Default     string `json:"default,omitempty"`
	Description string `json:"description,omitempty"`
}

// HotSetResult mirrors the admin POST /v1/config/hot response.
//
//nolint:govet // fieldalignment: field order mirrors the wire JSON order and the admin type
type HotSetResult struct {
	Key      string `json:"key"`
	OldValue string `json:"old_value"`
	NewValue string `json:"new_value"`
	// Applied is false exactly when the request asked for a dry run.
	Applied bool `json:"applied"`
	// Peers is the number of cluster peers that acknowledged the
	// fan-out of an applied set (0 on a single node, dry run, or
	// eventual-mode cluster).
	Peers   int    `json:"peers,omitempty"`
	Warning string `json:"warning,omitempty"`
}

// HotConfig lists the runtime-tunable knobs with their live values
// and sources (bouine config get / config diff).
func (c *Client) HotConfig(ctx context.Context) ([]HotKnob, error) {
	var out []HotKnob
	if err := c.get(ctx, "/v1/config/hot", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// HotSet applies a validated point mutation to one hot knob. With
// dryRun the value is validated and reported but not applied
// (bouine config set --dry-run).
func (c *Client) HotSet(ctx context.Context, key, value string, dryRun bool) (*HotSetResult, error) {
	var out HotSetResult
	body := struct {
		Key    string `json:"key"`
		Value  string `json:"value"`
		DryRun bool   `json:"dry_run,omitempty"`
	}{Key: key, Value: value, DryRun: dryRun}
	if err := c.post(ctx, "/v1/config/hot", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
