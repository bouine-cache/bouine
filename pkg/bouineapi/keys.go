package bouineapi

import (
	"context"
	"fmt"
	"net/url"

	"github.com/bouine-cache/bouine/pkg/api"
)

// KeysFindResult mirrors the admin GET /v1/keys/find report
// (issue #816). Wire-compatible with admin.KeysFindResult; add
// fields, never remove.
//
//nolint:govet // fieldalignment: field order mirrors the wire JSON order and the admin type
type KeysFindResult struct {
	// URL is the requested raw URL.
	URL string `json:"url"`
	// KeyHex is the 32-char hex of the computed primary key.
	KeyHex string `json:"key_hex"`
	// Route is the matched route label ("" when none matched).
	Route string `json:"route,omitempty"`
	// Host and Path echo the URL components used in key derivation.
	Host string `json:"host,omitempty"`
	Path string `json:"path,omitempty"`
	// QueryPolicy summarizes the query-parameter handling folded
	// into the key ("all" or a filter description).
	QueryPolicy string `json:"query_policy"`
	// Owner is the consistent-hash ring owner in cluster mode.
	Owner *api.PeerInfo `json:"owner,omitempty"`
	// Local is this node's storage state for the key.
	Local api.KeyState `json:"local"`
	// Variants is the number of tracked variant entries for the
	// primary key.
	Variants int `json:"variants"`
	// AbsentReason explains Local.Present=false ("no_entry").
	AbsentReason string `json:"absent_reason,omitempty"`
	// OwnerState is the owner's answer when peer=true and the query
	// succeeded.
	OwnerState *api.KeyState `json:"owner_state,omitempty"`
	// OwnerError carries the peer-query failure when peer=true and
	// the query failed; the local result is still served.
	OwnerError string `json:"owner_error,omitempty"`
}

// KeysTopEntry mirrors one admin listing row.
//
//nolint:govet // fieldalignment: field order mirrors the wire JSON order and the admin type
type KeysTopEntry struct {
	// Prefix is the recorded URL prefix (first three path segments).
	Prefix string `json:"prefix"`
	Route  string `json:"route,omitempty"`
	// Requests is the total sampled traffic recorded for the prefix.
	Requests    int64 `json:"requests"`
	Misses      int64 `json:"misses"`
	Stale       int64 `json:"stale"`
	Revalidated int64 `json:"revalidated"`
	Bypass      int64 `json:"bypass"`
	// LastSeen is the RFC 3339 UTC timestamp of the prefix's most
	// recent recorded request.
	LastSeen string `json:"last_seen,omitempty"`
}

// KeysTopResult mirrors the admin GET /v1/keys/top and
// GET /v1/keys/recent listing (issue #816). The ring samples non-HIT
// requests; this is a request-traffic view, not a storage inventory.
//
//nolint:govet // fieldalignment: field order mirrors the wire JSON order and the admin type
type KeysTopResult struct {
	Kind       string         `json:"kind"`
	Class      string         `json:"class,omitempty"`
	Limit      int            `json:"limit"`
	SampleRate int            `json:"sample_rate"`
	Entries    []KeysTopEntry `json:"entries"`
}

// KeysFind reports where a URL's cache object lives and what state it
// is in: the daemon computes the key with the matched route's key
// policy — the same key the data plane uses — and probes its tiers.
// With peer=true in cluster mode the ring owner is also queried;
// owner errors do not fail the call, they surface in OwnerError.
func (c *Client) KeysFind(ctx context.Context, rawURL string, peer bool) (*KeysFindResult, error) {
	path := fmt.Sprintf("/v1/keys/find?url=%s", url.QueryEscape(rawURL))
	if peer {
		path += "&peer=1"
	}
	var out KeysFindResult
	if err := c.get(ctx, path, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// KeysTop lists the URL ring's most-recorded URL prefixes with their
// cache-result breakdown: what the node is missing on. class filters
// to one cache result ("", "miss", "stale", "revalidated", "bypass").
func (c *Client) KeysTop(ctx context.Context, limit int, class string) (*KeysTopResult, error) {
	return c.keysList(ctx, "top", limit, class)
}

// KeysRecent lists the URL ring's prefixes ordered by most recent
// recorded request. Same parameters and semantics as KeysTop.
func (c *Client) KeysRecent(ctx context.Context, limit int, class string) (*KeysTopResult, error) {
	return c.keysList(ctx, "recent", limit, class)
}

func (c *Client) keysList(ctx context.Context, kind string, limit int, class string) (*KeysTopResult, error) {
	path := fmt.Sprintf("/v1/keys/%s?limit=%d", kind, limit)
	if class != "" {
		path += "&class=" + url.QueryEscape(class)
	}
	var out KeysTopResult
	if err := c.get(ctx, path, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
