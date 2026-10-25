package admin

import (
	"context"
	"fmt"
	"strconv"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

// Keys listing defaults and bounds. The limit ceiling is the URL
// ring's capacity: asking for more than the ring can hold is a
// configuration misunderstanding, not a truncation request.
const (
	defaultKeysTopLimit = 20
	maxKeysTopLimit     = observability.URLRingCap
)

// validKeysClass is the closed set of cache-result classes the ring
// records (the data-plane middleware skips HITs before recording, so
// "hit" is not a filterable class here).
var validKeysClass = map[string]bool{
	"miss":        true,
	"stale":       true,
	"revalidated": true,
	"bypass":      true,
}

// KeysFindResult is the JSON response for GET /v1/keys/find
// (bouine keys find, issue #816): the deterministic answer to "where
// would this URL live and what is its state on this node". The key is
// computed server-side with the same key policy the data plane uses
// for the matched route — no storage scan is involved, and the URL
// itself is unrecoverable from the stored hash (irreversible by
// design), which is why the query direction is URL → key, never the
// reverse.
//
// Unstable.
//
//nolint:govet // fieldalignment: field order mirrors the wire JSON order
type KeysFindResult struct {
	// URL is the requested raw URL.
	URL string `json:"url"`
	// KeyHex is the 32-char hex of the computed primary key.
	KeyHex string `json:"key_hex"`
	// Route is the matched route label. Empty when no route matched:
	// the default host-ful key policy then applies.
	Route string `json:"route,omitempty"`
	// Host and Path echo the URL components that feed key derivation.
	Host string `json:"host,omitempty"`
	Path string `json:"path,omitempty"`
	// QueryPolicy summarizes the query-parameter handling folded into
	// the key ("all" or a comma-joined filter description).
	QueryPolicy string `json:"query_policy"`
	// Owner is the consistent-hash ring owner of the key in cluster
	// mode; nil on a standalone node.
	Owner *api.PeerInfo `json:"owner,omitempty"`
	// Local is this node's storage state for the key.
	Local api.KeyState `json:"local"`
	// Variants is the number of tracked variant entries for the
	// primary key (0 for plain objects). A URL alone cannot select a
	// variant — that needs request headers (Vary) — so find reports
	// the resolver's state plus this count.
	Variants int `json:"variants"`
	// AbsentReason explains Local.Present=false when cheaply
	// derivable: "no_entry" when no tier holds the primary key.
	AbsentReason string `json:"absent_reason,omitempty"`
	// OwnerState is the owner node's answer when peer=true was
	// requested and the query succeeded; nil otherwise.
	OwnerState *api.KeyState `json:"owner_state,omitempty"`
	// OwnerError carries the peer-query failure when peer=true was
	// requested and the query failed. The local result is still
	// served — a peer outage must not hide local state.
	OwnerError string `json:"owner_error,omitempty"`
}

// KeysTopResult is the JSON response for GET /v1/keys/top and
// GET /v1/keys/recent (bouine keys top/recent, issue #816): the URL
// ring's recorded traffic. The ring samples non-HIT requests
// (url_ring_sample_rate) over URL prefixes, capped at the ring
// capacity — a request-traffic view of what the node is missing on,
// not a storage inventory.
//
// Unstable.
//
//nolint:govet // fieldalignment: field order mirrors the wire JSON order
type KeysTopResult struct {
	Kind string `json:"kind"`
	// Class is the requested cache-result filter ("", "miss",
	// "stale", "revalidated", "bypass").
	Class string `json:"class,omitempty"`
	// Limit is the applied entry limit.
	Limit int `json:"limit"`
	// SampleRate is the ring's current 1-in-N sampling rate (0 =
	// record every non-HIT request).
	SampleRate int            `json:"sample_rate"`
	Entries    []KeysTopEntry `json:"entries"`
}

// KeysTopEntry is one URL prefix in the keys listing.
//
//nolint:govet // fieldalignment: field order mirrors the wire JSON order
type KeysTopEntry struct {
	// Prefix is the recorded URL prefix (first three path segments).
	Prefix string `json:"prefix"`
	Route  string `json:"route,omitempty"`
	// Requests is the total sampled traffic recorded for the prefix.
	Requests int64 `json:"requests"`
	Misses   int64 `json:"misses"`
	Stale    int64 `json:"stale"`
	// Revalidated counts revalidation passes (REVALIDATED results).
	Revalidated int64 `json:"revalidated"`
	Bypass      int64 `json:"bypass"`
	// LastSeen is the RFC 3339 UTC timestamp of the prefix's most
	// recent recorded request.
	LastSeen string `json:"last_seen,omitempty"`
}

func (s *Server) keysFind(ctx *fasthttp.RequestCtx) {
	rawURL := string(ctx.QueryArgs().Peek("url"))
	if rawURL == "" {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "missing url query parameter"})
		return
	}
	v := string(ctx.QueryArgs().Peek("peer"))
	peer := v == "1" || v == "true"
	result, err := s.cfg.KeysFindFn(context.Background(), rawURL, peer)
	if err != nil {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if result.Local.Present {
		s.recordOp("keys find", rawURL, "found")
	} else {
		s.recordOp("keys find", rawURL, "absent")
	}
	writeJSON(ctx, fasthttp.StatusOK, result)
}

func (s *Server) keysTop(ctx *fasthttp.RequestCtx, kind string) {
	limit := defaultKeysTopLimit
	if v := string(ctx.QueryArgs().Peek("limit")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxKeysTopLimit {
			writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": fmt.Sprintf("limit must be between 1 and %d", maxKeysTopLimit)})
			return
		}
		limit = n
	}
	class := string(ctx.QueryArgs().Peek("class"))
	if class != "" && !validKeysClass[class] {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "class must be one of miss, stale, revalidated, bypass"})
		return
	}
	result := s.cfg.KeysTopFn(kind, class, limit)
	s.recordOp("keys "+kind, class, "ok")
	writeJSON(ctx, fasthttp.StatusOK, result)
}
