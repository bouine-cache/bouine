package bouineapi

import (
	"context"
	"net/url"
)

// ExplainResult mirrors the admin GET /v1/explain document (issue
// #814): the recomputed decision chain for one URL under the pinned
// nominal request. Wire-compatible with admin.ExplainResult; add
// fields, never remove.
//
// Verdict: "hit", "stale-hit", "miss", or "bypass". Reason (misses
// only): "banned", "expired", "variant-absent", "not-stored", or
// "not-cacheable".
//
// Unstable until the admin-surface epic settles.
type ExplainResult struct {
	URL         string              `json:"url"`
	Verdict     string              `json:"verdict"`
	CacheResult string              `json:"cache_result"`
	Reason      string              `json:"reason,omitempty"`
	Key         ExplainKey          `json:"key"`
	Route       ExplainRoute        `json:"route"`
	Variant     ExplainVariant      `json:"variant"`
	Cacheable   ExplainCacheability `json:"cacheability"`
	TTL         ExplainTTL          `json:"ttl"`
	Bans        []ExplainBan        `json:"bans"`
	Cluster     ExplainCluster      `json:"cluster"`
	Stored      ExplainStored       `json:"stored"`
}

// ExplainKey is the cache key the data plane computes and the
// derivation an operator can replay by hand.
type ExplainKey struct {
	Hex        string           `json:"hex"`
	Derivation []ExplainKeyStep `json:"derivation"`
}

// ExplainKeyStep is one derivation input: host, path, the raw
// query, or the query policy's stripped parameter names.
type ExplainKeyStep struct {
	Step     string   `json:"step"`
	Value    string   `json:"value,omitempty"`
	Stripped []string `json:"stripped,omitempty"`
}

// ExplainRoute names the matched route and its upstream pool.
type ExplainRoute struct {
	Name string `json:"name,omitempty"`
	Pool string `json:"pool,omitempty"`
}

// ExplainVariant reports the variant selection under explain's
// pinned nominal request: the stored Vary, the bucket the nominal
// request selects, and the live stored variants of the primary key.
type ExplainVariant struct {
	Vary           string                 `json:"vary,omitempty"`
	RequestBucket  string                 `json:"request_bucket,omitempty"`
	RequestKeyHex  string                 `json:"request_key_hex,omitempty"`
	StoredVariants []ExplainStoredVariant `json:"stored_variants,omitempty"`
	Matched        bool                   `json:"matched"`
}

// ExplainStoredVariant is one live stored variant.
type ExplainStoredVariant struct {
	KeyHex   string `json:"key_hex"`
	Bucket   string `json:"bucket,omitempty"`
	Tier     string `json:"tier,omitempty"`
	Age      string `json:"age,omitempty"`
	Lifetime string `json:"lifetime,omitempty"`
	Fresh    bool   `json:"fresh"`
}

// ExplainCacheability re-derives whether this route would store a
// response like the stored one; "unknown" when nothing is stored.
type ExplainCacheability struct {
	Verdict    string `json:"verdict"`
	Directives string `json:"directives,omitempty"`
}

// ExplainTTL is the freshness lifecycle of the explained object.
// Durations are Go duration strings, timestamps RFC3339. Verdict:
// "fresh", "stale-swr", "stale-sie", "dead", or "" when nothing is
// stored. FreshFor is the remaining life in the current zone.
type ExplainTTL struct {
	Verdict   string `json:"verdict"`
	FreshFor  string `json:"fresh_for,omitempty"`
	Age       string `json:"age,omitempty"`
	Lifetime  string `json:"lifetime,omitempty"`
	SWR       string `json:"swr,omitempty"`
	SIE       string `json:"sie,omitempty"`
	ExpiresAt string `json:"expires_at,omitempty"`
}

// ExplainStored is the stored view of the explained object.
type ExplainStored struct {
	Tier            string `json:"tier,omitempty"`
	SizeBytes       int64  `json:"size_bytes,omitempty"`
	VariantsPresent int    `json:"variants_present,omitempty"`
	Present         bool   `json:"present"`
	Protected       bool   `json:"protected,omitempty"`
}

// ExplainBan is one active lazy ban the explained URL is subject to.
type ExplainBan struct {
	HostRegex    string `json:"host_regex,omitempty"`
	PathRegex    string `json:"path_regex,omitempty"`
	SurrogateKey string `json:"surrogate_key,omitempty"`
	CreatedAt    string `json:"created_at"`
	ExpiresAt    string `json:"expires_at,omitempty"`
}

// ExplainCluster reports which peer owns the key on the ring.
type ExplainCluster struct {
	OwnerPeer string `json:"owner_peer,omitempty"`
	Local     bool   `json:"local"`
}

// Explain fetches the decision-tree document for one URL (bouine
// explain, issue #814). Read-only on the server: side-effect-free
// store reads only.
func (c *Client) Explain(ctx context.Context, rawURL string) (*ExplainResult, error) {
	var out ExplainResult
	if err := c.get(ctx, "/v1/explain?url="+url.QueryEscape(rawURL), &out); err != nil {
		return nil, err
	}
	return &out, nil
}
