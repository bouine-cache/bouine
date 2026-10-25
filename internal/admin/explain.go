package admin

import "time"

// ExplainResult is the JSON document served by GET /v1/explain and
// rendered by `bouine explain` (issue #814): the full decision chain
// for one URL, recomputed on demand against this node's stored view.
// Nothing on the data plane records decisions — every field here is
// re-derived by the engine from the same pure functions and the
// side-effect-free store Peek the issue pins.
//
// Verdict values: "hit" (fresh), "stale-hit" (inside the swr/sie
// window), "miss", or "bypass" (a cache-disabled route). Reason
// values for a miss: "banned", "expired", "variant-absent",
// "not-stored", "not-cacheable"; empty for hits.
//
// Unstable: the admin-surface epic (#812..#817) may still adjust
// sections additively.
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

// ExplainKey is the cache key the data plane computes for this URL
// and the derivation an operator can replay by hand.
type ExplainKey struct {
	Hex        string           `json:"hex"`
	Derivation []ExplainKeyStep `json:"derivation"`
}

// ExplainKeyStep is one derivation input: host, path, the raw query,
// and the query policy's effect (the parameter names it strips).
type ExplainKeyStep struct {
	Step     string   `json:"step"`
	Value    string   `json:"value,omitempty"`
	Stripped []string `json:"stripped,omitempty"`
}

// ExplainRoute names the matched route and its upstream pool. Both
// empty when no route matches (the default key policy applies).
type ExplainRoute struct {
	Name string `json:"name,omitempty"`
	Pool string `json:"pool,omitempty"`
}

// ExplainVariant reports the variant selection under explain's
// pinned nominal request (browser-default Accept-Encoding, no other
// Vary-affecting headers): the stored response's Vary header, the
// bucket the nominal request selects, and every stored variant of
// this primary key resolved through the variant index (O(variants),
// never a store scan). Bucket is empty when the assertion is not one
// of the derivable encoding buckets.
type ExplainVariant struct {
	Vary           string                 `json:"vary,omitempty"`
	RequestBucket  string                 `json:"request_bucket,omitempty"`
	RequestKeyHex  string                 `json:"request_key_hex,omitempty"`
	StoredVariants []ExplainStoredVariant `json:"stored_variants,omitempty"`
	Matched        bool                   `json:"matched"`
}

// ExplainStoredVariant is one stored variant of the primary key:
// its assertion key, the derivable bucket label, tier, and freshness
// (age and lifetime power the miss-by-variant freshness row).
type ExplainStoredVariant struct {
	KeyHex   string `json:"key_hex"`
	Bucket   string `json:"bucket,omitempty"`
	Tier     string `json:"tier,omitempty"`
	Age      string `json:"age,omitempty"`
	Lifetime string `json:"lifetime,omitempty"`
	Fresh    bool   `json:"fresh"`
}

// ExplainCacheability re-derives whether this route would store a
// response like the stored one (or, when nothing is stored, reports
// "unknown" — cacheability is a property of a response, and explain
// never fetches one). Directives is the stored Cache-Control verbatim.
type ExplainCacheability struct {
	Verdict    string `json:"verdict"`
	Directives string `json:"directives,omitempty"`
}

// ExplainTTL is the freshness lifecycle of the explained object.
// Durations are Go duration strings; timestamps RFC3339. Verdict:
// "fresh", "stale-swr", "stale-sie", "dead", or "" when nothing is
// stored. FreshFor counts down inside the current zone (remaining
// ttl inside fresh, remaining window inside swr/sie).
type ExplainTTL struct {
	Verdict   string `json:"verdict"`
	FreshFor  string `json:"fresh_for,omitempty"`
	Age       string `json:"age,omitempty"`
	Lifetime  string `json:"lifetime,omitempty"`
	SWR       string `json:"swr,omitempty"`
	SIE       string `json:"sie,omitempty"`
	ExpiresAt string `json:"expires_at,omitempty"`
}

// ExplainStored is the stored view of the explained object: tier,
// size, variant count from the handler's variant index, and the
// KeepGrace protection flag.
type ExplainStored struct {
	Tier            string `json:"tier,omitempty"`
	SizeBytes       int64  `json:"size_bytes,omitempty"`
	VariantsPresent int    `json:"variants_present,omitempty"`
	Present         bool   `json:"present"`
	Protected       bool   `json:"protected,omitempty"`
}

// ExplainBan is one active lazy ban the explained URL (or its stored
// object) is subject to. The engine maps it from the storage layer's
// descriptor — admin keeps no storage imports.
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

// FormatExplainTime renders a time as RFC3339 for the wire, or ""
// when zero — the same convention FormatStatusTime pins.
func FormatExplainTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
