package bouineapi

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// explainGoldenJSON is the canonical wire document for GET /v1/explain:
// every field populated. It is byte-identical to the fixture in
// internal/admin/explain_test.go — the SDK cannot import internal/, so
// the shared golden is duplicated and any drift between the admin
// encoder and this decoder breaks exactly one of the two tests. Edit
// both together.
const explainGoldenJSON = `{
  "url": "https://shop.example.com/p?utm_source=nl",
  "verdict": "miss",
  "cache_result": "MISS",
  "reason": "banned",
  "key": {
    "hex": "6b2d9a0e5c7f1a3b",
    "derivation": [
      {"step": "host", "value": "shop.example.com"},
      {"step": "path", "value": "/p"},
      {"step": "query", "value": "utm_source=nl"},
      {"step": "query_policy", "stripped": ["utm_source"]}
    ]
  },
  "route": {"name": "shop", "pool": "origins-shop"},
  "variant": {
    "vary": "Accept-Encoding",
    "request_bucket": "br",
    "request_key_hex": "abc12345",
    "stored_variants": [
      {"key_hex": "1111", "bucket": "gzip", "tier": "hot", "age": "12m0s", "lifetime": "1h0m0s", "fresh": true}
    ],
    "matched": false
  },
  "cacheability": {"verdict": "cacheable", "directives": "public, max-age=3600"},
  "ttl": {"verdict": "fresh", "fresh_for": "48m0s", "age": "12m0s", "lifetime": "1h0m0s", "swr": "5m0s", "sie": "30m0s", "expires_at": "2026-10-25T07:04:00Z"},
  "bans": [{"created_at": "2026-10-25T05:54:00Z", "expires_at": "2026-10-26T05:54:00Z", "host_regex": "shop.example.com", "path_regex": "^/p/", "surrogate_key": "products"}],
  "cluster": {"owner_peer": "bouine-2 (10.0.0.2:9010)", "local": false},
  "stored": {"tier": "hot", "size_bytes": 131328, "variants_present": 1, "present": true}
}`

// TestExplain_DecodesCanonicalGolden pins the SDK decoder against the
// canonical wire document: every field the admin plane encodes must
// surface here, or the CLI renders a lie. The same golden literal
// lives in internal/admin/explain_test.go to pin the encoder — edit
// both together.
func TestExplain_DecodesCanonicalGolden(t *testing.T) {
	t.Parallel()
	var doc ExplainResult
	require.NoError(t, json.Unmarshal([]byte(explainGoldenJSON), &doc))

	assert.Equal(t, "https://shop.example.com/p?utm_source=nl", doc.URL)
	assert.Equal(t, "miss", doc.Verdict)
	assert.Equal(t, "MISS", doc.CacheResult)
	assert.Equal(t, "banned", doc.Reason)

	assert.Equal(t, "6b2d9a0e5c7f1a3b", doc.Key.Hex)
	require.Len(t, doc.Key.Derivation, 4)
	assert.Equal(t, "host", doc.Key.Derivation[0].Step)
	assert.Equal(t, []string{"utm_source"}, doc.Key.Derivation[3].Stripped)

	assert.Equal(t, "shop", doc.Route.Name)
	assert.Equal(t, "origins-shop", doc.Route.Pool)

	assert.Equal(t, "Accept-Encoding", doc.Variant.Vary)
	assert.Equal(t, "br", doc.Variant.RequestBucket)
	assert.Equal(t, "abc12345", doc.Variant.RequestKeyHex)
	assert.False(t, doc.Variant.Matched)
	require.Len(t, doc.Variant.StoredVariants, 1)
	assert.Equal(t, "gzip", doc.Variant.StoredVariants[0].Bucket)
	assert.Equal(t, "hot", doc.Variant.StoredVariants[0].Tier)
	assert.True(t, doc.Variant.StoredVariants[0].Fresh)
	assert.Equal(t, "12m0s", doc.Variant.StoredVariants[0].Age)
	assert.Equal(t, "1h0m0s", doc.Variant.StoredVariants[0].Lifetime)

	assert.Equal(t, "cacheable", doc.Cacheable.Verdict)
	assert.Equal(t, "public, max-age=3600", doc.Cacheable.Directives)

	assert.Equal(t, "fresh", doc.TTL.Verdict)
	assert.Equal(t, "48m0s", doc.TTL.FreshFor)
	assert.Equal(t, "12m0s", doc.TTL.Age)
	assert.Equal(t, "1h0m0s", doc.TTL.Lifetime)
	assert.Equal(t, "5m0s", doc.TTL.SWR)
	assert.Equal(t, "30m0s", doc.TTL.SIE)
	assert.Equal(t, "2026-10-25T07:04:00Z", doc.TTL.ExpiresAt)

	require.Len(t, doc.Bans, 1)
	assert.Equal(t, "shop.example.com", doc.Bans[0].HostRegex)
	assert.Equal(t, "^/p/", doc.Bans[0].PathRegex)
	assert.Equal(t, "products", doc.Bans[0].SurrogateKey)
	assert.Equal(t, "2026-10-25T05:54:00Z", doc.Bans[0].CreatedAt)
	assert.Equal(t, "2026-10-26T05:54:00Z", doc.Bans[0].ExpiresAt)

	assert.False(t, doc.Cluster.Local)
	assert.Equal(t, "bouine-2 (10.0.0.2:9010)", doc.Cluster.OwnerPeer)

	assert.True(t, doc.Stored.Present)
	assert.Equal(t, "hot", doc.Stored.Tier)
	assert.Equal(t, int64(131328), doc.Stored.SizeBytes)
	assert.Equal(t, 1, doc.Stored.VariantsPresent)
}

// TestExplain_UnknownFieldsIgnored pins forward-compatibility: the
// additive-only wire contract means a newer server may send fields
// this SDK does not know; decoding must not fail.
func TestExplain_UnknownFieldsIgnored(t *testing.T) {
	t.Parallel()
	extended := explainGoldenJSON[:len(explainGoldenJSON)-2] + `,
  "future_field": {"nested": [1, 2, 3]},
  "another": 42
}`
	var doc ExplainResult
	require.NoError(t, json.Unmarshal([]byte(extended), &doc))
	assert.Equal(t, "miss", doc.Verdict, "the known fields still decode")
}
