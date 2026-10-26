package admin

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

// explainGoldenJSON is the canonical wire document for GET /v1/explain:
// every field populated. It is byte-identical to the fixture in
// pkg/bouineapi/explain_test.go — the two packages cannot import each
// other, so the shared golden is duplicated and any drift between the
// admin encoder and the SDK decoder breaks exactly one of the two
// tests. Edit both together.
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

func explainServer(t *testing.T, fn func(ctx context.Context, rawURL string) ExplainResult) *Server {
	t.Helper()
	return New(Config{
		Token:     "secret",
		ExplainFn: fn,
	})
}

func TestExplainEndpoint_ServesDocument(t *testing.T) {
	t.Parallel()
	s := explainServer(t, func(_ context.Context, rawURL string) ExplainResult {
		require.Equal(t, "https://example.com/p?utm_source=nl", rawURL)
		return ExplainResult{
			URL:         rawURL,
			Verdict:     "miss",
			CacheResult: "MISS",
			Reason:      "banned",
			Key: ExplainKey{
				Hex: "6b2d9a0e5c7f1a3b",
				Derivation: []ExplainKeyStep{
					{Step: "host", Value: "example.com"},
					{Step: "query_policy", Stripped: []string{"utm_source"}},
				},
			},
			Bans: []ExplainBan{{
				PathRegex: "^/p/", CreatedAt: "2026-10-25T05:54:00Z",
				ExpiresAt: "2026-10-26T05:54:00Z",
			}},
			Cluster: ExplainCluster{OwnerPeer: "bouine-2 (10.0.0.2:9010)"},
		}
	})
	status, body := authedGet(t, s, "/v1/explain?url=https%3A%2F%2Fexample.com%2Fp%3Futm_source%3Dnl")
	require.Equal(t, fasthttp.StatusOK, status, "body: %s", body)

	var got ExplainResult
	require.NoError(t, json.Unmarshal(body, &got))
	assert.Equal(t, "miss", got.Verdict)
	assert.Equal(t, "banned", got.Reason)
	assert.Equal(t, "utm_source", got.Key.Derivation[1].Stripped[0])
	require.Len(t, got.Bans, 1)
	assert.Equal(t, "^/p/", got.Bans[0].PathRegex)
	assert.Equal(t, "bouine-2 (10.0.0.2:9010)", got.Cluster.OwnerPeer)
}

func TestExplainEndpoint_MissingURLIs400(t *testing.T) {
	t.Parallel()
	s := explainServer(t, func(context.Context, string) ExplainResult { return ExplainResult{} })
	status, _ := authedGet(t, s, "/v1/explain")
	assert.Equal(t, fasthttp.StatusBadRequest, status)
}

func TestExplainEndpoint_RequiresAuth(t *testing.T) {
	t.Parallel()
	s := explainServer(t, func(context.Context, string) ExplainResult { return ExplainResult{} })
	ctx := testCtx("GET", "/v1/explain?url=https://example.com/x")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusUnauthorized, ctx.Response.StatusCode())
}

func TestExplainEndpoint_DisabledWithoutFn(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, nil)
	status, _ := authedGet(t, s, "/v1/explain?url=https://example.com/x")
	assert.Equal(t, fasthttp.StatusNotFound, status)
}

// TestExplainWire_EncodesCanonicalGolden pins the admin encoder
// against the canonical wire document: a fully-populated ExplainResult
// must encode to exactly the golden's field set (order-insensitive),
// and the golden must decode back losslessly. The same golden literal
// lives in pkg/bouineapi/explain_test.go to pin the SDK decoder —
// edit both together.
func TestExplainWire_EncodesCanonicalGolden(t *testing.T) {
	t.Parallel()
	full := ExplainResult{
		URL:         "https://shop.example.com/p?utm_source=nl",
		Verdict:     "miss",
		CacheResult: "MISS",
		Reason:      "banned",
		Key: ExplainKey{
			Hex: "6b2d9a0e5c7f1a3b",
			Derivation: []ExplainKeyStep{
				{Step: "host", Value: "shop.example.com"},
				{Step: "path", Value: "/p"},
				{Step: "query", Value: "utm_source=nl"},
				{Step: "query_policy", Stripped: []string{"utm_source"}},
			},
		},
		Route: ExplainRoute{Name: "shop", Pool: "origins-shop"},
		Variant: ExplainVariant{
			Vary:           "Accept-Encoding",
			RequestBucket:  "br",
			RequestKeyHex:  "abc12345",
			StoredVariants: []ExplainStoredVariant{{KeyHex: "1111", Bucket: "gzip", Tier: "hot", Age: "12m0s", Lifetime: "1h0m0s", Fresh: true}},
		},
		Cacheable: ExplainCacheability{Verdict: "cacheable", Directives: "public, max-age=3600"},
		TTL: ExplainTTL{
			Verdict: "fresh", FreshFor: "48m0s", Age: "12m0s", Lifetime: "1h0m0s",
			SWR: "5m0s", SIE: "30m0s", ExpiresAt: "2026-10-25T07:04:00Z",
		},
		Bans: []ExplainBan{{
			HostRegex: "shop.example.com", PathRegex: "^/p/", SurrogateKey: "products",
			CreatedAt: "2026-10-25T05:54:00Z", ExpiresAt: "2026-10-26T05:54:00Z",
		}},
		Cluster: ExplainCluster{OwnerPeer: "bouine-2 (10.0.0.2:9010)", Local: false},
		Stored:  ExplainStored{Tier: "hot", SizeBytes: 131328, VariantsPresent: 1, Present: true, Protected: false},
	}

	encoded, err := json.Marshal(full)
	require.NoError(t, err)
	var gotMap, wantMap map[string]any
	require.NoError(t, json.Unmarshal(encoded, &gotMap))
	require.NoError(t, json.Unmarshal([]byte(explainGoldenJSON), &wantMap))
	require.Equal(t, wantMap, gotMap,
		"the admin encoder must produce the canonical field set the SDK golden pins")

	var roundTripped ExplainResult
	require.NoError(t, json.Unmarshal([]byte(explainGoldenJSON), &roundTripped))
	assert.Equal(t, full, roundTripped, "the golden decodes losslessly into the admin type")
}
