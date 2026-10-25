package admin

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

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
