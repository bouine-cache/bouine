package admin

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

// hotKnobFixture covers all three sources so decoding tests exercise
// every field.
func hotKnobFixture() []HotKnob {
	return []HotKnob{
		{
			Key: "cluster.ban_ttl", Value: "24h0m0s", Source: "default",
			FileValue: "24h0m0s", Default: "24h0m0s",
			Description: "how long a lazy invalidation ban stays active",
		},
		{
			Key: "url_ring_sample_rate", Value: "100", Source: "runtime",
			FileValue: "0", Default: "0",
			Description: "1-in-N sampling for the dashboard URL ring",
		},
	}
}

func hotConfigServer(t *testing.T, knobs func() []HotKnob, set func(context.Context, HotSetRequest) (HotSetResult, error)) *Server {
	t.Helper()
	cfg := Config{Token: "secret"}
	if knobs != nil {
		cfg.HotKnobsFn = knobs
	}
	if set != nil {
		cfg.HotSetFn = set
	}
	return New(cfg)
}

func TestHotConfig_GetServesKnobs(t *testing.T) {
	t.Parallel()
	s := hotConfigServer(t, hotKnobFixture, nil)

	ctx := testCtxWithAuth("GET", "/v1/config/hot", "secret")
	s.Handler()(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode(), "body: %s", ctx.Response.Body())

	var got []HotKnob
	require.NoError(t, json.Unmarshal(ctx.Response.Body(), &got))
	require.Len(t, got, 2)
	assert.Equal(t, "cluster.ban_ttl", got[0].Key)
	assert.Equal(t, "default", got[0].Source)
	assert.Equal(t, "24h0m0s", got[0].FileValue)
	assert.Equal(t, "url_ring_sample_rate", got[1].Key)
	assert.Equal(t, "runtime", got[1].Source)
}

func TestHotConfig_GetRequiresAuth(t *testing.T) {
	t.Parallel()
	s := hotConfigServer(t, hotKnobFixture, nil)
	ctx := testCtx("GET", "/v1/config/hot")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusUnauthorized, ctx.Response.StatusCode())
}

func TestHotConfig_DisabledWithoutFns(t *testing.T) {
	t.Parallel()
	s := hotConfigServer(t, nil, nil)
	for _, tc := range []struct {
		method string
		body   []byte
	}{
		{method: "GET"},
		{method: "POST", body: []byte(`{"key":"cluster.ban_ttl","value":"1h"}`)},
	} {
		ctx := testCtxWithBodyAuth(tc.method, "/v1/config/hot", tc.body, "secret")
		s.Handler()(ctx)
		assert.Equal(t, fasthttp.StatusNotFound, ctx.Response.StatusCode(),
			"%s without wired fns must 404", tc.method)
	}
}

func TestHotConfig_PostApplies(t *testing.T) {
	t.Parallel()
	applied := "none"
	s := hotConfigServer(t, hotKnobFixture, func(_ context.Context, req HotSetRequest) (HotSetResult, error) {
		applied = req.Value
		return HotSetResult{
			Key: req.Key, OldValue: "24h0m0s", NewValue: "1h30m0s",
			Applied: true, Warning: HotSetWarning,
		}, nil
	})

	body := []byte(`{"key":"cluster.ban_ttl","value":"90m"}`)
	ctx := testCtxWithBodyAuth("POST", "/v1/config/hot", body, "secret")
	s.Handler()(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode(), "body: %s", ctx.Response.Body())
	assert.Equal(t, "90m", applied)

	var res HotSetResult
	require.NoError(t, json.Unmarshal(ctx.Response.Body(), &res))
	assert.True(t, res.Applied)
	assert.Equal(t, "1h30m0s", res.NewValue)
	assert.Equal(t, HotSetWarning, res.Warning)
}

func TestHotConfig_PostDryRunPassesThrough(t *testing.T) {
	t.Parallel()
	var got HotSetRequest
	s := hotConfigServer(t, hotKnobFixture, func(_ context.Context, req HotSetRequest) (HotSetResult, error) {
		got = req
		return HotSetResult{
			Key: req.Key, OldValue: "0", NewValue: "5",
			Applied: false, Warning: HotSetWarning,
		}, nil
	})

	body := []byte(`{"key":"url_ring_sample_rate","value":"5","dry_run":true}`)
	ctx := testCtxWithBodyAuth("POST", "/v1/config/hot", body, "secret")
	s.Handler()(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode(), "body: %s", ctx.Response.Body())
	assert.True(t, got.DryRun, "dry_run must reach the setter fn")

	var res HotSetResult
	require.NoError(t, json.Unmarshal(ctx.Response.Body(), &res))
	assert.False(t, res.Applied, "dry run results must report applied=false")
}

func TestHotConfig_PostErrorIs400(t *testing.T) {
	t.Parallel()
	s := hotConfigServer(t, hotKnobFixture, func(context.Context, HotSetRequest) (HotSetResult, error) {
		return HotSetResult{}, assert.AnError
	})
	body := []byte(`{"key":"cluster.ban_ttl","value":"bogus"}`)
	ctx := testCtxWithBodyAuth("POST", "/v1/config/hot", body, "secret")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
	assert.Contains(t, string(ctx.Response.Body()), "error")
}

func TestHotConfig_PostRequiresKey(t *testing.T) {
	t.Parallel()
	s := hotConfigServer(t, hotKnobFixture, func(context.Context, HotSetRequest) (HotSetResult, error) {
		t.Error("setter must not be called for a keyless request")
		return HotSetResult{}, nil
	})
	ctx := testCtxWithBodyAuth("POST", "/v1/config/hot", []byte(`{"value":"1h"}`), "secret")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
}

func TestHotConfig_MethodRouting(t *testing.T) {
	t.Parallel()
	s := hotConfigServer(t, hotKnobFixture, func(context.Context, HotSetRequest) (HotSetResult, error) {
		return HotSetResult{Key: "k", Applied: true}, nil
	})

	ctx := testCtxWithAuth("PUT", "/v1/config/hot", "secret")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusMethodNotAllowed, ctx.Response.StatusCode(), "PUT must 405")

	// GET still reaches the knob listing after the write table.
	ctx = testCtxWithAuth("GET", "/v1/config/hot", "secret")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode(), "GET must not be claimed by the write table")
}

// TestClusterPeers_DisabledExplainsWhy pins the 503 contract: a
// daemon without a cluster answers /v1/cluster/peers with the
// operator-facing reason instead of a bare 404; an unconfigured
// server keeps the historical 404.
func TestClusterPeers_DisabledExplainsWhy(t *testing.T) {
	t.Parallel()
	s := hotConfigServer(t, nil, nil)
	s.cfg.ClusterDisabledReason = "cluster is disabled: set listen.cluster to enable the gossip cluster"

	ctx := testCtx("GET", "/v1/cluster/peers") // route is auth-exempt
	s.Handler()(ctx)
	require.Equal(t, fasthttp.StatusServiceUnavailable, ctx.Response.StatusCode(), "body: %s", ctx.Response.Body())
	assert.Contains(t, string(ctx.Response.Body()), "set listen.cluster")

	s.cfg.ClusterDisabledReason = ""
	ctx = testCtx("GET", "/v1/cluster/peers")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusNotFound, ctx.Response.StatusCode(), "no reason configured keeps the bare 404")
}

// TestPeerHotSetRoute mounts and dispatches like the other peer
// routes: auth-exempt, one POST, JSON ack; an unmounted route 404s.
func TestPeerHotSetRoute(t *testing.T) {
	t.Parallel()
	var got []byte
	s := New(Config{
		Token: "secret",
		PeerHotSetHandler: func(ctx *fasthttp.RequestCtx) {
			got = append([]byte(nil), ctx.PostBody()...)
			writeJSON(ctx, fasthttp.StatusOK, map[string]string{"status": "set"})
		},
	})

	body := []byte(`{"key":"cluster.ban_ttl","value":"90m","issuer":"node-0","seq":1}`)
	ctx := testCtxWithBody("POST", "/v1/peer/config/hot", body) // no Authorization: peer routes are exempt
	s.Handler()(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode(), "body: %s", ctx.Response.Body())
	assert.Equal(t, body, got)

	s2 := hotConfigServer(t, nil, nil)
	ctx = testCtxWithBody("POST", "/v1/peer/config/hot", body)
	s2.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusNotFound, ctx.Response.StatusCode(), "no handler mounted falls through to 404")
}
