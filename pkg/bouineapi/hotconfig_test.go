package bouineapi

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
)

func TestClient_HotConfig(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		assert.Equal(t, "GET", string(ctx.Method()))
		writeJSON(ctx, []HotKnob{
			{
				Key: "cluster.ban_ttl", Value: "24h0m0s", Source: "default",
				FileValue: "24h0m0s", Default: "24h0m0s",
				Description: "ban lifetime",
			},
			{
				Key: "url_ring_sample_rate", Value: "100", Source: "runtime",
				FileValue: "0", Default: "0",
				Description: "URL ring sampling",
			},
		})
	})

	got, err := New("http://" + srv.Addr).HotConfig(context.Background())
	require.NoError(t, err, "HotConfig")
	require.Len(t, got, 2)
	assert.Equal(t, "cluster.ban_ttl", got[0].Key)
	assert.Equal(t, "24h0m0s", got[0].FileValue)
	assert.Equal(t, "default", got[0].Source)
	assert.Equal(t, "runtime", got[1].Source)
	assert.Equal(t, "URL ring sampling", got[1].Description)
}

func TestClient_HotSet(t *testing.T) {
	t.Parallel()
	var body map[string]any
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		assert.Equal(t, "POST", string(ctx.Method()))
		require.NoError(t, json.Unmarshal(ctx.Request.Body(), &body))
		writeJSON(ctx, HotSetResult{
			Key: "cluster.ban_ttl", OldValue: "24h0m0s", NewValue: "1h30m0s",
			Applied: true, Warning: "not persisted; add to your YAML before the next deploy",
		})
	})

	res, err := New("http://"+srv.Addr).HotSet(context.Background(), "cluster.ban_ttl", "90m", false)
	require.NoError(t, err, "HotSet")
	require.NotNil(t, res)
	assert.True(t, res.Applied)
	assert.Equal(t, "1h30m0s", res.NewValue)
	assert.Equal(t, "not persisted; add to your YAML before the next deploy", res.Warning)
	assert.Equal(t, "cluster.ban_ttl", body["key"])
	assert.Equal(t, "90m", body["value"])
	assert.Equal(t, false, body["dry_run"], "dry_run must be omitted-or-false on real sets")
}

// TestClient_HotSetDryRun pins the wire contract: the dry-run flag
// travels in the request body and the unapplied result decodes.
func TestClient_HotSetDryRun(t *testing.T) {
	t.Parallel()
	var body map[string]any
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		require.NoError(t, json.Unmarshal(ctx.Request.Body(), &body))
		writeJSON(ctx, HotSetResult{
			Key: "url_ring_sample_rate", OldValue: "0", NewValue: "5", Applied: false,
		})
	})

	res, err := New("http://"+srv.Addr).HotSet(context.Background(), "url_ring_sample_rate", "5", true)
	require.NoError(t, err, "HotSet dry-run")
	assert.False(t, res.Applied)
	assert.Equal(t, true, body["dry_run"])
}
