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

func TestClient_ConfigReload(t *testing.T) {
	t.Parallel()
	var body map[string]any
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		assert.Equal(t, "POST", string(ctx.Method()))
		assert.Equal(t, "/v1/config/reload", string(ctx.Path()))
		require.NoError(t, json.Unmarshal(ctx.Request.Body(), &body))
		writeJSON(ctx, ConfigReloadResult{
			Path:         "/etc/bouine/config.yaml",
			Applied:      []ConfigReloadChange{{Key: "cluster.ban_ttl", Old: "24h0m0s", New: "1h30m0s"}},
			Unchanged:    []string{"url_ring_sample_rate"},
			NeedsRestart: []string{"listen.http: 127.0.0.1:1 -> 127.0.0.1:2"},
		})
	})

	res, err := New("http://"+srv.Addr).ConfigReload(context.Background(), "/etc/bouine/config.yaml")
	require.NoError(t, err, "ConfigReload")
	assert.Equal(t, "/etc/bouine/config.yaml", res.Path)
	require.Len(t, res.Applied, 1)
	assert.Equal(t, "1h30m0s", res.Applied[0].New)
	assert.Equal(t, []string{"url_ring_sample_rate"}, res.Unchanged)
	require.Len(t, res.NeedsRestart, 1)
	assert.Equal(t, "/etc/bouine/config.yaml", body["path"])
}

func TestClient_ConfigReloadEmptyPathOmitted(t *testing.T) {
	t.Parallel()
	var raw map[string]json.RawMessage
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		require.NoError(t, json.Unmarshal(ctx.Request.Body(), &raw))
		writeJSON(ctx, ConfigReloadResult{Path: "/default.yaml"})
	})

	res, err := New("http://"+srv.Addr).ConfigReload(context.Background(), "")
	require.NoError(t, err)
	assert.Equal(t, "/default.yaml", res.Path)
	_, hasPath := raw["path"]
	assert.False(t, hasPath, "empty path must be omitted from the body")
}
