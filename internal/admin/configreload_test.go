package admin

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

func reloadServer(t *testing.T, fn func(context.Context, ConfigReloadRequest) (ConfigReloadResult, error)) *Server {
	t.Helper()
	cfg := Config{Token: "secret", ReloadFn: fn}
	return New(cfg)
}

func TestConfigReload_EndpointMatrix(t *testing.T) {
	t.Parallel()
	s := reloadServer(t, func(_ context.Context, req ConfigReloadRequest) (ConfigReloadResult, error) {
		return ConfigReloadResult{
			Path:         req.Path,
			Applied:      []ConfigReloadChange{{Key: "cluster.ban_ttl", Old: "24h0m0s", New: "1h30m0s"}},
			Unchanged:    []string{"url_ring_sample_rate"},
			NeedsRestart: []string{"listen.http: 127.0.0.1:1 -> 127.0.0.1:2"},
		}, nil
	})

	ctx := testCtxWithBodyAuth("POST", "/v1/config/reload", []byte(`{"path":"/etc/bouine/config.yaml"}`), "secret")
	s.Handler()(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode(), "body: %s", ctx.Response.Body())

	var got ConfigReloadResult
	require.NoError(t, json.Unmarshal(ctx.Response.Body(), &got))
	assert.Equal(t, "/etc/bouine/config.yaml", got.Path)
	require.Len(t, got.Applied, 1)
	assert.Equal(t, "1h30m0s", got.Applied[0].New)
	assert.Equal(t, []string{"url_ring_sample_rate"}, got.Unchanged)
	require.Len(t, got.NeedsRestart, 1)
}

func TestConfigReload_DisabledAnswers503(t *testing.T) {
	t.Parallel()
	s := reloadServer(t, func(context.Context, ConfigReloadRequest) (ConfigReloadResult, error) {
		return ConfigReloadResult{}, ErrReloadDisabled
	})
	ctx := testCtxWithBodyAuth("POST", "/v1/config/reload", []byte(`{}`), "secret")
	s.Handler()(ctx)
	require.Equal(t, fasthttp.StatusServiceUnavailable, ctx.Response.StatusCode(), "body: %s", ctx.Response.Body())
	assert.Contains(t, string(ctx.Response.Body()), "experimental.config_reload")
}

func TestConfigReload_RejectionAnswers400(t *testing.T) {
	t.Parallel()
	s := reloadServer(t, func(context.Context, ConfigReloadRequest) (ConfigReloadResult, error) {
		return ConfigReloadResult{}, errors.New("reload /etc/bouine/config.yaml: yaml decode: boom")
	})
	ctx := testCtxWithBodyAuth("POST", "/v1/config/reload", []byte(`{}`), "secret")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
	assert.Contains(t, string(ctx.Response.Body()), "yaml decode")
}

func TestConfigReload_Guards(t *testing.T) {
	t.Parallel()
	s := reloadServer(t, func(context.Context, ConfigReloadRequest) (ConfigReloadResult, error) {
		return ConfigReloadResult{}, nil
	})

	// Unauthenticated (the reload route is NOT peer-exempt).
	ctx := testCtxWithBody("POST", "/v1/config/reload", []byte(`{}`))
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusUnauthorized, ctx.Response.StatusCode())

	// Wrong method.
	ctx = testCtxWithAuth("GET", "/v1/config/reload", "secret")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusMethodNotAllowed, ctx.Response.StatusCode())

	// Malformed body.
	ctx = testCtxWithBodyAuth("POST", "/v1/config/reload", []byte("nope"), "secret")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())

	// Unknown members are rejected, matching the hot-set endpoint.
	ctx = testCtxWithBodyAuth("POST", "/v1/config/reload", []byte(`{"oops":1}`), "secret")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())

	// Unwired (experimental flag off): falls through to 404.
	s2 := hotConfigServer(t, nil, nil)
	ctx = testCtxWithBodyAuth("POST", "/v1/config/reload", []byte(`{}`), "secret")
	s2.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusNotFound, ctx.Response.StatusCode())
}
