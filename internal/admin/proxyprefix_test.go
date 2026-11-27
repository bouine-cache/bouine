package admin

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

func TestStripProxyPrefix_RoutesIdentically(t *testing.T) {
	t.Parallel()
	token := "tok"
	s := New(Config{
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Token:   token,
		ReadyFn: func() bool { return true },
		PurgeFn: func(api.Key) error { return nil },
	})

	cases := []struct {
		direct string
		proxy  string
		method string
		body   []byte
	}{
		{direct: "/healthz", proxy: "/proxy/healthz", method: "GET"},
		{direct: "/v1/purge", proxy: "/proxy/v1/purge", method: "POST", body: []byte(`{"url":"http://x/"}`)},
		{direct: "/proxyv1/thing", proxy: "/proxyv1/thing", method: "GET"},
	}
	for _, tc := range cases {
		directCtx := testCtxWithBodyAuth(tc.method, tc.direct, tc.body, token)
		proxyCtx := testCtxWithBodyAuth(tc.method, tc.proxy, tc.body, token)
		s.Handler()(directCtx)
		s.Handler()(proxyCtx)
		assert.Equal(t, directCtx.Response.StatusCode(), proxyCtx.Response.StatusCode(),
			"status mismatch %s vs %s", tc.direct, tc.proxy)
		if tc.proxy != tc.direct {
			assert.Equal(t, true, proxyCtx.UserValue(viaProxyKey), "proxy request should be tagged")
			assert.Nil(t, directCtx.UserValue(viaProxyKey), "direct request must not be tagged")
		}
	}
}

func TestStripProxyPrefix_BarePrefix(t *testing.T) {
	t.Parallel()
	h := stripProxyPrefix(func(ctx *fasthttp.RequestCtx) {
		ctx.SetBody([]byte(ctx.Path()))
	})
	ctx := testCtx("GET", "/proxy")
	h(ctx)
	assert.Equal(t, "/", string(ctx.Response.Body()))
	assert.Equal(t, true, ctx.UserValue(viaProxyKey))
}

func TestStripProxyPrefix_AuthStillRequired(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Token:   "tok",
		PurgeFn: func(api.Key) error { return nil },
	})
	ctx := testCtxWithBody("POST", "/proxy/v1/purge", []byte(`{"url":"http://x/"}`))
	s.Handler()(ctx)
	require.Equal(t, fasthttp.StatusUnauthorized, ctx.Response.StatusCode())
}

func TestStripProxyPrefix_BodyLimitStillApplies(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Token:        "tok",
		MaxBodyBytes: 8,
		PurgeFn:      func(api.Key) error { return nil },
	})
	ctx := testCtxWithBodyAuth("POST", "/proxy/v1/purge", []byte(`{"url":"http://x/"}`), "tok")
	s.Handler()(ctx)
	require.Equal(t, fasthttp.StatusRequestEntityTooLarge, ctx.Response.StatusCode())
}
