package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

func TestPeerPurgeHandler_DecodesAndCalls(t *testing.T) {
	t.Parallel()
	var received api.PurgeEvent
	handler := NewPeerPurgeHandler(func(evt api.PurgeEvent) error {
		received = evt
		return nil
	})

	evt := api.PurgeEvent{Key: testkey.Key(42), Issuer: "node-0"}
	body, _ := EncodePurgeHTTP(evt)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/v1/peer/purge")
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetBody(body)
	handler(ctx)

	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
	assert.Equal(t, testkey.Key(42), received.Key)
}

func TestPeerPurgeHandler_BadBody(t *testing.T) {
	t.Parallel()
	handler := NewPeerPurgeHandler(func(api.PurgeEvent) error {
		t.Fatal("should not be called")
		return nil
	})

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/v1/peer/purge")
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetBody([]byte("bad"))
	handler(ctx)

	require.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
}

func TestPeerBanHandler_DecodesAndCalls(t *testing.T) {
	t.Parallel()
	var received api.BanEvent
	handler := NewPeerBanHandler(func(evt api.BanEvent) error {
		received = evt
		return nil
	})

	evt := api.BanEvent{Issuer: "node-0", Predicate: api.BanExpr{HostRegex: "example.com"}}
	body, _ := EncodeBanHTTP(evt)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/v1/peer/ban")
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetBody(body)
	handler(ctx)

	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
	assert.Equal(t, "example.com", received.Predicate.HostRegex)
}

func TestPeerRefreshHandler_DecodesAndCalls(t *testing.T) {
	t.Parallel()
	var received api.RefreshEvent
	handler := NewPeerRefreshHandler(func(evt api.RefreshEvent) error {
		received = evt
		return nil
	})

	evt := api.RefreshEvent{Key: testkey.Key(42), Issuer: "node-0"}
	body, _ := EncodeRefreshHTTP(evt)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/v1/peer/refresh")
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetBody(body)
	handler(ctx)

	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
	assert.Equal(t, testkey.Key(42), received.Key)
	assert.Equal(t, "node-0", received.Issuer)
}

func TestPeerRefreshHandler_BadBody(t *testing.T) {
	t.Parallel()
	handler := NewPeerRefreshHandler(func(api.RefreshEvent) error {
		t.Fatal("should not be called")
		return nil
	})

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/v1/peer/refresh")
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetBody([]byte("bad"))
	handler(ctx)

	require.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
}
