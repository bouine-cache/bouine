package cluster

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/hashicorp/memberlist"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

func TestRing_Segments(t *testing.T) {
	t.Parallel()
	r := newRing(256)
	r.add("alpha", 256)
	r.add("beta", 256)
	segs := r.segments()
	require.Len(t, segs, 2)
	var total float64
	for _, s := range segs {
		total += s.Frac
	}
	assert.InDelta(t, 1.0, total, 0.01)
}

func TestRing_Segments_Empty(t *testing.T) {
	t.Parallel()
	r := newRing(256)
	assert.Nil(t, r.segments())
}

func TestCluster_RingSegments(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "ring-test", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	segs := c.RingSegments()
	require.Len(t, segs, 1)
	assert.Equal(t, "ring-test", segs[0].NodeName)
}

func TestCluster_Config(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "cfg-test", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	got := c.Config()
	assert.Equal(t, "cfg-test", got.NodeName)
	assert.Equal(t, "strong", got.Mode)
}

func TestCluster_Mode(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "mode-test", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	assert.Equal(t, "strong", c.Mode())
}

func TestMetrics_SetMode(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	m.SetMode("strong")
	m.SetMode("eventual")
	m.SetMode("unknown")
}

func TestMetrics_SetMode_Nil(t *testing.T) {
	t.Parallel()
	var m *Metrics
	m.SetMode("strong")
}

func TestMetrics_BroadcastFailuresCount(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	require.Equal(t, int64(0), m.BroadcastFailuresCount())
	m.IncBroadcastFailure("purge", "dial")
	m.IncBroadcastFailure("ban", "timeout")
	assert.Equal(t, int64(2), m.BroadcastFailuresCount())
}

func TestMetrics_BroadcastFailuresCount_Nil(t *testing.T) {
	t.Parallel()
	var m *Metrics
	assert.Equal(t, int64(0), m.BroadcastFailuresCount())
}

func TestMetrics_IncGossipInvalidation_Nil(t *testing.T) {
	t.Parallel()
	var m *Metrics
	m.IncGossipInvalidation("purge")
}

func TestMetrics_IncHTTPInvalidation_Nil(t *testing.T) {
	t.Parallel()
	var m *Metrics
	m.IncHTTPInvalidation("ban")
}

func TestMetrics_IncBroadcastFailure_Nil(t *testing.T) {
	t.Parallel()
	var m *Metrics
	m.IncBroadcastFailure("purge", "dial")
}

func TestPeerPurgeHandler_FnError(t *testing.T) {
	t.Parallel()
	handler := NewPeerPurgeHandler(func(api.PurgeEvent) error {
		return errors.New("internal failure")
	})

	evt := api.PurgeEvent{Key: testkey.Key(1), Issuer: "node-0"}
	body, _ := EncodePurgeHTTP(evt)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/v1/peer/purge")
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetBody(body)
	handler(ctx)

	require.Equal(t, fasthttp.StatusInternalServerError, ctx.Response.StatusCode())
}

func TestPeerPurgeHandler_ReadError(t *testing.T) {
	t.Parallel()
	handler := NewPeerPurgeHandler(func(api.PurgeEvent) error {
		return nil
	})

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/v1/peer/purge")
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetBody([]byte("bad"))
	handler(ctx)

	require.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
}

func TestPeerBanHandler_BadBody(t *testing.T) {
	t.Parallel()
	handler := NewPeerBanHandler(func(api.BanEvent) error {
		return nil
	})

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/v1/peer/ban")
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetBody([]byte("bad"))
	handler(ctx)

	require.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
}

func TestPeerBanHandler_FnError(t *testing.T) {
	t.Parallel()
	handler := NewPeerBanHandler(func(api.BanEvent) error {
		return errors.New("ban failed")
	})

	evt := api.BanEvent{Issuer: "node-0"}
	body, _ := EncodeBanHTTP(evt)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/v1/peer/ban")
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetBody(body)
	handler(ctx)

	require.Equal(t, fasthttp.StatusInternalServerError, ctx.Response.StatusCode())
}

func TestPeerBanHandler_ReadError(t *testing.T) {
	t.Parallel()
	handler := NewPeerBanHandler(func(api.BanEvent) error {
		return nil
	})

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/v1/peer/ban")
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetBody([]byte("bad"))
	handler(ctx)

	require.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
}

func TestPeerRefreshHandler_FnError(t *testing.T) {
	t.Parallel()
	handler := NewPeerRefreshHandler(func(api.RefreshEvent) error {
		return errors.New("refresh failed")
	})

	evt := api.RefreshEvent{Key: testkey.Key(1), Issuer: "node-0"}
	body, _ := EncodeRefreshHTTP(evt)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/v1/peer/refresh")
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetBody(body)
	handler(ctx)

	require.Equal(t, fasthttp.StatusInternalServerError, ctx.Response.StatusCode())
}

func TestPeerRefreshHandler_ReadError(t *testing.T) {
	t.Parallel()
	handler := NewPeerRefreshHandler(func(api.RefreshEvent) error {
		return nil
	})

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/v1/peer/refresh")
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetBody([]byte("bad"))
	handler(ctx)

	require.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
}

func TestNotifyUpdate_DelegatesToNotifyJoin(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "notify-test", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	info := api.PeerInfo{Name: "new-node", Addr: "127.0.0.1:1234"}
	meta, _ := json.Marshal(info)

	node := &memberlist.Node{Name: "new-node", Meta: meta}
	c.NotifyUpdate(node)

	members := c.Members()
	found := false
	for _, m := range members {
		if m.Name == "new-node" {
			found = true
			break
		}
	}
	assert.True(t, found, "NotifyUpdate should add the peer")
}

func TestNodeMeta_RoundTrip(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig(t, "meta-test", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	meta := c.NodeMeta(512)
	require.NotEmpty(t, meta)

	var info api.PeerInfo
	require.NoError(t, json.Unmarshal(meta, &info))
	assert.Equal(t, "meta-test", info.Name)
}
