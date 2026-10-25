package cluster

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

func TestHotSetCodecRoundTrip(t *testing.T) {
	t.Parallel()
	evt := api.HotSetEvent{
		Key:   "pool:api.health.passive.consecutive_5xx",
		Value: "5", Issuer: "node-0", Seq: 7,
	}
	body, err := EncodeHotSetHTTP(evt)
	require.NoError(t, err)
	got, err := DecodeHotSetHTTP(body)
	require.NoError(t, err)
	assert.Equal(t, evt, got)
}

func TestHotSetCodecRejectsMalformedFrames(t *testing.T) {
	t.Parallel()
	_, err := DecodeHotSetHTTP([]byte(`{"key":"k","value":"v","unexpected":1}`))
	require.Error(t, err, "unknown fields must be rejected")

	_, err = DecodeHotSetHTTP([]byte(`{"key":"k","value":"v"} {"key":"k2"}`))
	require.Error(t, err, "trailing data must be rejected")

	_, err = DecodeHotSetHTTP([]byte(`not json`))
	require.Error(t, err)

	_, err = EncodeHotSetHTTP(api.HotSetEvent{Value: strings.Repeat("x", maxStringLen+1)})
	require.Error(t, err, "oversize values must be rejected before the wire")
}

func TestPeerHotSetHandler(t *testing.T) {
	t.Parallel()
	var got api.HotSetEvent
	h := NewPeerHotSetHandler(func(evt api.HotSetEvent) error {
		got = evt
		return nil
	})

	body, err := EncodeHotSetHTTP(api.HotSetEvent{Key: "cluster.ban_ttl", Value: "90m", Issuer: "node-0", Seq: 1})
	require.NoError(t, err)
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodPost)
	ctx.Request.SetRequestURI("http://test/v1/peer/config/hot")
	ctx.Request.SetBodyRaw(body)
	h(ctx)

	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode(), "body: %s", ctx.Response.Body())
	assert.Equal(t, "cluster.ban_ttl", got.Key)
	assert.Equal(t, "90m", got.Value)
}

func TestPeerHotSetHandlerRejections(t *testing.T) {
	t.Parallel()
	h := NewPeerHotSetHandler(func(api.HotSetEvent) error { return nil })

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodPost)
	ctx.Request.SetRequestURI("http://test/v1/peer/config/hot")
	ctx.Request.SetBodyRaw([]byte("garbage"))
	h(ctx)
	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())

	hErr := NewPeerHotSetHandler(func(api.HotSetEvent) error { return assert.AnError })
	ctx = &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodPost)
	ctx.Request.SetRequestURI("http://test/v1/peer/config/hot")
	ctx.Request.SetBodyRaw([]byte(`{"key":"k","value":"v"}`))
	hErr(ctx)
	assert.Equal(t, fasthttp.StatusInternalServerError, ctx.Response.StatusCode())
}

// TestBroadcaster_BroadcastHotSet pins the fan-out: every live peer
// receives the set on /v1/peer/config/hot, self is skipped, and the
// return value counts acknowledgements only.
func TestBroadcaster_BroadcastHotSet(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	received := make(map[string]api.HotSetEvent)
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		if string(ctx.Path()) != "/v1/peer/config/hot" {
			t.Errorf("unexpected path: %s", string(ctx.Path()))
		}
		evt, err := DecodeHotSetHTTP(ctx.PostBody())
		if err != nil {
			t.Errorf("decode: %v", err)
			return
		}
		mu.Lock()
		received[evt.Issuer+evt.Key] = evt
		mu.Unlock()
		ctx.SetStatusCode(fasthttp.StatusOK)
	})

	c := minimalCluster(t, "node-0")
	c.peers["node-1"] = &Member{Info: api.PeerInfo{Name: "node-1", AdminAddr: srv.Addr}}
	c.peers["node-0"] = &Member{Info: api.PeerInfo{Name: "node-0", AdminAddr: srv.Addr}} // self must be skipped

	b := NewBroadcaster(c, nil)
	acknowledged := b.BroadcastHotSet(context.Background(), "cluster.ban_ttl", "90m")
	b.Close()

	assert.Equal(t, 1, acknowledged, "one peer acknowledged, self not counted")
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, received, 1)
	evt := received["node-0cluster.ban_ttl"]
	assert.Equal(t, "90m", evt.Value)
	assert.Equal(t, "node-0", evt.Issuer)
	assert.NotZero(t, evt.Seq)
}

// TestBroadcaster_BroadcastHotSetCountsFailures verifies the honest
// coverage report: a peer that fails does not count towards the
// acknowledged total.
func TestBroadcaster_BroadcastHotSetCountsFailures(t *testing.T) {
	t.Parallel()
	ok := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(fasthttp.StatusOK)
	})
	dead := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
	})
	dead.Close() // every post now fails at the transport layer

	c := minimalCluster(t, "node-0")
	c.peers["node-1"] = &Member{Info: api.PeerInfo{Name: "node-1", AdminAddr: ok.Addr}}
	c.peers["node-2"] = &Member{Info: api.PeerInfo{Name: "node-2", AdminAddr: dead.Addr}}

	b := NewBroadcaster(c, nil)
	acknowledged := b.BroadcastHotSet(context.Background(), "cluster.ban_ttl", "1h")
	b.Close()

	assert.Equal(t, 1, acknowledged, "only the healthy peer counts")
}

// TestBroadcaster_BroadcastHotSetEventualNoFanout pins that
// eventual-mode clusters do not fan out (runtime knobs ride no
// gossip; the issuing node alone carries the override).
func TestBroadcaster_BroadcastHotSetEventualNoFanout(t *testing.T) {
	t.Parallel()
	called := false
	srv := fasthttptest.NewServer(t, func(*fasthttp.RequestCtx) { called = true })

	c := minimalCluster(t, "node-0")
	c.cfg.Mode = "eventual"
	c.peers["node-1"] = &Member{Info: api.PeerInfo{Name: "node-1", AdminAddr: srv.Addr}}

	b := NewBroadcaster(c, nil)
	acknowledged := b.BroadcastHotSet(context.Background(), "cluster.ban_ttl", "1h")
	b.Close()

	assert.Equal(t, 0, acknowledged)
	assert.False(t, called, "eventual mode must not post to peer admin APIs")
}
