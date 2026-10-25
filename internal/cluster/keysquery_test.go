package cluster

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

func TestPeerKeysFindHandler(t *testing.T) {
	t.Parallel()
	key := testkey.Key(11)
	handler := NewPeerKeysFindHandler(func(k api.Key) (api.KeyState, bool) {
		if k == key {
			return api.KeyState{
				Present:      true,
				Tier:         "hot",
				SizeBytes:    128,
				TTLSeconds:   60,
				StatusCode:   200,
				WindowHits:   3,
				Banned:       false,
				BackedToWarm: true,
			}, true
		}
		return api.KeyState{}, false
	})

	srv := fasthttptest.NewServer(t, handler)
	defer srv.Close()

	client := &fasthttp.Client{}
	do := func(query string) (int, api.KeyState) {
		t.Helper()
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)
		req.SetRequestURI("http://" + srv.Addr + PeerKeysFindPath + "?" + query)
		require.NoError(t, client.Do(req, resp))
		var st api.KeyState
		if resp.StatusCode() == fasthttp.StatusOK {
			require.NoError(t, json.Unmarshal(resp.Body(), &st))
		}
		return resp.StatusCode(), st
	}

	code, st := do("key=" + key.Hex())
	require.Equal(t, fasthttp.StatusOK, code)
	assert.True(t, st.Present)
	assert.Equal(t, "hot", st.Tier)
	assert.Equal(t, int64(128), st.SizeBytes)
	assert.Equal(t, int64(60), st.TTLSeconds)
	assert.True(t, st.BackedToWarm)

	// Absence is data, not an error.
	code, st = do("key=" + testkey.Key(12).Hex())
	require.Equal(t, fasthttp.StatusOK, code)
	assert.False(t, st.Present)

	// Missing and malformed key parameters are 400s.
	code, _ = do("")
	assert.Equal(t, fasthttp.StatusBadRequest, code)
	code, _ = do("key=deadbeef")
	assert.Equal(t, fasthttp.StatusBadRequest, code)
}

func TestPeerFetcher_QueryKeyState(t *testing.T) {
	t.Parallel()
	key := testkey.Key(21)
	want := api.KeyState{
		Present:    true,
		Tier:       "warm",
		SizeBytes:  4096,
		TTLSeconds: 300,
		Protected:  true,
	}

	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		require.Equal(t, "GET", string(ctx.Method()))
		require.Equal(t, key.Hex(), string(ctx.QueryArgs().Peek("key")))
		require.Equal(t, ClusterProtocolVersion, string(ctx.Request.Header.Peek(ClusterVersionHeader)))
		ctx.Response.Header.SetContentType("application/json")
		_ = json.NewEncoder(ctx).Encode(want)
	})
	defer srv.Close()

	f := NewPeerFetcherWithConfig(PeerFetcherConfig{MaxIdleConnDuration: 100 * time.Millisecond}, nil, nil)
	defer func() { _ = f.Close(context.Background()) }()

	got, err := f.QueryKeyState(context.Background(), api.PeerInfo{AdminAddr: srv.Addr}, key)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestPeerFetcher_QueryKeyState_RejectsMalformed(t *testing.T) {
	t.Parallel()
	key := testkey.Key(22)

	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		_, _ = ctx.WriteString(`{"present":true,"tier":"hot"} trailing-garbage`)
	})
	defer srv.Close()

	f := NewPeerFetcherWithConfig(PeerFetcherConfig{MaxIdleConnDuration: 100 * time.Millisecond}, nil, nil)
	defer func() { _ = f.Close(context.Background()) }()

	_, err := f.QueryKeyState(context.Background(), api.PeerInfo{AdminAddr: srv.Addr}, key)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "trailing data")

	// Unknown fields are rejected, matching the hot-set frames'
	// strictness: a newer sender cannot smuggle fields an older
	// receiver would silently ignore.
	strict := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		_, _ = ctx.WriteString(`{"present":true,"future_field":1}`)
	})
	defer strict.Close()
	_, err = f.QueryKeyState(context.Background(), api.PeerInfo{AdminAddr: strict.Addr}, key)
	require.Error(t, err)

	// Non-200 answers surface as errors.
	badStatus := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
	})
	defer badStatus.Close()
	_, err = f.QueryKeyState(context.Background(), api.PeerInfo{AdminAddr: badStatus.Addr}, key)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 500")

	// Transport failures surface as errors (nothing listens here).
	_, err = f.QueryKeyState(context.Background(), api.PeerInfo{AdminAddr: "127.0.0.1:1"}, key)
	require.Error(t, err)
}
