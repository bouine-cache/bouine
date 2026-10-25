package admin

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

func keysServer(t *testing.T, cfg Config) *Server {
	t.Helper()
	cfg.Token = "secret"
	return New(cfg)
}

func TestKeysFindEndpoint_ServesResult(t *testing.T) {
	t.Parallel()
	var gotURL string
	var gotPeer bool
	var ops []string
	s := keysServer(t, Config{
		KeysFindFn: func(_ context.Context, rawURL string, peer bool) (KeysFindResult, error) {
			gotURL, gotPeer = rawURL, peer
			return KeysFindResult{
				URL:         rawURL,
				KeyHex:      "abc",
				Route:       "pp",
				Host:        "example.com",
				Path:        "/pages/p",
				QueryPolicy: "all",
				Local:       api.KeyState{Present: true, Tier: "hot", SizeBytes: 128, TTLSeconds: 60},
				Variants:    2,
			}, nil
		},
		OpsLogFn: func(op, arg, result string) {
			ops = append(ops, op+" "+arg+" "+result)
		},
	})

	status, body := authedGet(t, s, "/v1/keys/find?url=http%3A%2F%2Fexample.com%2Fpages%2Fp%3Fx%3D1")
	require.Equal(t, fasthttp.StatusOK, status, "body: %s", body)
	require.Equal(t, "http://example.com/pages/p?x=1", gotURL)
	require.False(t, gotPeer)

	var got KeysFindResult
	require.NoError(t, json.Unmarshal(body, &got))
	assert.Equal(t, "abc", got.KeyHex)
	assert.Equal(t, "pp", got.Route)
	assert.True(t, got.Local.Present)
	assert.Equal(t, "hot", got.Local.Tier)
	assert.Equal(t, 2, got.Variants)
	require.Len(t, ops, 1)
	assert.Equal(t, "keys find http://example.com/pages/p?x=1 found", ops[0])
}

func TestKeysFindEndpoint_Parameters(t *testing.T) {
	t.Parallel()
	var gotPeer bool
	find := Config{KeysFindFn: func(_ context.Context, _ string, peer bool) (KeysFindResult, error) {
		gotPeer = peer
		return KeysFindResult{Local: api.KeyState{}}, nil
	}}

	// peer=1 and peer=true both ask for the owner query.
	for _, q := range []string{"?url=u&peer=1", "?url=u&peer=true"} {
		s := keysServer(t, find)
		status, body := authedGet(t, s, "/v1/keys/find"+q)
		require.Equal(t, fasthttp.StatusOK, status, "body: %s", body)
		assert.True(t, gotPeer, q)
		gotPeer = false
	}
	// Anything else (including a bare peer param) does not.
	for _, q := range []string{"?url=u", "?url=u&peer", "?url=u&peer=0"} {
		s := keysServer(t, find)
		status, _ := authedGet(t, s, "/v1/keys/find"+q)
		require.Equal(t, fasthttp.StatusOK, status)
		assert.False(t, gotPeer, q)
	}

	// Missing url and provider errors are 400s.
	s := keysServer(t, find)
	status, body := authedGet(t, s, "/v1/keys/find")
	require.Equal(t, fasthttp.StatusBadRequest, status, "body: %s", body)

	errServer := keysServer(t, Config{KeysFindFn: func(_ context.Context, _ string, _ bool) (KeysFindResult, error) {
		return KeysFindResult{}, assert.AnError
	}})
	status, body = authedGet(t, errServer, "/v1/keys/find?url=nope")
	require.Equal(t, fasthttp.StatusBadRequest, status, "body: %s", body)

	// An absent result still answers 200 and records the absent op.
	var ops []string
	absent := keysServer(t, Config{
		KeysFindFn: func(_ context.Context, _ string, _ bool) (KeysFindResult, error) {
			return KeysFindResult{AbsentReason: "no_entry"}, nil
		},
		OpsLogFn: func(op, arg, result string) {
			ops = append(ops, op+" "+arg+" "+result)
		},
	})
	status, body = authedGet(t, absent, "/v1/keys/find?url=u")
	require.Equal(t, fasthttp.StatusOK, status, "body: %s", body)
	require.Len(t, ops, 1)
	assert.Equal(t, "keys find u absent", ops[0])
}

func TestKeysFindEndpoint_RequiresAuth(t *testing.T) {
	t.Parallel()
	s := keysServer(t, Config{KeysFindFn: func(_ context.Context, _ string, _ bool) (KeysFindResult, error) {
		return KeysFindResult{}, nil
	}})
	ctx := testCtx("GET", "/v1/keys/find?url=u")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusUnauthorized, ctx.Response.StatusCode())
}

func TestKeysTopEndpoint_Parameters(t *testing.T) {
	t.Parallel()
	var gotKind, gotClass string
	var gotLimit int
	top := Config{KeysTopFn: func(kind, class string, limit int) KeysTopResult {
		gotKind, gotClass, gotLimit = kind, class, limit
		return KeysTopResult{Kind: kind, Class: class, Limit: limit, SampleRate: 100,
			Entries: []KeysTopEntry{{Prefix: "/a/one", Requests: 3, Misses: 3}}}
	}}

	// Defaults: limit 20, no class.
	s := keysServer(t, top)
	status, body := authedGet(t, s, "/v1/keys/top")
	require.Equal(t, fasthttp.StatusOK, status, "body: %s", body)
	assert.Equal(t, "top", gotKind)
	assert.Equal(t, 20, gotLimit)
	assert.Equal(t, "", gotClass)

	// /v1/keys/recent routes the same provider with kind=recent.
	s = keysServer(t, top)
	status, body = authedGet(t, s, "/v1/keys/recent?limit=5&class=miss")
	require.Equal(t, fasthttp.StatusOK, status, "body: %s", body)
	assert.Equal(t, "recent", gotKind)
	assert.Equal(t, 5, gotLimit)
	assert.Equal(t, "miss", gotClass)

	var got KeysTopResult
	require.NoError(t, json.Unmarshal(body, &got))
	assert.Equal(t, "recent", got.Kind)
	assert.Equal(t, 100, got.SampleRate)
	require.Len(t, got.Entries, 1)
	assert.Equal(t, "/a/one", got.Entries[0].Prefix)

	// Out-of-range and non-numeric limits are 400s.
	for _, q := range []string{"?limit=0", "?limit=-1", "?limit=513", "?limit=abc"} {
		s = keysServer(t, top)
		status, body = authedGet(t, s, "/v1/keys/top"+q)
		assert.Equal(t, fasthttp.StatusBadRequest, status, "query %q body %s", q, body)
	}

	// Unknown classes are 400s; "hit" is not a recorded class (the
	// ring only sees non-HIT traffic).
	for _, class := range []string{"hit", "everything"} {
		s = keysServer(t, top)
		status, body = authedGet(t, s, "/v1/keys/top?class="+class)
		assert.Equal(t, fasthttp.StatusBadRequest, status, "body: %s", body)
	}

	// Unwired provider: the route falls through to 404.
	s = keysServer(t, Config{})
	status, _ = authedGet(t, s, "/v1/keys/top")
	assert.Equal(t, fasthttp.StatusNotFound, status)
}

func TestPeerKeysFindRoute(t *testing.T) {
	t.Parallel()
	// peerRouteKeyHex is a valid 32-char key hex with deliberately
	// low entropy (a repeating pattern) so the secret scanner does not
	// read the fixture as a credential literal.
	keyHex := strings.Repeat("01", 16)
	s := keysServer(t, Config{
		PeerKeysFindHandler: NewTestPeerKeysHandler(),
	})
	// Peer routes are token-exempt: no Authorization header.
	ctx := testCtx("GET", "/v1/peer/keys/find?key="+keyHex)
	s.Handler()(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode(), "body: %s", ctx.Response.Body())
	var st api.KeyState
	require.NoError(t, json.Unmarshal(ctx.Response.Body(), &st))
	assert.True(t, st.Present)
	assert.Equal(t, "hot", st.Tier)

	// Unwired: the route falls through (404), never a 500.
	bare := keysServer(t, Config{})
	ctx = testCtx("GET", "/v1/peer/keys/find?key="+keyHex)
	bare.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusNotFound, ctx.Response.StatusCode())
}

// NewTestPeerKeysHandler is a canned peer keys handler for route
// tests; the cluster package's keysquery_test covers the real one.
func NewTestPeerKeysHandler() fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		key := string(ctx.QueryArgs().Peek("key"))
		if key == "" {
			ctx.Error("missing key", fasthttp.StatusBadRequest)
			return
		}
		ctx.Response.Header.SetContentType("application/json")
		_ = json.NewEncoder(ctx).Encode(api.KeyState{Present: true, Tier: "hot"})
	}
}
