package bouineapi

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

// cannedKeyHex is the fixture's key hex: a valid 32-char hex with
// deliberately low entropy (a repeating pattern) so the secret
// scanner does not read the fixture as a credential literal.
func cannedKeyHex() string { return strings.Repeat("01", 16) }

func TestClient_KeysFind(t *testing.T) {
	t.Parallel()
	var path string
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		assert.Equal(t, "GET", string(ctx.Method()))
		path = string(ctx.URI().Path()) + "?" + string(ctx.URI().QueryString())
		writeJSON(ctx, KeysFindResult{
			URL:         "http://example.com/p?a b=c",
			KeyHex:      cannedKeyHex(),
			Route:       "pp",
			QueryPolicy: "all",
			Local:       api.KeyState{Present: true, Tier: "hot", SizeBytes: 10, TTLSeconds: 60},
			Variants:    1,
		})
	})

	got, err := New("http://"+srv.Addr).KeysFind(context.Background(), "http://example.com/p?a b=c", false)
	require.NoError(t, err, "KeysFind")
	require.Contains(t, path, "/v1/keys/find?url=http%3A%2F%2Fexample.com%2Fp%3Fa+b%3Dc",
		"the URL must be query-escaped on the wire")
	assert.NotContains(t, path, "peer=1")
	assert.Equal(t, cannedKeyHex(), got.KeyHex)
	assert.True(t, got.Local.Present)
	assert.Equal(t, "hot", got.Local.Tier)
	assert.Equal(t, 1, got.Variants)
}

func TestClient_KeysFindPeer(t *testing.T) {
	t.Parallel()
	var sawPeer string
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		sawPeer = string(ctx.QueryArgs().Peek("peer"))
		writeJSON(ctx, KeysFindResult{URL: "u", KeyHex: "k"})
	})

	_, err := New("http://"+srv.Addr).KeysFind(context.Background(), "u", true)
	require.NoError(t, err)
	assert.Equal(t, "1", sawPeer)
}

func TestClient_KeysTop(t *testing.T) {
	t.Parallel()
	var path string
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		path = string(ctx.URI().Path()) + "?" + string(ctx.URI().QueryString())
		writeJSON(ctx, KeysTopResult{
			Kind:       "top",
			Class:      "miss",
			Limit:      50,
			SampleRate: 100,
			Entries: []KeysTopEntry{{
				Prefix: "/api/users", Route: "api", Requests: 9, Misses: 9,
				LastSeen: "2026-10-25T12:00:00Z",
			}},
		})
	})

	got, err := New("http://"+srv.Addr).KeysTop(context.Background(), 50, "miss")
	require.NoError(t, err, "KeysTop")
	assert.Equal(t, "/v1/keys/top?limit=50&class=miss", path)
	require.Len(t, got.Entries, 1)
	assert.Equal(t, "/api/users", got.Entries[0].Prefix)
	assert.Equal(t, int64(9), got.Entries[0].Requests)
	assert.Equal(t, 100, got.SampleRate)
}

func TestClient_KeysRecent(t *testing.T) {
	t.Parallel()
	var path string
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		path = string(ctx.URI().Path()) + "?" + string(ctx.URI().QueryString())
		writeJSON(ctx, KeysTopResult{Kind: "recent", Limit: 5, Entries: []KeysTopEntry{}})
	})

	got, err := New("http://"+srv.Addr).KeysRecent(context.Background(), 5, "")
	require.NoError(t, err, "KeysRecent")
	assert.Equal(t, "/v1/keys/recent?limit=5", path)
	assert.Equal(t, "recent", got.Kind)
	assert.Empty(t, got.Entries)
}
