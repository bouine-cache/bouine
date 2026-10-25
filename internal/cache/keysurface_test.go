package cache

import (
	"context"
	"log/slog"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// TestBuildKeyFromURL_MatchesFastPath pins the property the admin
// keys surface depends on (issue #816): BuildKeyFromURL — used by
// bouine keys find, purge, refresh, and cachecheck — must compute
// the exact key the data plane's BuildKeyFast computes for the same
// request, for every key-policy shape. A divergence here means the
// admin plane inspects keys that were never stored.
func TestBuildKeyFromURL_MatchesFastPath(t *testing.T) {
	t.Parallel()
	policies := []*KeyPolicy{
		nil,
		NewKeyPolicy(nil, nil, nil, nil, false, false, nil, true),                                  // excludeHost
		NewKeyPolicy(map[string]bool{"utm_source": true}, nil, nil, nil, false, false, nil, false), // strip
		NewKeyPolicy(nil, map[string]bool{"v": true}, nil, nil, false, false, nil, false),          // allow
		NewKeyPolicy(nil, nil, nil, []string{"/pfx"}, true, true, nil, false),                      // prefixes + stripEmpty + dedup
	}
	urls := []string{
		"http://example.com/",
		"http://example.com:80/a/b",
		"https://example.com:443/a/b?b=2&a=1",
		"https://Example.com:8443//dup//slash?x=1&x=2&z=",
		"http://example.com/path?utm_source=mail&v=3&id=9",
		"http://example.com/pfx/kept?x=",
	}
	for _, policy := range policies {
		for _, raw := range urls {
			fromURL := BuildKeyFromURL(raw, policy)
			require.False(t, fromURL.IsZero(), "key must not be zero: url=%s", raw)
			u, err := url.Parse(raw)
			require.NoError(t, err)
			fast := BuildKeyFast([]byte("GET"), []byte(raw), []byte(u.Host), []byte(u.Path), u.Scheme == "https", policy)
			require.Equal(t, fast, fromURL, "GET key mismatch between URL and fast-path builders: url=%s", raw)
			head := BuildKeyFast([]byte("HEAD"), []byte(raw), []byte(u.Host), []byte(u.Path), u.Scheme == "https", policy)
			require.Equal(t, head, fromURL, "HEAD must fold to the GET key: url=%s", raw)
		}
	}
}

func TestKeyPolicy_QuerySummary(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "all", (*KeyPolicy)(nil).QuerySummary())
	assert.Equal(t, "all", NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).QuerySummary())
	assert.Equal(t, "allow:2",
		NewKeyPolicy(nil, map[string]bool{"v": true, "id": true}, nil, nil, false, false, nil, false).QuerySummary())
	assert.Equal(t, "strip:1,strip-prefix:1,strip-empty,dedup",
		NewKeyPolicy(map[string]bool{"utm": true}, nil, nil, []string{"/x"}, true, true, nil, false).QuerySummary())
}

// TestHandler_VariantCount pins the count the admin keys surface
// reports (bouine keys find): one tracked store key per live variant.
func TestHandler_VariantCount(t *testing.T) {
	t.Parallel()

	orig := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.Response.Header.Set(header.Vary, "X-Test-Variant")
		_, _ = ctx.Write([]byte("body"))
	}
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20, ReaperInterval: -1})
	h := NewHandler(HandlerConfig{
		Upstream:   orig,
		FastClient: &testFastClient{handler: orig},
		Store:      store,
		Logger:     slog.Default(),
	})

	req := testCtx("GET", "http://example.com/vary")
	primaryKey := h.buildKey(req)
	require.Equal(t, 0, h.VariantCount(primaryKey), "no variants before the first request")

	req.Request.Header.Set("X-Test-Variant", "a")
	h.ServeRequest(req)
	require.Equal(t, 1, h.VariantCount(primaryKey))

	req2 := testCtx("GET", "http://example.com/vary")
	req2.Request.Header.Set("X-Test-Variant", "b")
	h.ServeRequest(req2)
	require.Equal(t, 2, h.VariantCount(primaryKey))

	// A key with no variants reports zero, not a false absence.
	other := BuildKeyFromURL("http://example.com/other", nil)
	require.Equal(t, 0, h.VariantCount(other))

	_ = store.Close(context.Background())
}
