package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestBuildKey_StripQueryParams(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(map[string]bool{"utm_source": true, "fbclid": true}, nil, nil, nil, false, false)

	k1 := BuildKey(requestInfoFromURL("GET", "http://example.com/page?a=1&utm_source=email&b=2"), policy)
	k2 := BuildKey(requestInfoFromURL("GET", "http://example.com/page?a=1&b=2"), nil)

	assert.Equal(t, k2, k1)
}

func TestBuildKey_StripQueryParams_AllStripped(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(map[string]bool{"a": true, "b": true}, nil, nil, nil, false, false)

	k1 := BuildKey(requestInfoFromURL("GET", "http://example.com/page?a=1&b=2"), policy)
	k2 := BuildKey(requestInfoFromURL("GET", "http://example.com/page"), nil)

	assert.Equal(t, k2, k1)
}

func TestBuildKey_StripQueryParams_NilNoEffect(t *testing.T) {
	t.Parallel()
	k1 := BuildKey(requestInfoFromURL("GET", "http://example.com/page?a=1&b=2"), nil)
	k2 := BuildKey(requestInfoFromURL("GET", "http://example.com/page?a=1&b=2"), nil)

	assert.Equal(t, k2, k1)
}

func TestBuildKey_StripQueryParams_StripsSingleParam(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(map[string]bool{"utm_source": true}, nil, nil, nil, false, false)

	k1 := BuildKey(requestInfoFromURL("GET", "http://example.com/page?a=1&utm_source=x"), policy)
	k2 := BuildKey(requestInfoFromURL("GET", "http://example.com/page?a=1"), nil)

	assert.Equal(t, k2, k1)
}

func TestStripQueryParams_HandlerIntegration(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := func(ctx *fasthttp.RequestCtx) {
		calls++
		if string(ctx.URI().QueryArgs().Peek("utm_source")) == "" {
			t.Error("upstream should receive the full query string including utm_source")
		}
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store:      store,
		Policy:     NewKeyPolicy(map[string]bool{"utm_source": true, "fbclid": true}, nil, nil, nil, false, false),
	})

	ctx1 := testCtx("GET", "http://example.com/page?a=1&utm_source=email")
	serveRequest(h, ctx1)

	ctx2 := testCtx("GET", "http://example.com/page?a=1&utm_source=twitter")
	serveRequest(h, ctx2)

	assert.Equal(t, "HIT", respHeader(ctx2, header.XCache))
	assert.Equal(t, 1, calls)
}
