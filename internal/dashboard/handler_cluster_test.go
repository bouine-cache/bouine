package dashboard

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/api"
)

func TestHandler_ClusterWithStoreData(t *testing.T) {
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:        "test",
			HotMaxBytes:  512 << 20, // 512 MiB
			WarmMaxBytes: 20 << 30,  // 20 GiB
			StoreFn: func() api.Stats {
				return api.Stats{
					HotBytes:    14 << 20, // 14 MiB
					HotEntries:  4303,
					WarmBytes:   9 << 20, // 9 MiB
					WarmEntries: 110,
					Evictions:   0,
				}
			},
			Rings: rings,
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", nil),
	}

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/cluster")
	h.cluster(ctx)

	body := string(ctx.Response.Body())
	assert.Contains(t, body, "total cached")
	assert.Contains(t, body, "total objects")
	assert.Contains(t, body, "tier-bar-fill warm")
	// Legend should contain percentage for hot bytes share.
	assert.Contains(t, body, "(61%)")
	assert.Contains(t, body, "(39%)")
	// Entries legend: 4303 / 4413 ≈ 98%, 110 / 4413 ≈ 2%.
	assert.Contains(t, body, "(98%)")
	assert.Contains(t, body, "(2%)")
}

func TestHandler_ClusterWithoutStoreData(t *testing.T) {
	rings := observability.NewRings("self")
	h := &Handler{
		cfg:  Config{Token: "test", Rings: rings},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", nil),
	}

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/cluster")
	h.cluster(ctx)

	body := string(ctx.Response.Body())
	assert.False(t, strings.Contains(body, "c-cache-bytes"))
	assert.False(t, strings.Contains(body, "total cached"))
}

// substringAround returns a window of +/- n chars around the first
// occurrence of needle in s, for debugging test failures.
func substringAround(s, needle string, n int) string {
	idx := strings.Index(s, needle)
	if idx < 0 {
		return "(not found)"
	}
	start := idx - n
	if start < 0 {
		start = 0
	}
	end := idx + len(needle) + n
	if end > len(s) {
		end = len(s)
	}
	return s[start:end]
}
