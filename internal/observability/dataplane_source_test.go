package observability

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

func TestNormaliseSource(t *testing.T) {
	t.Parallel()
	cases := []struct {
		input string
		want  string
	}{
		{"hot", "hot"},
		{"warm", "warm"},
		{"peer", "peer"},
		{"origin", "origin"},
		{"", ""},
		{"unknown", ""},
	}
	for _, c := range cases {
		got := normaliseSource(c.input)
		assert.Equal(t, c.want, got)
	}
}

func TestNormaliseCacheResult(t *testing.T) {
	t.Parallel()
	cases := []struct {
		input string
		want  string
	}{
		{"HIT", "HIT"},
		{"MISS", "MISS"},
		{"STALE", "STALE"},
		{"REVALIDATED", "REVALIDATED"},
		{"BYPASS", "BYPASS"},
		{"", "MISS"},
		{"WEIRD-CACHE-VALUE", "UNKNOWN"},
		{"hit", "UNKNOWN"},
	}
	for _, c := range cases {
		got := normaliseCacheResult(c.input)
		assert.Equal(t, c.want, got)
	}
}

func TestFastHTTPMiddleware_SpoofedRouteHeader_OnNoMatch(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)

	h := m.FastHTTPMiddleware(func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.XCache, "MISS")
		ctx.SetStatusCode(fasthttp.StatusNotFound)
	})

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/nonexistent")
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.Header.Set(header.XBouineRoute, "evil-route-12345")
	h(ctx)

	got, err := reg.Gather()
	require.NoError(t, err, "gather")
	foundDefault := false
	for _, mf := range got {
		if mf.GetName() != "bouine_requests_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			route := labelValue(metric, "route")
			assert.NotEqual(t, "evil-route-12345", route,
				"spoofed X-Bouine-Route header must not appear as route label")
			if route == "_default" {
				foundDefault = true
			}
		}
	}
	assert.True(t, foundDefault, "route label must be _default on no-match, not empty or spoofed")
}

func TestFastHTTPMiddleware_SpoofedRouteHeader_StrippedBeforeHandler(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)

	var seenHeader string
	h := m.FastHTTPMiddleware(func(ctx *fasthttp.RequestCtx) {
		seenHeader = string(ctx.Request.Header.Peek(header.XBouineRoute))
	})

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/test")
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.Header.Set(header.XBouineRoute, "attacker-value")
	h(ctx)

	assert.Empty(t, seenHeader,
		"handler must not see attacker-supplied X-Bouine-Route header")
}

func TestFastHTTPMiddleware_RouterSetsRouteLabel(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)

	h := m.FastHTTPMiddleware(func(ctx *fasthttp.RequestCtx) {
		ctx.SetUserValue(header.XBouineRoute, "my-route")
		ctx.Response.Header.Set(header.XCache, "MISS")
		ctx.SetStatusCode(200)
	})

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/api/v1/foo")
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.Header.Set(header.XBouineRoute, "spoofed")
	h(ctx)

	got, err := reg.Gather()
	require.NoError(t, err, "gather")
	found := false
	spoofedFound := false
	for _, mf := range got {
		if mf.GetName() != "bouine_requests_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			route := labelValue(metric, "route")
			if route == "my-route" {
				found = true
			}
			if route == "spoofed" {
				spoofedFound = true
			}
		}
	}
	assert.True(t, found, "route label must be 'my-route' (set by router, not spoofed)")
	assert.False(t, spoofedFound, "spoofed route label must not appear in metrics")
}

func TestFastHTTPMiddleware_UnknownCacheResultMapsToUnknown(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)

	h := m.FastHTTPMiddleware(func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.XCache, "WEIRD-CACHE-VALUE")
		ctx.SetStatusCode(200)
	})

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/test")
	ctx.Request.Header.SetMethod("GET")
	h(ctx)

	got, err := reg.Gather()
	require.NoError(t, err, "gather")
	found := false
	for _, mf := range got {
		if mf.GetName() != "bouine_requests_total" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			cr := labelValue(metric, "cache_result")
			if cr == "UNKNOWN" {
				found = true
			}
			assert.NotEqual(t, "WEIRD-CACHE-VALUE", cr,
				"unknown X-Cache value must not appear as cache_result label")
		}
	}
	assert.True(t, found, "cache_result must be UNKNOWN for unrecognized X-Cache value")
}

func TestFastHTTPMiddleware_SourceLabel(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)

	h := m.FastHTTPMiddleware(func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.XCache, "HIT")
		ctx.Response.Header.Set(header.XCacheSource, "hot")
		ctx.SetStatusCode(200)
		ctx.Write([]byte("ok"))
	})

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/test")
	ctx.Request.Header.SetMethod("GET")
	h(ctx)

	got, err := reg.Gather()
	require.NoError(t, err, "gather")
	var foundRequests, foundBytes bool
	for _, mf := range got {
		switch mf.GetName() {
		case "bouine_requests_total":
			for _, m := range mf.GetMetric() {
				if labelValue(m, "source") == "hot" && labelValue(m, "cache_result") == "HIT" {
					foundRequests = true
				}
			}
		case "bouine_response_bytes_total":
			for _, m := range mf.GetMetric() {
				if labelValue(m, "source") == "hot" && labelValue(m, "cache_result") == "HIT" {
					foundBytes = true
				}
			}
		}
	}
	assert.True(t, foundRequests)
	assert.True(t, foundBytes)
}

func TestFastHTTPMiddleware_SourceLabel_Empty(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)

	h := m.FastHTTPMiddleware(func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.XCache, "BYPASS")
		ctx.SetStatusCode(200)
	})

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/test")
	ctx.Request.Header.SetMethod("GET")
	h(ctx)

	got, err := reg.Gather()
	require.NoError(t, err, "gather")
	for _, mf := range got {
		if mf.GetName() != "bouine_requests_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelValue(m, "cache_result") == "BYPASS" && labelValue(m, "source") == "" {
				return
			}
		}
	}
	t.Error("requests_total: no BYPASS series with empty source")
}

func TestResponseBytesOut_HasCacheResultAndSource(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewDataPlaneMetrics(reg)

	h := m.FastHTTPMiddleware(func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.XCache, "MISS")
		ctx.Response.Header.Set(header.XCacheSource, "origin")
		ctx.SetStatusCode(200)
		ctx.Write([]byte("body"))
	})

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/test")
	ctx.Request.Header.SetMethod("GET")
	h(ctx)

	got, err := reg.Gather()
	require.NoError(t, err, "gather")
	for _, mf := range got {
		if mf.GetName() != "bouine_response_bytes_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelValue(m, "cache_result") == "MISS" && labelValue(m, "source") == "origin" {
				assert.Len(t, "body", int(m.GetCounter().GetValue()))
				return
			}
		}
	}
	t.Error("response_bytes_total: no MISS/origin series found")
}

func labelValue(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

func TestMethodIndex(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0, methodIndex("GET"))
	assert.Equal(t, 1, methodIndex("HEAD"))
	// Non-GET/HEAD methods take the WithLabelValues fallback so the
	// exact token is preserved on requests_total (issue #607 phase 1.1).
	assert.Equal(t, -1, methodIndex("POST"))
	assert.Equal(t, -1, methodIndex(""))
}

func TestStatusIndex(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0, statusIndex(200))
	assert.Equal(t, 1, statusIndex(206))
	assert.Equal(t, 2, statusIndex(304))
	assert.Equal(t, 3, statusIndex(301))
	assert.Equal(t, 4, statusIndex(302))
	assert.Equal(t, 5, statusIndex(404))
	assert.Equal(t, 6, statusIndex(500))
	assert.Equal(t, -1, statusIndex(999))
	assert.Equal(t, -1, statusIndex(0))
}

func TestCacheResultIndex(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0, cacheResultIndex("HIT"))
	assert.Equal(t, 1, cacheResultIndex("MISS"))
	assert.Equal(t, 2, cacheResultIndex("STALE"))
	assert.Equal(t, 3, cacheResultIndex("REVALIDATED"))
	assert.Equal(t, 4, cacheResultIndex("BYPASS"))
	assert.Equal(t, -1, cacheResultIndex("UNKNOWN"))
}

func TestSourceIndex(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0, sourceIndex(string(api.SourceHot)))
	assert.Equal(t, 1, sourceIndex(string(api.SourceWarm)))
	assert.Equal(t, 2, sourceIndex(string(api.SourcePeer)))
	assert.Equal(t, 3, sourceIndex(string(api.SourceOrigin)))
	assert.Equal(t, 4, sourceIndex(""))
	assert.Equal(t, -1, sourceIndex("unknown"))
}

func TestAccessLogMessage(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "served cache hit", accessLogMessage("HIT", 200))
	assert.Equal(t, "served cache miss", accessLogMessage("MISS", 200))
	assert.Equal(t, "bypassed cache", accessLogMessage("BYPASS", 200))
	assert.Equal(t, "served stale response", accessLogMessage("STALE", 200))
	assert.Equal(t, "served revalidated response", accessLogMessage("REVALIDATED", 200))
	assert.Equal(t, "served uncached response", accessLogMessage("", 200))
	assert.Equal(t, "request completed with error", accessLogMessage("HIT", 500))
	assert.Contains(t, accessLogMessage("UNKNOWN", 200), "unknown")
}

func TestShouldLogAccess_ZeroRate(t *testing.T) {
	t.Parallel()
	m := &DataPlaneMetrics{accessSampleRate: 0}
	assert.True(t, m.shouldLogAccess(api.Key{}))
}

func TestShouldLogAccess_WithKey(t *testing.T) {
	t.Parallel()
	m := &DataPlaneMetrics{accessSampleRate: 100}
	key := api.Key{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	result := m.shouldLogAccess(key)
	_ = result
}
