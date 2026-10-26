package cmd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/cache"
	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/origin"
	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// buildExplainRouter assembles a routed runState with a configurable
// origin, the same wiring builder_routematch_test's buildTestRouter
// uses — but the origin here varies by request header so the tests
// below can fill genuine Vary variants through the real data plane.
func buildExplainRouter(t *testing.T, originHandler fasthttp.RequestHandler, routes ...config.Route) *runState {
	t.Helper()
	originSrv := fasthttptest.NewServer(t, originHandler)
	t.Cleanup(originSrv.Close)
	e := &engine{
		cfg: &config.Config{
			UpstreamPools: []config.UpstreamPool{
				{Name: "app", Targets: []string{originSrv.Addr}},
			},
			Routes: routes,
			// A zero budget (the bare-config default) makes every Put
			// overshoot and the sweeper eat the fills before explain
			// can probe them.
			Storage: config.Storage{HotMaxBytes: 1 << 20},
		},
		logger:  newTestLogger(),
		metrics: observability.NewMetrics(),
	}
	store, err := e.buildStore(nil, nil, nil)
	require.NoError(t, err)
	m := origin.RegisterMetrics(e.metrics.Registry)
	pools, err := e.buildPools(m)
	require.NoError(t, err)
	rs := &runState{
		store:     store,
		pools:     pools,
		dpMetrics: observability.NewDataPlaneMetrics(e.metrics.Registry),
	}
	router := e.buildRouter(rs)
	require.NotNil(t, router)
	rs.router = router
	return rs
}

// waitForFill synchronizes with the routed fast path's asynchronous
// fill: the MISS response returns before the store write lands, so a
// test that explains immediately races the fill. Polling the probe is
// the sanctioned synchronization (AGENTS.md §8 bans sleeps, not
// condition polling with a deadline).
func waitForFill(t *testing.T, rs *runState, rawURL string) {
	t.Helper()
	key := cache.BuildKeyFromURL(rawURL, policyForURL(rs, rawURL))
	require.Eventually(t, func() bool {
		st, ok := rs.store.Probe(key)
		return ok && st.Present
	}, 2*time.Second, 5*time.Millisecond, "the fill never landed in the store")
}

// serveWithAE serves one request through the data plane with an
// Accept-Encoding header, the way a real client would.
func serveWithAE(t *testing.T, rs *runState, host, path, ae string) {
	t.Helper()
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI(path)
	ctx.Request.Header.SetHost(host)
	ctx.Request.Header.Set(header.AcceptEncoding, ae)
	rs.router.ServeRequest(ctx)
}

// TestExplainRouted_VariantMatchHit is the end-to-end pin for the
// acceptance criterion the routeless tests cannot reach: the nominal
// browser request selects the br bucket, the br variant IS stored, and
// explain reports a hit — with the route's name and pool, the fill
// path's own cacheability verdict, and both variants enumerated with
// their bucket labels through the handler's index.
func TestExplainRouted_VariantMatchHit(t *testing.T) {
	t.Parallel()
	originHandler := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.Response.Header.Set(header.Vary, header.AcceptEncoding)
		ctx.SetStatusCode(fasthttp.StatusOK)
		_, _ = ctx.Write([]byte("body-" + string(ctx.Request.Header.Peek(header.AcceptEncoding))))
	}
	rs := buildExplainRouter(t, originHandler, config.Route{
		Name: "shop", Pool: "app", Match: config.RouteMatch{Host: "www.example.com"},
	})

	// Fill the br variant through the data plane — the bucket the
	// pinned nominal request selects. (The two-dialect enumeration is
	// pinned at the cache layer in TestVariantsOf; here one genuine
	// fill is enough to pin the routed end-to-end chain.)
	serveWithAE(t, rs, "www.example.com", "/v", "br")
	waitForFill(t, rs, "http://www.example.com/v")

	result := explainURL(context.Background(), "http://www.example.com/v", rs)
	assert.Equal(t, "hit", result.Verdict)
	assert.Equal(t, "HIT", result.CacheResult)
	assert.Equal(t, "shop", result.Route.Name)
	assert.Equal(t, "app", result.Route.Pool)
	assert.Equal(t, "cacheable", result.Cacheable.Verdict)
	assert.Equal(t, "max-age=3600", result.Cacheable.Directives)

	assert.Equal(t, header.AcceptEncoding, result.Variant.Vary)
	assert.Equal(t, "br", result.Variant.RequestBucket,
		"the pinned nominal request selects the br bucket")
	assert.True(t, result.Variant.Matched, "the br variant is stored")

	require.Len(t, result.Variant.StoredVariants, 1)
	sv := result.Variant.StoredVariants[0]
	assert.Equal(t, "br", sv.Bucket, "the index enumerated the variant with its derived bucket label")
	assert.True(t, sv.Fresh, "the fill is young")
	assert.Equal(t, "hot", sv.Tier)
	assert.NotEmpty(t, sv.Age, "the freshness row reads the variant's age")
	assert.Equal(t, len(result.Variant.StoredVariants), result.Stored.VariantsPresent,
		"the stored section reports the enumerated variant count")
	assert.Equal(t, "fresh", result.TTL.Verdict)
	assert.True(t, result.Stored.Present)
	assert.Empty(t, result.Bans)
}

// TestExplainRouted_QueryPolicyDerivation pins the key derivation on
// a route with a query policy: the raw query and the policy's
// stripped names both appear, computed by the same predicate the key
// builder uses.
func TestExplainRouted_QueryPolicyDerivation(t *testing.T) {
	t.Parallel()
	originHandler := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(fasthttp.StatusOK)
	}
	rs := buildExplainRouter(t, originHandler, config.Route{
		Name: "track", Pool: "app",
		Cache: config.RouteCache{Key: config.RouteKey{
			StripQueryParams: []string{"utm_source"},
		}},
	})

	result := explainURL(context.Background(), "http://www.example.com/t?utm_source=x&v=2", rs)
	assert.Equal(t, "track", result.Route.Name)
	var stripped []string
	for _, step := range result.Key.Derivation {
		switch step.Step {
		case "host":
			assert.Equal(t, "www.example.com", step.Value)
		case "path":
			assert.Equal(t, "/t", step.Value)
		case "query":
			assert.Equal(t, "utm_source=x&v=2", step.Value)
		case "query_policy":
			stripped = step.Stripped
		}
	}
	assert.Equal(t, []string{"utm_source"}, stripped,
		"the derivation reports the policy's actual effect on this query")
}

// TestExplainRouted_NotCacheable pins the not-cacheable branch end to
// end: a stored object whose headers fail the route's fill-time
// verdict (config drift, the one way this happens) explains as
// MISS · not cacheable, with the directives reported verbatim.
func TestExplainRouted_NotCacheable(t *testing.T) {
	t.Parallel()
	originHandler := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(fasthttp.StatusOK)
	}
	rs := buildExplainRouter(t, originHandler, config.Route{
		Name: "shop", Pool: "app", Match: config.RouteMatch{Host: "www.example.com"},
	})

	// Hand-store a response the route would no longer accept, the way
	// a config change between fill and explain leaves the store.
	// A policy-less route resolves to the default host-ful policy,
	// exactly what the data plane computes for such routes.
	key := cache.BuildKeyFromURL("http://www.example.com/private", policyForURL(rs, "http://www.example.com/private"))
	m := header.NewMap(2)
	m.Set(header.XBouineHost, "www.example.com")
	m.Set(header.XBouinePath, "/private")
	obj := &api.Object{
		Key:          key,
		Header:       m,
		StatusCode:   200,
		StoredAt:     time.Now(),
		TTL:          time.Hour,
		CacheControl: "private",
	}
	require.NoError(t, rs.store.Put(context.Background(), key, obj.CloneForStorage(nil)))

	result := explainURL(context.Background(), "http://www.example.com/private", rs)
	assert.Equal(t, "miss", result.Verdict)
	assert.Equal(t, "not-cacheable", result.Reason)
	assert.Equal(t, "not-cacheable", result.Cacheable.Verdict)
	assert.Equal(t, "private", result.Cacheable.Directives)
	assert.Equal(t, "fresh", result.TTL.Verdict,
		"the entry itself is fresh — cacheability is the config-drift signal")
}

// TestExplainRouted_Bypass pins the bypass contract cachecheck
// established: an unparseable URL produces no key, so there is no
// chain to explain.
func TestExplainRouted_Bypass(t *testing.T) {
	t.Parallel()
	rs := explainTestState(t)
	for _, rawURL := range []string{"", "://unparseable"} {
		result := explainURL(context.Background(), rawURL, rs)
		assert.Equal(t, "bypass", result.Verdict, "url %q", rawURL)
		assert.Equal(t, "BYPASS", result.CacheResult)
	}
}
