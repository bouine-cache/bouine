package cmd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/cache"
	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/origin"
	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// keysFixture builds a minimal runState with a real store, one
// upstream pool, and one host-agnostic route — the same shape the
// policyForURL tests use — so the keys surface can be exercised
// against the genuine route→policy→key→probe chain.
func keysFixture(t *testing.T) (*engine, *runState) {
	t.Helper()
	originSrv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(fasthttp.StatusOK)
	})
	t.Cleanup(originSrv.Close)

	no := false
	e := &engine{
		cfg: &config.Config{
			Storage: config.Storage{HotMaxBytes: 64 << 20},
			UpstreamPools: []config.UpstreamPool{
				{Name: "echo", Targets: []string{originSrv.Addr}},
			},
			Routes: []config.Route{
				{
					Name:  "pp",
					Pool:  "echo",
					Match: config.RouteMatch{PathPrefix: "/pages/"},
					Cache: config.RouteCache{Key: config.RouteKey{IncludeHost: &no}},
				},
			},
		},
		logger:  newTestLogger(),
		metrics: observability.NewMetrics(),
	}
	store, err := e.buildStore(nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	m := origin.RegisterMetrics(e.metrics.Registry)
	pools, err := e.buildPools(m)
	require.NoError(t, err)
	rs := &runState{
		store:     store,
		pools:     pools,
		rings:     observability.NewRings("test-node"),
		dpMetrics: observability.NewDataPlaneMetrics(e.metrics.Registry),
	}
	rs.router = e.buildRouter(rs)
	require.Len(t, rs.handlers, 1)
	// Record ring traffic while the default rate (0) still records
	// everything, then report a non-zero sample rate for the wire.
	for range 3 {
		rs.rings.URL.RecordURL("/api/users", "api", "MISS")
	}
	rs.rings.URL.RecordURL("/api/users", "api", "BYPASS")
	rs.rings.URL.RecordURL("/static/img", "static", "STALE")
	rs.rings.URL.SetSampleRate(100)
	return e, rs
}

// TestKeysFindFn_ResolvesAndProbes pins the end-to-end find chain
// (issue #816): the route is resolved by the same first-match-wins
// walk the data plane uses, the key is built by the route's policy,
// and the local block comes from the side-effect-free probe.
func TestKeysFindFn_ResolvesAndProbes(t *testing.T) {
	t.Parallel()
	e, rs := keysFixture(t)

	rawURL := "http://internal.example.com/pages/p?x=1"
	key := cache.BuildKeyFromURL(rawURL, policyForURL(rs, rawURL))
	storedAt := time.Now().Add(-30 * time.Second)
	require.NoError(t, rs.store.Put(context.Background(), key, &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.NewMap(2),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   storedAt,
		TTL:        time.Minute,
		Pool:       "echo",
	}))

	res, err := e.keysFindFn(rs)(context.Background(), rawURL, false)
	require.NoError(t, err)
	assert.Equal(t, rawURL, res.URL)
	assert.Equal(t, key.Hex(), res.KeyHex)
	assert.Equal(t, "pp", res.Route, "the URL must resolve to the pp route")
	assert.Equal(t, "internal.example.com", res.Host)
	assert.Equal(t, "/pages/p", res.Path)
	assert.Equal(t, "all", res.QueryPolicy)
	assert.Nil(t, res.Owner, "standalone: no ring owner")

	require.True(t, res.Local.Present)
	assert.Equal(t, "hot", res.Local.Tier)
	assert.Equal(t, int64(4), res.Local.SizeBytes)
	assert.Equal(t, int64(60), res.Local.TTLSeconds)
	assert.Equal(t, 200, res.Local.StatusCode)
	assert.Equal(t, "echo", res.Local.Pool)
	assert.Equal(t, 0, res.Variants)

	// An absent URL under the same route reports why.
	absent, err := e.keysFindFn(rs)(context.Background(), "http://internal.example.com/pages/other", false)
	require.NoError(t, err)
	assert.False(t, absent.Local.Present)
	assert.Equal(t, "no_entry", absent.AbsentReason)

	// Unparseable URLs are a client error, not a silent zero key.
	_, err = e.keysFindFn(rs)(context.Background(), "ht tp://bad\x00", false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unparseable")
}

// TestKeysTopFn_ServesRing pins the top/recent provider against the
// real URL ring: kind routing, breakdown, and the sample rate report.
func TestKeysTopFn_ServesRing(t *testing.T) {
	t.Parallel()
	e, rs := keysFixture(t)

	fn := e.keysTopFn(rs)
	res := fn("top", "", 10)
	assert.Equal(t, "top", res.Kind)
	assert.Equal(t, 100, res.SampleRate)
	require.Len(t, res.Entries, 2)
	assert.Equal(t, "/api/users", res.Entries[0].Prefix)
	assert.Equal(t, int64(4), res.Entries[0].Requests)
	assert.Equal(t, int64(3), res.Entries[0].Misses)
	assert.Equal(t, int64(1), res.Entries[0].Bypass)
	assert.Equal(t, "/static/img", res.Entries[1].Prefix)
	assert.Equal(t, int64(1), res.Entries[1].Stale)

	filtered := fn("top", "bypass", 10)
	require.Len(t, filtered.Entries, 1)
	assert.Equal(t, "/api/users", filtered.Entries[0].Prefix)

	recent := fn("recent", "", 1)
	assert.Equal(t, "recent", recent.Kind)
	require.Len(t, recent.Entries, 1)
	assert.Equal(t, "/static/img", recent.Entries[0].Prefix,
		"the most recently recorded prefix leads the recent listing")
	assert.NotEmpty(t, recent.Entries[0].LastSeen)
}
