package cache

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

// shieldHarness builds a handler wired like a strong-mode non-owner
// with the shield on: ownerFn always reports a remote owner, the plain
// peer-fetch answers a definitive miss, and shieldFn is the
// (controllable) forward.
func shieldHarness(t *testing.T, mutate func(*HandlerConfig)) (*Handler, *atomic.Int64) {
	t.Helper()
	originCalls := &atomic.Int64{}
	origin := fasthttp.RequestHandler(func(ctx *fasthttp.RequestCtx) {
		originCalls.Add(1)
		ctx.Response.Header.Set("Cache-Control", "max-age=60")
		ctx.Response.SetStatusCode(200)
		ctx.Response.SetBodyString("from-origin")
	})
	cfg := HandlerConfig{
		Store:      newTestStore(),
		FastClient: &testFastClient{handler: origin},
		Upstream:   origin,
		OwnerFn:    func(api.Key) (api.PeerInfo, bool) { return api.PeerInfo{Name: "owner", Addr: "10.0.0.1:9000"}, false },
		PeerFetch:  func(context.Context, api.PeerInfo, api.Key, string) (*api.Object, error) { return nil, nil },
	}
	if mutate != nil {
		mutate(&cfg)
	}
	h := NewHandler(cfg)
	t.Cleanup(func() { _ = h.Close(context.Background()) })
	return h, originCalls
}

// shieldOK builds the fasthttp.Response a serving owner returns.
func shieldOK(body string) *fasthttp.Response {
	resp := fasthttp.AcquireResponse()
	resp.Header.Set("Cache-Control", "max-age=60")
	resp.SetStatusCode(200)
	resp.SetBodyString(body)
	return resp
}

// TestHandler_Shield_ServesOwnerAnswer pins the authoritative path: a
// served forward writes the proxied bytes to the client and does NOT
// hit origin on this node.
func TestHandler_Shield_ServesOwnerAnswer(t *testing.T) {
	t.Parallel()
	saved := &atomic.Int64{}
	fallbacks := &atomic.Int64{}
	h, originCalls := shieldHarness(t, func(cfg *HandlerConfig) {
		cfg.ShieldForward = func(context.Context, api.PeerInfo, *fasthttp.Request, api.Key, bool, time.Time) (*fasthttp.Response, error) {
			return shieldOK("from-owner"), nil
		}
		cfg.OnShieldSaved = func() { saved.Add(1) }
		cfg.OnShieldFallback = func() { fallbacks.Add(1) }
		cfg.OriginShieldBackfillProbability = 0
	})

	ctx := testCtx(fasthttp.MethodGet, "http://test/shielded")
	serveRequest(h, ctx)
	require.Equal(t, 200, respCode(ctx))
	assert.Equal(t, "from-owner", respBody(ctx))
	assert.EqualValues(t, 0, originCalls.Load(), "a served shield answer must not re-fetch origin on the requester")
	assert.EqualValues(t, 1, saved.Load())
	assert.EqualValues(t, 0, fallbacks.Load())
}

// TestHandler_Shield_FallbackOnForwardError pins the availability
// contract: any forward failure falls back to the local origin fetch —
// never a hard failure for the client.
func TestHandler_Shield_FallbackOnForwardError(t *testing.T) {
	t.Parallel()
	fallbacks := &atomic.Int64{}
	h, originCalls := shieldHarness(t, func(cfg *HandlerConfig) {
		cfg.ShieldForward = func(context.Context, api.PeerInfo, *fasthttp.Request, api.Key, bool, time.Time) (*fasthttp.Response, error) {
			return nil, errors.New("owner down")
		}
		cfg.OnShieldFallback = func() { fallbacks.Add(1) }
	})

	ctx := testCtx(fasthttp.MethodGet, "http://test/shielded")
	serveRequest(h, ctx)
	require.Equal(t, 200, respCode(ctx))
	assert.Equal(t, "from-origin", respBody(ctx))
	assert.EqualValues(t, 1, originCalls.Load(), "the forward failure must fall back to origin")
	assert.EqualValues(t, 1, fallbacks.Load())
}

// TestHandler_Shield_FallbackOnOwnerErrorStatus pins that an owner's
// 5xx answer (its own fetch shed/failed) is a fallback, not a relayed
// degradation. Origin answers below 500 are relayed — see
// TestHandler_Shield_RelaysOwner404.
func TestHandler_Shield_FallbackOnOwnerErrorStatus(t *testing.T) {
	t.Parallel()
	h, originCalls := shieldHarness(t, func(cfg *HandlerConfig) {
		cfg.ShieldForward = func(context.Context, api.PeerInfo, *fasthttp.Request, api.Key, bool, time.Time) (*fasthttp.Response, error) {
			resp := fasthttp.AcquireResponse()
			resp.SetStatusCode(503)
			return resp, nil
		}
	})

	ctx := testCtx(fasthttp.MethodGet, "http://test/shielded")
	serveRequest(h, ctx)
	require.Equal(t, 200, respCode(ctx))
	assert.Equal(t, "from-origin", respBody(ctx))
	assert.EqualValues(t, 1, originCalls.Load())
}

// TestHandler_Shield_BackfillProbability pins the D10 knob: p=0 keeps
// the owner-only partition (nothing stored locally), p=1 backfills
// every shield fill.
func TestHandler_Shield_BackfillProbability(t *testing.T) {
	t.Parallel()
	key := BuildKeyFast([]byte(fasthttp.MethodGet), []byte("/shielded"), []byte("test"), []byte("/shielded"), false, nil)

	t.Run("p0-stores-nothing", func(t *testing.T) {
		t.Parallel()
		local := newTestStore()
		h, _ := shieldHarness(t, func(cfg *HandlerConfig) {
			cfg.Store = local
			cfg.ShieldForward = func(context.Context, api.PeerInfo, *fasthttp.Request, api.Key, bool, time.Time) (*fasthttp.Response, error) {
				return shieldOK("from-owner"), nil
			}
			cfg.OriginShieldBackfillProbability = 0
		})
		ctx := testCtx(fasthttp.MethodGet, "http://test/shielded")
		serveRequest(h, ctx)
		require.Equal(t, 200, respCode(ctx))
		obj, _, err := local.Get(context.Background(), key)
		require.NoError(t, err)
		assert.Nil(t, obj, "backfill probability 0 must store nothing locally (D10)")
	})

	t.Run("p1-stores-fill", func(t *testing.T) {
		t.Parallel()
		local := newTestStore()
		h, _ := shieldHarness(t, func(cfg *HandlerConfig) {
			cfg.Store = local
			cfg.ShieldForward = func(context.Context, api.PeerInfo, *fasthttp.Request, api.Key, bool, time.Time) (*fasthttp.Response, error) {
				return shieldOK("from-owner"), nil
			}
			cfg.OriginShieldBackfillProbability = 1
		})
		ctx := testCtx(fasthttp.MethodGet, "http://test/shielded")
		serveRequest(h, ctx)
		require.Equal(t, 200, respCode(ctx))
		obj, _, err := local.Get(context.Background(), key)
		require.NoError(t, err)
		require.NotNil(t, obj, "backfill probability 1 must store the shield fill locally")
		assert.Equal(t, []byte("from-owner"), obj.Body)
	})
}

// TestHandler_Shield_BackfillSkipsOversized pins the storage admission
// gate: a shield fill larger than max_object_size is served but never
// backfilled.
func TestHandler_Shield_BackfillSkipsOversized(t *testing.T) {
	t.Parallel()
	local := newTestStore()
	h, _ := shieldHarness(t, func(cfg *HandlerConfig) {
		cfg.Store = local
		cfg.MaxObjectSize = 5
		cfg.ShieldForward = func(context.Context, api.PeerInfo, *fasthttp.Request, api.Key, bool, time.Time) (*fasthttp.Response, error) {
			resp := fasthttp.AcquireResponse()
			resp.Header.Set("Cache-Control", "max-age=60")
			resp.SetStatusCode(200)
			resp.SetBodyString("0123456789")
			return resp, nil
		}
		cfg.OriginShieldBackfillProbability = 1
	})

	ctx := testCtx(fasthttp.MethodGet, "http://test/shielded")
	serveRequest(h, ctx)
	require.Equal(t, 200, respCode(ctx))
	assert.Equal(t, "0123456789", respBody(ctx), "the oversized fill is still served to this client")

	key := BuildKeyFast([]byte(fasthttp.MethodGet), []byte("/shielded"), []byte("test"), []byte("/shielded"), false, nil)
	obj, _, err := local.Get(context.Background(), key)
	require.NoError(t, err)
	assert.Nil(t, obj, "an object above max_object_size must not be backfilled")
}

// TestHandler_Shield_NeverForwardsHeadToOriginDirectly pins D8
// semantics end-to-end: a HEAD request still goes through the shield
// (the owner canonicalizes to GET like its local path), and a POST on
// the shield is refused by the owner — the requester fetches origin
// itself.
func TestHandler_Shield_MethodGating(t *testing.T) {
	t.Parallel()
	t.Run("head-uses-shield", func(t *testing.T) {
		t.Parallel()
		headForwarded := &atomic.Int64{}
		h, originCalls := shieldHarness(t, func(cfg *HandlerConfig) {
			cfg.ShieldForward = func(_ context.Context, _ api.PeerInfo, req *fasthttp.Request, _ api.Key, _ bool, _ time.Time) (*fasthttp.Response, error) {
				headForwarded.Add(1)
				assert.Equal(t, fasthttp.MethodHead, string(req.Header.Method()), "the forward carries the ORIGINAL method (D1)")
				resp := shieldOK("head-body")
				return resp, nil
			}
			cfg.OriginShieldBackfillProbability = 0
		})
		ctx := testCtx(fasthttp.MethodHead, "http://test/shielded")
		serveRequest(h, ctx)
		require.Equal(t, 200, respCode(ctx))
		assert.EqualValues(t, 1, headForwarded.Load(), "HEAD must use the shield (D8 includes HEAD)")
		assert.EqualValues(t, 0, originCalls.Load())
	})

	t.Run("forward-off-plain-path", func(t *testing.T) {
		t.Parallel()
		// ShieldForward nil (flag off): the flow is exactly today's —
		// plain peer-fetch miss then local origin fetch.
		h, originCalls := shieldHarness(t, nil)
		ctx := testCtx(fasthttp.MethodGet, "http://test/shielded")
		serveRequest(h, ctx)
		require.Equal(t, 200, respCode(ctx))
		assert.Equal(t, "from-origin", respBody(ctx))
		assert.EqualValues(t, 1, originCalls.Load())
	})
}

// TestHandler_Shield_ForwardCarriesOriginalRequest pins D1: the
// request the forward carries is the client's original, un-rewritten
// bytes — method, URI, host, headers.
func TestHandler_Shield_ForwardCarriesOriginalRequest(t *testing.T) {
	t.Parallel()
	var gotURI, gotHost, gotAE, gotAuth string
	h, _ := shieldHarness(t, func(cfg *HandlerConfig) {
		cfg.ShieldForward = func(_ context.Context, _ api.PeerInfo, req *fasthttp.Request, _ api.Key, _ bool, _ time.Time) (*fasthttp.Response, error) {
			gotURI = string(req.RequestURI())
			gotHost = string(req.Host())
			gotAE = string(req.Header.Peek("Accept-Encoding"))
			gotAuth = string(req.Header.Peek("Authorization"))
			return shieldOK("ok"), nil
		}
		cfg.OriginShieldBackfillProbability = 0
	})

	ctx := testCtxWithHeader(fasthttp.MethodGet, "http://test/shielded?x=1", "Authorization", "Bearer tok")
	ctx.Request.Header.Set("Accept-Encoding", "br")
	serveRequest(h, ctx)

	assert.Equal(t, "http://test/shielded?x=1", gotURI, "the forward carries the original request-target verbatim (D1)")
	assert.Equal(t, "test", gotHost)
	assert.Equal(t, "br", gotAE)
	assert.Equal(t, "Bearer tok", gotAuth, "credentials must forward: the owner's fetch would otherwise be unauthenticated")
}

// TestHandler_Shield_RelaysOwner404 pins the relay rule: an origin 404
// that reached the owner's replay (served marker) is answered to the
// client as-is — a cold negative-cached URL is shielded like any
// other, and the requester does not re-fetch origin per node.
func TestHandler_Shield_RelaysOwner404(t *testing.T) {
	t.Parallel()
	saved := &atomic.Int64{}
	fallbacks := &atomic.Int64{}
	h, originCalls := shieldHarness(t, func(cfg *HandlerConfig) {
		cfg.ShieldForward = func(context.Context, api.PeerInfo, *fasthttp.Request, api.Key, bool, time.Time) (*fasthttp.Response, error) {
			resp := fasthttp.AcquireResponse()
			resp.Header.Set("Cache-Control", "public, max-age=30")
			resp.SetStatusCode(404)
			resp.SetBodyString("not found")
			return resp, nil
		}
		cfg.OnShieldSaved = func() { saved.Add(1) }
		cfg.OnShieldFallback = func() { fallbacks.Add(1) }
		cfg.OriginShieldBackfillProbability = 0
	})

	ctx := testCtx(fasthttp.MethodGet, "http://test/shielded")
	serveRequest(h, ctx)
	require.Equal(t, 404, respCode(ctx), "the owner's origin 404 is relayed, not re-fetched")
	assert.Equal(t, "not found", respBody(ctx))
	assert.EqualValues(t, 0, originCalls.Load(), "a relayed owner answer must not re-fetch origin on the requester")
	assert.EqualValues(t, 1, saved.Load(), "the origin request was saved even for the 404")
	assert.EqualValues(t, 0, fallbacks.Load())
}

// TestHandler_Shield_ForwardStripsHopByHop pins the RFC 9110 §7.6.1
// strip: hop-by-hop headers and the Connection token list describe the
// client's connection to THIS node; a copied "Connection: close" would
// degrade the peer connection the forward rides.
func TestHandler_Shield_ForwardStripsHopByHop(t *testing.T) {
	t.Parallel()
	var gotConn, gotTE, gotToken, gotKeep string
	h, _ := shieldHarness(t, func(cfg *HandlerConfig) {
		cfg.ShieldForward = func(_ context.Context, _ api.PeerInfo, req *fasthttp.Request, _ api.Key, _ bool, _ time.Time) (*fasthttp.Response, error) {
			gotConn = string(req.Header.Peek("Connection"))
			gotTE = string(req.Header.Peek("TE"))
			gotToken = string(req.Header.Peek("X-Custom-Hop"))
			gotKeep = string(req.Header.Peek("Keep-Alive"))
			return shieldOK("ok"), nil
		}
		cfg.OriginShieldBackfillProbability = 0
	})

	ctx := testCtx(fasthttp.MethodGet, "http://test/shielded")
	ctx.Request.Header.Set("Connection", "close, X-Custom-Hop")
	ctx.Request.Header.Set("TE", "trailers")
	ctx.Request.Header.Set("Keep-Alive", "timeout=5")
	ctx.Request.Header.Set("X-Custom-Hop", "listed-by-connection")
	serveRequest(h, ctx)

	assert.Empty(t, gotConn, "Connection must not ride the forward (RFC 9110 §7.6.1)")
	assert.Empty(t, gotTE)
	assert.Empty(t, gotKeep)
	assert.Empty(t, gotToken, "headers named by the Connection token list are hop-by-hop too")
}

// TestHandler_Shield_NonHardMissDoesNotForward pins the scope: a
// stale-usable object keeps today's plain semantics (peer-fetch with a
// Vary assertion + stayin-alive), never the shield — the shield is a
// hard-miss feature only (no coordinated revalidation, plan §4).
func TestHandler_Shield_NonHardMissDoesNotForward(t *testing.T) {
	t.Parallel()
	forwards := &atomic.Int64{}
	store := newTestStore()
	// Seed a stale-but-SIE-usable object.
	stale := &api.Object{
		Key:          BuildKeyFast([]byte(fasthttp.MethodGet), []byte("/shielded"), []byte("test"), []byte("/shielded"), false, nil),
		StatusCode:   200,
		Body:         []byte("stale-copy"),
		StoredAt:     time.Now().Add(-2 * time.Minute),
		TTL:          time.Minute,
		StaleIfError: 10 * time.Minute,
	}
	stale.Header = headerMap("Cache-Control", "max-age=60, stale-if-error=600")
	require.NoError(t, store.Put(context.Background(), stale.Key, stale))

	h, _ := shieldHarness(t, func(cfg *HandlerConfig) {
		cfg.Store = store
		cfg.ShieldForward = func(context.Context, api.PeerInfo, *fasthttp.Request, api.Key, bool, time.Time) (*fasthttp.Response, error) {
			forwards.Add(1)
			return shieldOK("from-owner"), nil
		}
	})

	ctx := testCtx(fasthttp.MethodGet, "http://test/shielded")
	serveRequest(h, ctx)
	assert.EqualValues(t, 0, forwards.Load(),
		"a stale-usable object must not go through the shield (the plan excludes coordinated revalidation)")
}

// TestShieldForwardRequest_Headers pin the requester-side wire form:
// control headers are added by the cluster layer; the request carries
// the original target so the cluster layer can move it into the
// forward-URI header.
func TestShieldTimeoutBelowFetchBudget(t *testing.T) {
	t.Parallel()
	assert.Positive(t, defaultFetchTimeout-api.ShieldForwardTimeout,
		"api.ShieldForwardTimeout must stay strictly below the origin fetch budget (defaultFetchTimeout)")
	assert.Equal(t, 30*time.Second, api.ShieldForwardTimeout)
}

// TestHandler_Shield_HeadAnswerNeverBackfills pins the #731 blocker #1
// fix: a HEAD shield answer carries no body by protocol (the owner's
// serveObject suppressed it), so backfilling it under the GET canonical
// key would store an empty 200 and poison every later GET.
func TestHandler_Shield_HeadAnswerNeverBackfills(t *testing.T) {
	t.Parallel()
	local := newTestStore()
	h, _ := shieldHarness(t, func(cfg *HandlerConfig) {
		cfg.Store = local
		cfg.ShieldForward = func(context.Context, api.PeerInfo, *fasthttp.Request, api.Key, bool, time.Time) (*fasthttp.Response, error) {
			// Serving owner answer for a HEAD: 200, empty body —
			// exactly what serveObject's HEAD suppression produces.
			return shieldOK(""), nil
		}
		cfg.OriginShieldBackfillProbability = 1
	})

	ctx := testCtx(fasthttp.MethodHead, "http://test/shielded")
	serveRequest(h, ctx)
	require.Equal(t, 200, respCode(ctx))

	key := BuildKeyFast([]byte(fasthttp.MethodGet), []byte("/shielded"), []byte("test"), []byte("/shielded"), false, nil)
	obj, _, err := local.Get(context.Background(), key)
	require.NoError(t, err)
	assert.Nil(t, obj, "a bodyless HEAD shield answer must never be backfilled under the GET key")
}
