package cache

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// TestResolver304_MirrorEvictedFallsBackToFill pins the fallback leg of
// ADR-0068: the metadata-only resolver serves its mirror variant's body
// after a validating 304 — but when the mirror is gone from both tiers
// (evicted or purged), the 304 proves nothing servable and the request
// must fall through to the ordinary unconditional fill. Correctness
// over optimization: never fabricate a body the store cannot back.
func TestResolver304_MirrorEvictedFallsBackToFill(t *testing.T) {
	t.Parallel()
	fetches := 0
	// Conditional fetches 304 on the resolver's tag; plain fetches answer
	// 200 with the requested variant's body.
	upstream := func(ctx *fasthttp.RequestCtx) {
		fetches++
		ctx.Response.Header.Set(header.Vary, "X-Test-Variant")
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.ETag, `"v1"`)
		if inm := ctx.Request.Header.Peek(header.IfNoneMatch); len(inm) > 0 &&
			strings.Contains(string(inm), `"v1"`) {
			ctx.SetStatusCode(fasthttp.StatusNotModified)
			return
		}
		_, _ = ctx.Write([]byte("body-" + string(ctx.Request.Header.Peek("X-Test-Variant"))))
	}
	h := testHandler(t, upstream)

	reqA := testCtx("GET", "http://example.com/mirror-gone")
	reqA.Request.Header.Set("X-Test-Variant", "a")
	h.ServeRequest(reqA)
	require.Equal(t, "body-a", respBody(reqA))
	require.Equal(t, 1, fetches)

	primaryKey := h.buildKey(reqA)
	variantKey := VariantKey(primaryKey, "X-Test-Variant", headerFromCtx(reqA), nil)
	// Simulate SIEVE pressure evicting the fat variant while the tiny
	// resolver survives — ADR-0068's pressure shape.
	require.NoError(t, h.store.Delete(t.Context(), variantKey))

	reqB := testCtx("GET", "http://example.com/mirror-gone")
	reqB.Request.Header.Set("X-Test-Variant", "b")
	h.ServeRequest(reqB)

	require.Equal(t, 200, respCode(reqB))
	require.Equal(t, "body-b", respBody(reqB),
		"mirror gone: the validated resolver cannot serve — the ordinary fill must answer with variant b's fresh body")
	require.Equal(t, 3, fetches,
		"conditional resolver fetch (304) + full miss fill; the resolver's validators are an optimization, never a requirement")
}

// TestDoBackgroundRevalidate_SkipsResolver pins ADR-0068: resolvers
// never register for proactive refresh, and a resolver reaching
// doBackgroundRevalidate anyway (a pre-upgrade registry entry surviving
// an upgrade) must be skipped — a full-200 refresh of the primary key
// would re-store a body there, resurrecting the duplicate #824 removed.
func TestDoBackgroundRevalidate_SkipsResolver(t *testing.T) {
	t.Parallel()
	fetches := 0
	upstream := func(ctx *fasthttp.RequestCtx) {
		fetches++
		ctx.Response.Header.Set(header.Vary, "X-Test-Variant")
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		_, _ = ctx.Write([]byte("body"))
	}
	h := testHandler(t, upstream)

	req := testCtx("GET", "http://example.com/bg-skip")
	req.Request.Header.Set("X-Test-Variant", "a")
	h.ServeRequest(req)

	primaryKey := h.buildKey(req)
	resolverObj, _, err := h.store.Get(t.Context(), primaryKey)
	require.NoError(t, err)
	require.NotNil(t, resolverObj)
	require.NotZero(t, resolverObj.PairedKey, "precondition: the primary entry is a metadata-only resolver")

	h.doBackgroundRevalidate(t.Context(), requestInfoFromCtx(req), primaryKey, resolverObj)

	require.Equal(t, 1, fetches,
		"only the initial fill may fetch — a resolver must never be background-refreshed; its mirror variant refreshes on its own cycle")
}

// TestResolverPeerVariant_TwoStepResolution pins the ADR-0068
// convergence path for a node with no local Vary context: the owner
// answers the bare primary-key ask with the metadata-only resolver, and
// this node must derive ITS variant key from the learned Vary list and
// re-ask the variant's owner with a real vary-key assertion — never
// fall straight through to origin. On main this path did not exist:
// streamed resolvers leaked through the owner's withhold gate and were
// served as primary-key hits carrying the first variant's body.
func TestResolverPeerVariant_TwoStepResolution(t *testing.T) {
	t.Parallel()
	originHits := 0
	upstream := func(ctx *fasthttp.RequestCtx) {
		originHits++
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		_, _ = ctx.Write([]byte("origin-fill"))
	}

	req := testCtx("GET", "http://example.com/two-step")
	req.Request.Header.Set("X-Test-Variant", "a")
	primaryKey := BuildKey(requestInfoFromCtx(req), nil)
	variantKey := VariantKey(primaryKey, "X-Test-Variant", headerFromCtx(req), nil)
	wantAssertion := BuildVaryKey("X-Test-Variant", headerFromCtx(req), nil)

	type ask struct {
		key       api.Key
		assertion string
	}
	var asks []ask
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:       1 << 20,
			NumShards:      2,
			ReaperInterval: -1,
		}),
		// This node owns nothing — the non-owner cold-lookup shape.
		OwnerFn: func(api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "owner:1"}, false
		},
		PeerFetch: func(_ context.Context, _ api.PeerInfo, key api.Key, assertion string) (*api.Object, error) {
			asks = append(asks, ask{key: key, assertion: assertion})
			switch key {
			case primaryKey:
				// The metadata-only resolver, as the primary key's
				// owner would answer it (ADR-0068).
				res := &api.Object{
					Key:        primaryKey,
					PairedKey:  variantKey,
					StatusCode: 200,
					VaryValue:  "X-Test-Variant",
					StoredAt:   time.Now(),
					TTL:        time.Minute,
				}
				res.Header = header.NewMap(1)
				res.Header.AppendEntry("Cache-Control", "max-age=60")
				return res, nil
			case variantKey:
				if assertion != wantAssertion {
					return nil, nil
				}
				variant := &api.Object{
					Key:        variantKey,
					VaryKey:    wantAssertion,
					VaryValue:  "X-Test-Variant",
					StatusCode: 200,
					Body:       []byte("variant-a"),
					BodySize:   9,
					StoredAt:   time.Now(),
					TTL:        time.Minute,
				}
				variant.Header = header.NewMap(1)
				variant.Header.AppendEntry("Cache-Control", "max-age=60")
				return variant, nil
			}
			return nil, nil
		},
	})

	h.ServeRequest(req)

	require.Equal(t, 200, respCode(req))
	require.Equal(t, "HIT", respHeader(req, header.XCache),
		"the two-step resolution must serve the variant, not origin")
	require.Equal(t, "variant-a", respBody(req))
	require.Zero(t, originHits, "the resolver metadata must prevent the origin fetch")
	require.Len(t, asks, 2,
		"two peer asks: the bare primary-key ask, then the variant-key ask")
	require.Equal(t, primaryKey, asks[0].key)
	require.Empty(t, asks[0].assertion, "the cold ask carries no assertion — no local Vary context")
	require.Equal(t, variantKey, asks[1].key)
	require.Equal(t, wantAssertion, asks[1].assertion,
		"the second ask must assert THIS request's variant dimension")
}

// TestResolverPeerVariant_ReusesTransferredResolver pins the ADR-0068
// fast-path transfer end to end at the slow path: when the H1 fast path
// already fetched the metadata-only resolver for the identical bare
// ask (h1parser transfers it under api.OwnerResolverContextKey),
// handleCacheMiss must reuse it and run the two-step WITHOUT asking the
// primary key's owner again — one variant-key ask, no bare ask. A
// transferred resolver whose Key does not match the lookup key is a
// different question and must NOT be reused.
func TestResolverPeerVariant_ReusesTransferredResolver(t *testing.T) {
	t.Parallel()

	newCase := func(t *testing.T, transferKeyMismatch bool) (asks []api.Key, wantVariantKey api.Key, served string, code int) {
		t.Helper()
		upstream := func(ctx *fasthttp.RequestCtx) {
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			_, _ = ctx.Write([]byte("origin-fill"))
		}
		req := testCtx("GET", "http://example.com/transfer")
		req.Request.Header.Set("X-Test-Variant", "a")
		primaryKey := BuildKey(requestInfoFromCtx(req), nil)
		variantKey := VariantKey(primaryKey, "X-Test-Variant", headerFromCtx(req), nil)
		wantAssertion := BuildVaryKey("X-Test-Variant", headerFromCtx(req), nil)

		resolver := &api.Object{
			Key:        primaryKey,
			PairedKey:  variantKey,
			StatusCode: 200,
			VaryValue:  "X-Test-Variant",
			StoredAt:   time.Now(),
			TTL:        time.Minute,
		}
		resolver.Header = header.NewMap(1)
		resolver.Header.AppendEntry("Cache-Control", "max-age=60")
		if transferKeyMismatch {
			// The fast path fetched this resolver for a DIFFERENT key —
			// e.g. a variant miss, where the slow path asks the variant
			// key, not the primary. The identity check must reject it.
			other := &api.Object{
				Key:        api.Key{0x99},
				PairedKey:  variantKey,
				StatusCode: 200,
				VaryValue:  "X-Test-Variant",
				StoredAt:   time.Now(),
				TTL:        time.Minute,
			}
			other.Header = header.NewMap(1)
			other.Header.AppendEntry("Cache-Control", "max-age=60")
			req.SetUserValue(api.OwnerResolverContextKey, other)
		} else {
			req.SetUserValue(api.OwnerResolverContextKey, resolver)
		}

		h := NewHandler(HandlerConfig{
			Upstream:   upstream,
			FastClient: &testFastClient{handler: upstream},
			Store: storage.NewHotStore(storage.HotConfig{
				MaxBytes:       1 << 20,
				NumShards:      2,
				ReaperInterval: -1,
			}),
			OwnerFn: func(api.Key) (api.PeerInfo, bool) {
				return api.PeerInfo{Addr: "owner:1"}, false
			},
			PeerFetch: func(_ context.Context, _ api.PeerInfo, key api.Key, _ string) (*api.Object, error) {
				asks = append(asks, key)
				switch key {
				case primaryKey:
					return resolver, nil
				case variantKey:
					variant := &api.Object{
						Key:        variantKey,
						VaryKey:    wantAssertion,
						VaryValue:  "X-Test-Variant",
						StatusCode: 200,
						Body:       []byte("variant-a"),
						BodySize:   9,
						StoredAt:   time.Now(),
						TTL:        time.Minute,
					}
					variant.Header = header.NewMap(1)
					variant.Header.AppendEntry("Cache-Control", "max-age=60")
					return variant, nil
				}
				return nil, nil
			},
		})
		h.ServeRequest(req)
		return asks, variantKey, respBody(req), respCode(req)
	}

	asks, wantVariant, body, code := newCase(t, false)
	require.Equal(t, 200, code)
	require.Equal(t, "variant-a", body, "the two-step must serve the variant")
	require.Len(t, asks, 1,
		"only the variant-key ask: the transferred resolver must spare the identical bare ask")
	require.Equal(t, wantVariant, asks[0],
		"the single ask must be the variant-key ask with a real assertion")

	asks, _, _, _ = newCase(t, true)
	require.Len(t, asks, 2,
		"a transferred resolver with a mismatched Key answers a different question and must not be reused")
}

// TestStoreFromPeer_NoRefreshRegistrationWithoutURI pins the URI gate
// of shouldRegisterRefresh (ADR-0068 review): a refresh-enabled handler
// must NOT register a peer-put object for proactive refresh.
// StoreFromPeer is the one storeObject site whose RequestInfo carries
// no URI — the write-to-owner RPC delivers an api.Object, and the
// object codec has no URL field — and the refresh executor rebuilds
// its fetch from the registry entry's url (refresh_registry.go), so
// such a registration schedules a fetch that fails locally on every
// attempt and retries until the TTL lapses: a scheduled failure
// loop, never a refresh. Peer-put objects keep request-driven
// revalidation (SWR/SIE/stale-on-hit).
func TestStoreFromPeer_NoRefreshRegistrationWithoutURI(t *testing.T) {
	t.Parallel()
	orig := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		_, _ = ctx.Write([]byte("body"))
	}
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20})
	h := NewHandler(HandlerConfig{
		Upstream:            orig,
		FastClient:          &testFastClient{handler: orig},
		Store:               store,
		RefreshBeforeExpiry: true,
	})

	// Contrast: a URI-bearing local fill joins the refresh registry —
	// the gate must not over-suppress ordinary registration.
	req := testCtx("GET", "http://example.com/refresh-pin")
	h.ServeRequest(req)
	require.NotNil(t, h.refreshRegistry.Lookup(h.buildKey(req)),
		"sanity: an ordinary fill registers for proactive refresh")

	// The peer-put shape: stored, never registered.
	key := BuildKey(requestInfoFromCtx(testCtx("GET", "http://example.com/peer-put-refresh")), nil)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		TTL:        time.Hour,
		StoredAt:   time.Now(),
		Header:     header.Map{},
	}
	h.StoreFromPeer(context.Background(), obj)

	stored, _, err := store.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, stored, "the peer-put object is stored")
	require.Nil(t, h.refreshRegistry.Lookup(key),
		"a peer-put object carries no URL on the wire — registering it would schedule a guaranteed-failing refresh")
}

// TestConfigDerivedVary pins the ADR-0071 derivation's normalization: the
// Vary list a KeyPolicy alone determines is the include_headers union with
// the synthetic cookie-presence field, in exactly effectiveVary's stored
// shape (sorted, deduplicated, ", "-joined) so a correct derivation
// hashes to the stored variant keys byte-for-byte.
func TestConfigDerivedVary(t *testing.T) {
	t.Parallel()
	require.Empty(t, configDerivedVary(nil),
		"no policy: the Vary list is the origin's alone — never derivable")
	require.Empty(t, configDerivedVary(NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false)),
		"a policy without key headers keys no variants — never derivable")
	require.Equal(t, "accept-language, bm-market",
		configDerivedVary(NewKeyPolicy(nil, nil, nil, nil, false, false, []string{"BM-Market", "Accept-Language"}, false)),
		"the include list is normalized exactly as effectiveVary normalizes the stored union")
	require.Equal(t, "bm-market, x-bouine-cookie-presence",
		configDerivedVary(NewKeyPolicy(nil, nil, nil, nil, false, false, []string{"BM-Market"}, false).
			WithCookiePresence([]string{"BM_Advertising"})),
		"the synthetic cookie-presence field unions in and sorts with the include list")
}

// TestResolverPeerVariant_ConfigDerivedSingleAsk pins the ADR-0071
// single-ask resolution: on a route whose KeyPolicy alone determines the
// Vary list (include_headers), a plain cold miss on a non-owner must NOT
// pay the bare primary-key ask for resolver metadata — the slow path
// derives the variant key locally and asks the variant's owner directly
// with a real vary-key assertion. One peer ask instead of two.
func TestResolverPeerVariant_ConfigDerivedSingleAsk(t *testing.T) {
	t.Parallel()
	originHits := 0
	upstream := func(ctx *fasthttp.RequestCtx) {
		originHits++
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		_, _ = ctx.Write([]byte("origin-fill"))
	}

	req := testCtx("GET", "http://example.com/config-derived")
	derivedFallbacks := 0
	req.Request.Header.Set("X-Test-Variant", "a")
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, []string{"X-Test-Variant"}, false)
	ri := requestInfoFromCtx(req)
	primaryKey := BuildKey(ri, policy)
	derivedVary := configDerivedVary(policy)
	derivedVk := VariantKey(primaryKey, derivedVary, ri.Header, policy)
	derivedAssertion := BuildVaryKey(derivedVary, ri.Header, policy)

	type ask struct {
		key       api.Key
		assertion string
	}
	var asks []ask
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Policy:     policy,
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:       1 << 20,
			NumShards:      2,
			ReaperInterval: -1,
		}),
		OnResolverDerivedFallback: func() { derivedFallbacks++ },
		// This node owns nothing — the non-owner cold-lookup shape.
		OwnerFn: func(api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "owner:1"}, false
		},
		PeerFetch: func(_ context.Context, _ api.PeerInfo, key api.Key, assertion string) (*api.Object, error) {
			asks = append(asks, ask{key: key, assertion: assertion})
			if key != derivedVk || assertion != derivedAssertion {
				return nil, nil
			}
			variant := &api.Object{
				Key:        derivedVk,
				VaryKey:    derivedAssertion,
				VaryValue:  derivedVary,
				StatusCode: 200,
				Body:       []byte("variant-a"),
				BodySize:   9,
				StoredAt:   time.Now(),
				TTL:        time.Minute,
			}
			variant.Header = header.NewMap(1)
			variant.Header.AppendEntry("Cache-Control", "max-age=60")
			return variant, nil
		},
	})

	h.ServeRequest(req)

	require.Equal(t, 200, respCode(req))
	require.Equal(t, "HIT", respHeader(req, header.XCache),
		"the derived ask must serve the variant, not origin")
	require.Equal(t, "variant-a", respBody(req))
	require.Zero(t, originHits, "the derived ask must prevent the origin fetch")
	require.Len(t, asks, 1,
		"one peer ask: the derived variant-key ask — never the bare primary-key ask")
	require.Equal(t, derivedVk, asks[0].key)
	require.Equal(t, derivedAssertion, asks[0].assertion,
		"the single ask must assert THIS request's variant dimension")
	require.Zero(t, derivedFallbacks,
		"a converged single ask is not a fallback: the derived-fallback counter must stay zero")
}

// TestResolverPeerVariant_ConfigDerivedFallsBackOnWrongDerivation pins
// the ADR-0071 fallback: when the origin declared a Vary field beyond
// the policy's include list, the config-derived Vary list is wrong — the
// derived variant key hashes elsewhere and its owner answers a miss.
// The bare ask must then run (it fetches the authoritative resolver
// metadata) and the ADR-0068 two-step must complete against the REAL Vary
// list: three asks total, the correct variant served, never a wrong
// body.
func TestResolverPeerVariant_ConfigDerivedFallsBackOnWrongDerivation(t *testing.T) {
	t.Parallel()
	originHits := 0
	upstream := func(ctx *fasthttp.RequestCtx) {
		originHits++
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		_, _ = ctx.Write([]byte("origin-fill"))
	}

	// effectiveVary stores the union sorted and ", "-joined; the origin
	// declared X-Origin-Extra, which is NOT in the include list.
	const realVary = "x-origin-extra, x-test-variant"
	derivedFallbacks := 0
	req := testCtx("GET", "http://example.com/wrong-derivation")
	req.Request.Header.Set("X-Test-Variant", "a")
	req.Request.Header.Set("X-Origin-Extra", "z")
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, []string{"X-Test-Variant"}, false)
	ri := requestInfoFromCtx(req)
	primaryKey := BuildKey(ri, policy)
	derivedVary := configDerivedVary(policy)
	derivedVk := VariantKey(primaryKey, derivedVary, ri.Header, policy)
	derivedAssertion := BuildVaryKey(derivedVary, ri.Header, policy)
	realVk := VariantKey(primaryKey, realVary, ri.Header, policy)
	realAssertion := BuildVaryKey(realVary, ri.Header, policy)

	type ask struct {
		key       api.Key
		assertion string
	}
	var asks []ask
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Policy:     policy,
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:       1 << 20,
			NumShards:      2,
			ReaperInterval: -1,
		}),
		OwnerFn: func(api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "owner:1"}, false
		},
		PeerFetch: func(_ context.Context, _ api.PeerInfo, key api.Key, assertion string) (*api.Object, error) {
			asks = append(asks, ask{key: key, assertion: assertion})
			switch key {
			case primaryKey:
				// The metadata-only resolver, as the primary key's owner
				// would answer the bare ask (ADR-0068): the REAL Vary
				// list, beyond what the policy alone determines.
				res := &api.Object{
					Key:        primaryKey,
					PairedKey:  realVk,
					StatusCode: 200,
					VaryValue:  realVary,
					StoredAt:   time.Now(),
					TTL:        time.Minute,
				}
				res.Header = header.NewMap(1)
				res.Header.AppendEntry("Cache-Control", "max-age=60")
				return res, nil
			case realVk:
				if assertion != realAssertion {
					return nil, nil
				}
				variant := &api.Object{
					Key:        realVk,
					VaryKey:    realAssertion,
					VaryValue:  realVary,
					StatusCode: 200,
					Body:       []byte("variant-a"),
					BodySize:   9,
					StoredAt:   time.Now(),
					TTL:        time.Minute,
				}
				variant.Header = header.NewMap(1)
				variant.Header.AppendEntry("Cache-Control", "max-age=60")
				return variant, nil
			}
			// The derived key — including a correctly derived one — has
			// no entry: the derivation was wrong.
			return nil, nil
		},
		OnResolverDerivedFallback: func() { derivedFallbacks++ },
	})

	h.ServeRequest(req)

	require.Equal(t, 200, respCode(req))
	require.Equal(t, "variant-a", respBody(req))
	require.Zero(t, originHits)
	require.Len(t, asks, 3,
		"the wrong derivation costs one extra ask, then falls back to the two-step")
	require.Equal(t, derivedVk, asks[0].key)
	require.Equal(t, derivedAssertion, asks[0].assertion,
		"ask one: the derived variant key with its real assertion")
	require.Equal(t, primaryKey, asks[1].key)
	require.Empty(t, asks[1].assertion,
		"ask two: the bare primary-key ask — no local Vary context")
	require.Equal(t, realVk, asks[2].key)
	require.Equal(t, realAssertion, asks[2].assertion,
		"ask three: the real variant key, asserted from the authoritative Vary list")
	require.Equal(t, 1, derivedFallbacks,
		"the wrong derivation must signal the fallback exactly once: the route is in the degraded three-ask mode")
}

// TestResolverPeerVariant_ConfigDerivedStaleSkipsRederivation pins the
// ADR-0071 rejected-answer skip: when the derived variant ask IS answered
// but the answer is stale (or rejected), the bare ask's metadata carries
// the SAME derived Vary list — re-deriving would repeat the identical
// variant-key question. The two-step must be skipped: two asks total
// (derived ask, bare ask) and the ordinary origin fill, never a third
// identical variant ask.
func TestResolverPeerVariant_ConfigDerivedStaleSkipsRederivation(t *testing.T) {
	t.Parallel()
	originHits := 0
	upstream := func(ctx *fasthttp.RequestCtx) {
		originHits++
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		_, _ = ctx.Write([]byte("origin-fill"))
	}

	req := testCtx("GET", "http://example.com/stale-derived")
	derivedFallbacks := 0
	req.Request.Header.Set("X-Test-Variant", "a")
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, []string{"X-Test-Variant"}, false)
	ri := requestInfoFromCtx(req)
	primaryKey := BuildKey(ri, policy)
	derivedVary := configDerivedVary(policy)
	derivedVk := VariantKey(primaryKey, derivedVary, ri.Header, policy)
	derivedAssertion := BuildVaryKey(derivedVary, ri.Header, policy)

	type ask struct {
		key       api.Key
		assertion string
	}
	var asks []ask
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Policy:     policy,
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:       1 << 20,
			NumShards:      2,
			ReaperInterval: -1,
		}),
		OwnerFn: func(api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "owner:1"}, false
		},
		PeerFetch: func(_ context.Context, _ api.PeerInfo, key api.Key, assertion string) (*api.Object, error) {
			asks = append(asks, ask{key: key, assertion: assertion})
			if key == primaryKey {
				// The metadata-only resolver (ADR-0068) carrying the SAME
				// derived Vary list: the derivation was right, the
				// variant answer was stale.
				res := &api.Object{
					Key:        primaryKey,
					PairedKey:  derivedVk,
					StatusCode: 200,
					VaryValue:  derivedVary,
					StoredAt:   time.Now(),
					TTL:        time.Minute,
				}
				res.Header = header.NewMap(1)
				res.Header.AppendEntry("Cache-Control", "max-age=60")
				return res, nil
			}
			if key == derivedVk && assertion == derivedAssertion {
				// Answered but expired: no SWR window, so the freshness
				// evaluation rejects it and servePeerHit returns false.
				variant := &api.Object{
					Key:        derivedVk,
					VaryKey:    derivedAssertion,
					VaryValue:  derivedVary,
					StatusCode: 200,
					Body:       []byte("variant-stale"),
					BodySize:   13,
					StoredAt:   time.Now().Add(-time.Hour),
					TTL:        time.Minute,
				}
				variant.Header = header.NewMap(1)
				variant.Header.AppendEntry("Cache-Control", "max-age=60")
				return variant, nil
			}
			return nil, nil
		},
		OnResolverDerivedFallback: func() { derivedFallbacks++ },
	})

	h.ServeRequest(req)

	require.Equal(t, 200, respCode(req))
	require.Equal(t, "origin-fill", respBody(req),
		"the stale variant must not be served; the fill is the recovery")
	require.Equal(t, 1, originHits)
	require.Len(t, asks, 2,
		"two asks total: the derived ask, then the bare ask — the identical re-derivation is skipped")
	require.Equal(t, derivedVk, asks[0].key)
	require.Equal(t, derivedAssertion, asks[0].assertion)
	require.Equal(t, primaryKey, asks[1].key)
	require.Empty(t, asks[1].assertion)
	require.Zero(t, derivedFallbacks,
		"the derivation was right and the variant was stale — that is a skip, not a wrong-derivation fallback")
}
