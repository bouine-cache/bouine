package cache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

// countingOrigin counts the origin requests that reach the fast client
// and delays the first one until released, so concurrent callers can be
// collapsed onto the same singleflight.
type countingOrigin struct {
	calls   atomic.Int64
	release chan struct{}
	once    sync.Once
	handler fasthttp.RequestHandler
}

func (c *countingOrigin) serve(ctx *fasthttp.RequestCtx) {
	c.calls.Add(1)
	c.once.Do(func() {
		<-c.release
	})
	if c.handler != nil {
		c.handler(ctx)
		return
	}
	ctx.Response.Header.Set("Content-Type", "text/plain")
	ctx.Response.Header.Set("Cache-Control", "max-age=60")
	ctx.Response.SetStatusCode(200)
	ctx.Response.SetBodyString("shielded")
}

// TestFetchOrigin_CollapsesConcurrentFetches is the owner-side
// acceptance unit test: N concurrent coalesced peer fetches for the
// same key share ONE origin request (singleflight), and the object is
// stored locally on the owner.
func TestFetchOrigin_CollapsesConcurrentFetches(t *testing.T) {
	t.Parallel()
	origin := &countingOrigin{release: make(chan struct{})}
	store := newTestStore()
	h := NewHandler(HandlerConfig{
		Store:      store,
		FastClient: &testFastClient{handler: origin.serve},
	})
	defer h.Close(context.Background())

	// The owner-side envelope is rebuilt per peer RPC in production
	// (peerfetch.go handleCoalesce), so each waiter brings its own
	// request here too.
	newOriginReq := func() *fasthttp.Request {
		r := fasthttp.AcquireRequest()
		r.Header.SetMethod(fasthttp.MethodGet)
		r.SetRequestURI("/shielded?x=1")
		r.SetHost("origin.example")
		return r
	}

	key := BuildKeyFast([]byte(fasthttp.MethodGet), []byte("/shielded?x=1"), []byte("origin.example"), []byte("/shielded"), false, nil)

	const waiters = 8
	var wg sync.WaitGroup
	errs := make([]error, waiters)
	objs := make([]*api.Object, waiters)
	for i := range waiters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			obj, err := h.FetchOrigin(context.Background(), key, newOriginReq())
			errs[i] = err
			objs[i] = obj
		}()
	}
	// Give the waiters time to pile onto the singleflight, then let the
	// leader's origin fetch complete.
	time.Sleep(50 * time.Millisecond)
	close(origin.release)
	wg.Wait()

	require.NoError(t, errors.Join(errs...))
	assert.EqualValues(t, 1, origin.calls.Load(), "origin must see exactly one request")
	for i := range waiters {
		require.NotNil(t, objs[i])
		assert.Equal(t, 200, objs[i].StatusCode)
	}
	stored, _, err := store.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, stored, "owner must store its own fill")
}

// FetchOrigin surface: a transport failure returns an error (the
// peer-fetch handler maps it to the dedicated failure status), a
// non-GET/HEAD envelope is refused outright, and an origin error
// STATUS is an authoritative answer (negative caching), never an error.
func TestFetchOrigin_Errors(t *testing.T) {
	t.Parallel()
	origin := fasthttp.RequestHandler(func(ctx *fasthttp.RequestCtx) {
		ctx.Response.SetStatusCode(502)
	})
	h := NewHandler(HandlerConfig{
		Store:      newTestStore(),
		FastClient: &testFastClient{handler: origin},
	})
	defer h.Close(context.Background())

	originReq := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(originReq)
	originReq.Header.SetMethod(fasthttp.MethodGet)
	originReq.SetRequestURI("/broken")
	key := BuildKeyFast([]byte(fasthttp.MethodGet), []byte("/broken"), []byte("o"), []byte("/broken"), false, nil)

	post := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(post)
	post.Header.SetMethod(fasthttp.MethodPost)
	post.SetRequestURI("/broken")
	_, err := h.FetchOrigin(context.Background(), key, post)
	require.ErrorIs(t, err, errCoalesceMethod)

	obj, err := h.FetchOrigin(context.Background(), key, originReq)
	require.NoError(t, err, "an origin error STATUS is an authoritative answer, not a flight failure (negative caching survives when a negative_ttl policy covers the status)")
	require.NotNil(t, obj)
	assert.Equal(t, 502, obj.StatusCode)
}

// TestHandler_CoalesceWait_FallbackOnPeerError pins the availability
// path: a peer error sends the request to origin (never a hard failure
// for the client), while a coalesced answer is authoritative and does
// NOT trigger an origin request.
func TestHandler_CoalesceWait(t *testing.T) {
	t.Parallel()
	originCalls := atomic.Int64{}
	origin := fasthttp.RequestHandler(func(ctx *fasthttp.RequestCtx) {
		originCalls.Add(1)
		ctx.Response.Header.Set("Cache-Control", "max-age=60")
		ctx.Response.SetStatusCode(200)
		ctx.Response.SetBodyString("from-origin")
	})
	store := newTestStore()

	t.Run("authoritative", func(t *testing.T) {
		originCalls.Store(0)
		local := newTestStore()
		saved := atomic.Int64{}
		h := NewHandler(HandlerConfig{
			Store:      local,
			FastClient: &testFastClient{handler: origin},
			Upstream:   origin,
			OwnerFn:    func(api.Key) (api.PeerInfo, bool) { return api.PeerInfo{Name: "owner"}, false },
			PeerFetch:  func(context.Context, api.PeerInfo, api.Key, string) (*api.Object, error) { return nil, nil },
			PeerFetchCoalesce: func(_ context.Context, _ api.PeerInfo, _ api.Key, _ string, _ *api.OriginRequest) (*api.Object, error) {
				obj := &api.Object{
					Key:        api.Key{},
					StatusCode: 200,
					Body:       []byte("from-owner"),
					BodySize:   10,
					StoredAt:   time.Now(),
					TTL:        time.Minute,
				}
				obj.Header = headerMap("Cache-Control", "max-age=60")
				return obj, nil
			},
			OnCoalescedSaved:        func() { saved.Add(1) },
			PeerBackfillProbability: 0,
		})
		defer h.Close(context.Background())

		ctx := testCtx(fasthttp.MethodGet, "http://test/shielded")
		serveRequest(h, ctx)
		require.Equal(t, 200, respCode(ctx))
		assert.Equal(t, "from-owner", respBody(ctx))
		assert.EqualValues(t, 0, originCalls.Load(), "a coalesced answer must not re-fetch origin")
		assert.EqualValues(t, 1, saved.Load())
		// backfill probability 0: nothing stored locally.
		key := BuildKeyFast([]byte(fasthttp.MethodGet), []byte("/shielded"), []byte("test"), []byte("/shielded"), false, nil)
		obj, _, err := local.Get(context.Background(), key)
		require.NoError(t, err)
		assert.Nil(t, obj, "backfill probability 0 must store nothing locally")
	})

	t.Run("fallback-on-error", func(t *testing.T) {
		originCalls.Store(0)
		h := NewHandler(HandlerConfig{
			Store:      store,
			FastClient: &testFastClient{handler: origin},
			Upstream:   origin,
			OwnerFn:    func(api.Key) (api.PeerInfo, bool) { return api.PeerInfo{Name: "owner"}, false },
			PeerFetch:  func(context.Context, api.PeerInfo, api.Key, string) (*api.Object, error) { return nil, nil },
			PeerFetchCoalesce: func(context.Context, api.PeerInfo, api.Key, string, *api.OriginRequest) (*api.Object, error) {
				return nil, fmt.Errorf("owner down")
			},
		})
		defer h.Close(context.Background())

		ctx := testCtx(fasthttp.MethodGet, "http://test/shielded")
		serveRequest(h, ctx)
		require.Equal(t, 200, respCode(ctx))
		assert.Equal(t, "from-origin", respBody(ctx))
		assert.EqualValues(t, 1, originCalls.Load(), "peer error must fall back to origin")
	})

	t.Run("coalesce-off-uses-plain-peer-fetch", func(t *testing.T) {
		originCalls.Store(0)
		peerCalls := atomic.Int64{}
		h := NewHandler(HandlerConfig{
			Store:      newTestStore(),
			FastClient: &testFastClient{handler: origin},
			Upstream:   origin,
			OwnerFn:    func(api.Key) (api.PeerInfo, bool) { return api.PeerInfo{Name: "owner"}, false },
			PeerFetch: func(context.Context, api.PeerInfo, api.Key, string) (*api.Object, error) {
				peerCalls.Add(1)
				return nil, nil
			},
		})
		defer h.Close(context.Background())

		ctx := testCtx(fasthttp.MethodGet, "http://test/shielded")
		serveRequest(h, ctx)
		require.Equal(t, 200, respCode(ctx))
		assert.EqualValues(t, 1, peerCalls.Load(), "the plain peer fetch must still run")
		assert.EqualValues(t, 1, originCalls.Load())
	})
}

// TestHandler_BackfillProbabilityOne pins the default-knob behavior:
// p=1.0 stores every coalesced object on the non-owner too.
func TestHandler_BackfillProbabilityOne(t *testing.T) {
	t.Parallel()
	origin := fasthttp.RequestHandler(func(ctx *fasthttp.RequestCtx) {
		ctx.Response.SetStatusCode(200)
		ctx.Response.SetBodyString("x")
	})
	local := newTestStore()
	h := NewHandler(HandlerConfig{
		Store:      local,
		FastClient: &testFastClient{handler: origin},
		Upstream:   origin,
		OwnerFn:    func(api.Key) (api.PeerInfo, bool) { return api.PeerInfo{Name: "owner"}, false },
		PeerFetch:  func(context.Context, api.PeerInfo, api.Key, string) (*api.Object, error) { return nil, nil },
		PeerFetchCoalesce: func(_ context.Context, _ api.PeerInfo, _ api.Key, _ string, _ *api.OriginRequest) (*api.Object, error) {
			obj := &api.Object{
				Key:        api.Key{},
				StatusCode: 200,
				Body:       []byte("from-owner"),
				BodySize:   10,
				StoredAt:   time.Now(),
				TTL:        time.Minute,
			}
			obj.Header = headerMap("Cache-Control", "max-age=60")
			return obj, nil
		},
		PeerBackfillProbability: 1.0,
	})
	defer h.Close(context.Background())

	ctx := testCtx(fasthttp.MethodGet, "http://test/shielded")
	serveRequest(h, ctx)
	require.Equal(t, 200, respCode(ctx))

	key := BuildKeyFast([]byte(fasthttp.MethodGet), []byte("/shielded"), []byte("test"), []byte("/shielded"), false, nil)
	obj, _, err := local.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, obj, "backfill probability 1.0 must store the coalesced object locally")
	assert.Equal(t, []byte("from-owner"), obj.Body)
}

// TestFetchOrigin_HeadEnvelopeStoresGetBody is the regression test for
// the HEAD store-poisoning bug: a HEAD envelope used to fetch origin
// with HEAD and store the empty body under the (HEAD→GET-canonicalized)
// cache key, so every later GET on the owner served an empty body. The
// owner must canonicalize HEAD→GET like the local miss path does.
func TestFetchOrigin_HeadEnvelopeStoresGetBody(t *testing.T) {
	t.Parallel()
	originMethods := make(chan string, 4)
	origin := fasthttp.RequestHandler(func(ctx *fasthttp.RequestCtx) {
		originMethods <- string(ctx.Method())
		ctx.Response.Header.Set("Cache-Control", "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("shielded"))
	})
	store := newTestStore()
	h := NewHandler(HandlerConfig{Store: store, FastClient: &testFastClient{handler: origin}})
	defer h.Close(context.Background())

	oreq := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(oreq)
	oreq.Header.SetMethod(fasthttp.MethodHead)
	oreq.SetRequestURI("/heady")
	oreq.SetHost("o")

	key := BuildKeyFast([]byte(fasthttp.MethodHead), []byte("/heady"), []byte("o"), []byte("/heady"), false, nil)
	obj, err := h.FetchOrigin(context.Background(), key, oreq)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, []byte("shielded"), obj.Body, "a HEAD envelope must be answered with the GET body")
	require.Len(t, originMethods, 1)
	assert.Equal(t, fasthttp.MethodGet, <-originMethods, "the owner must fetch origin with GET")

	stored, _, err := store.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, stored, "the owner must store its fill under the canonical key")
	assert.Equal(t, []byte("shielded"), stored.Body, "the stored object must carry the GET body, not the empty HEAD body")
}

// TestOriginEnvelope_HeadBecomesGetAndForwardsCredentials pins the
// envelope contract: HEAD is rewritten to GET (the owner replays a GET
// fetch), and Authorization is forwarded — the local path sends it, and
// dropping it would make the owner fetch unauthenticated and negatively
// cache the wrong answer.
func TestOriginEnvelope_HeadBecomesGetAndForwardsCredentials(t *testing.T) {
	t.Parallel()
	h := NewHandler(HandlerConfig{Store: newTestStore()})
	defer h.Close(context.Background())

	ctx := testCtxWithHeader(fasthttp.MethodHead, "http://test/envelope", "Authorization", "Bearer tok")
	ctx.Request.Header.Set("Accept-Encoding", "br")
	ctx.Request.Header.Set("X-Irrelevant", "drop-me")

	oreq := h.originEnvelope(ctx)
	assert.Equal(t, fasthttp.MethodGet, oreq.Method, "HEAD must be canonicalized to GET in the envelope")

	names := map[string]string{}
	for _, hd := range oreq.Headers {
		names[strings.ToLower(hd.Name)] = hd.Value
	}
	assert.Equal(t, "Bearer tok", names["authorization"], "Authorization must be forwarded: it changes the origin's answer")
	assert.Equal(t, "br", names["accept-encoding"])
	assert.NotContains(t, names, "x-irrelevant", "the envelope is a closed set, not an open forward")
}

// TestHandler_BackfillSkipsOversized pins the storage admission gate:
// a coalesced object larger than max_object_size is served to the
// client but never backfilled into the local store.
func TestHandler_BackfillSkipsOversized(t *testing.T) {
	t.Parallel()
	origin := fasthttp.RequestHandler(func(ctx *fasthttp.RequestCtx) {
		ctx.Response.SetStatusCode(200)
		ctx.Response.SetBodyString("x")
	})
	local := newTestStore()
	h := NewHandler(HandlerConfig{
		Store:         local,
		FastClient:    &testFastClient{handler: origin},
		Upstream:      origin,
		MaxObjectSize: 5,
		OwnerFn:       func(api.Key) (api.PeerInfo, bool) { return api.PeerInfo{Name: "owner"}, false },
		PeerFetch:     func(context.Context, api.PeerInfo, api.Key, string) (*api.Object, error) { return nil, nil },
		PeerFetchCoalesce: func(_ context.Context, _ api.PeerInfo, _ api.Key, _ string, _ *api.OriginRequest) (*api.Object, error) {
			obj := &api.Object{
				Key:        api.Key{},
				StatusCode: 200,
				Body:       []byte("0123456789"),
				BodySize:   10,
				StoredAt:   time.Now(),
				TTL:        time.Minute,
			}
			obj.Header = headerMap("Cache-Control", "max-age=60")
			return obj, nil
		},
		PeerBackfillProbability: 1.0,
	})
	defer h.Close(context.Background())

	ctx := testCtx(fasthttp.MethodGet, "http://test/shielded")
	serveRequest(h, ctx)
	require.Equal(t, 200, respCode(ctx))
	assert.Equal(t, "0123456789", respBody(ctx), "the oversized object is still served to this client")

	key := BuildKeyFast([]byte(fasthttp.MethodGet), []byte("/shielded"), []byte("test"), []byte("/shielded"), false, nil)
	obj, _, err := local.Get(context.Background(), key)
	require.NoError(t, err)
	assert.Nil(t, obj, "an object above max_object_size must not be backfilled")
}

// TestCoalesceWait_FallbackCounted pins the waiter-side observability:
// a failed coalesced wait (owner down, timeout, shed, variant-gate
// rejection) must be counted as a fallback — the owner-side counters
// cannot see it, and without it the shield-effectiveness math is wrong.
func TestCoalesceWait_FallbackCounted(t *testing.T) {
	t.Parallel()
	origin := fasthttp.RequestHandler(func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("Cache-Control", "max-age=60")
		ctx.Response.SetStatusCode(200)
		ctx.Response.SetBodyString("from-origin")
	})
	saved := atomic.Int64{}
	fallbacks := atomic.Int64{}
	h := NewHandler(HandlerConfig{
		Store:      newTestStore(),
		FastClient: &testFastClient{handler: origin},
		Upstream:   origin,
		OwnerFn:    func(api.Key) (api.PeerInfo, bool) { return api.PeerInfo{Name: "owner"}, false },
		PeerFetch:  func(context.Context, api.PeerInfo, api.Key, string) (*api.Object, error) { return nil, nil },
		PeerFetchCoalesce: func(context.Context, api.PeerInfo, api.Key, string, *api.OriginRequest) (*api.Object, error) {
			return nil, fmt.Errorf("owner down")
		},
		OnCoalescedSaved:    func() { saved.Add(1) },
		OnCoalescedFallback: func() { fallbacks.Add(1) },
	})
	defer h.Close(context.Background())

	ctx := testCtx(fasthttp.MethodGet, "http://test/shielded")
	serveRequest(h, ctx)
	require.Equal(t, 200, respCode(ctx))
	assert.EqualValues(t, 1, fallbacks.Load(), "the failed wait must count as a fallback")
	assert.EqualValues(t, 0, saved.Load())
}

// TestCoalesceTimeoutBelowFetchBudget pins the cross-package invariant:
// the coalesced wait must stay below the origin fetch budget so a
// waiter that gives up can still run its own fetch in time.
func TestCoalesceTimeoutBelowFetchBudget(t *testing.T) {
	assert.Positive(t, defaultFetchTimeout-api.CoalesceFetchTimeout,
		"api.CoalesceFetchTimeout must stay strictly below the origin fetch budget (defaultFetchTimeout)")
	assert.Equal(t, 30*time.Second, api.CoalesceFetchTimeout)
}
