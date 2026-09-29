package cluster

import (
	"context"
	"errors"
	"math/big"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// shieldTestKey builds a deterministic key distinct per test name and
// returns it with its wire (hex) form.
func shieldTestKey(t *testing.T) api.Key {
	t.Helper()
	var key api.Key
	copy(key[:], t.Name())
	return key
}

// fwdCtx builds an admin-plane RequestCtx carrying a shield forward,
// with the mandatory control headers set the way
// PeerFetcher.ShieldForward sets them: the wire URI addresses the
// endpoint, the original target travels in the forward-URI header.
func fwdCtx(method string, key api.Key, mutate func(*fasthttp.RequestCtx)) *fasthttp.RequestCtx {
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(method)
	ctx.Request.SetRequestURI(PeerForwardPath)
	ctx.Request.Header.Set(header.XBouineDeadline, strconv.FormatInt(time.Now().Add(time.Hour).UnixNano(), 10))
	ctx.Request.Header.Set(header.XBouineShieldKey, key.Hex())
	ctx.Request.Header.Set(header.XBouineForwardURI, "/x")
	ctx.Request.Header.Set(BouineHopHeader, "1")
	if mutate != nil {
		mutate(ctx)
	}
	return ctx
}

// echoDataPlane is a data-plane stand-in: it asserts the replayed
// request's shape and answers 200 with the replay facts, including that
// no shield control header leaked into the replay.
func echoDataPlane(ctx *fasthttp.RequestCtx) {
	for _, h := range []string{header.XBouineDeadline, header.XBouineShieldKey, header.XBouineScheme, header.XBouineForwardURI, BouineHopHeader, ClusterVersionHeader} {
		if v := ctx.Request.Header.Peek(h); len(v) > 0 {
			ctx.Error("control header leaked into replay: "+h, fasthttp.StatusBadRequest)
			return
		}
	}
	ctx.Response.Header.Set("X-Forwarded-URI", string(ctx.RequestURI()))
	ctx.Response.Header.Set("X-Forwarded-Scheme", string(ctx.Request.URI().Scheme()))
	ctx.Response.Header.Set("X-Forwarded-TLS", map[bool]string{true: "true", false: "false"}[ctx.IsTLS()])
	ctx.Response.Header.Set("X-Forwarded-Rewrite", "applied")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.Response.SetBodyString("origin-fill")
}

func TestPeerForwardHandler_ServesOwner(t *testing.T) {
	t.Parallel()
	key := shieldTestKey(t)
	h := NewPeerForwardHandler(echoDataPlane, func(api.Key) bool { return true }, 0, 0, true, nil, nil)

	ctx := fwdCtx(fasthttp.MethodGet, key, func(c *fasthttp.RequestCtx) {
		c.Request.Header.Set(header.XBouineScheme, "http")
	})
	h.Handle(ctx)

	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
	assert.Equal(t, "/x", string(ctx.Response.Header.Peek("X-Forwarded-URI")))
	assert.Equal(t, "origin-fill", string(ctx.Response.Body()))
	assert.Equal(t, "applied", string(ctx.Response.Header.Peek("X-Forwarded-Rewrite")),
		"the owner's data plane runs the route rewrites — the forward must hit it verbatim")
	assert.Equal(t, "served", string(ctx.Response.Header.Peek(header.XBouineShieldResult)),
		"every replay-produced reply is marked served, whatever its status")
}

func TestPeerForwardHandler_RefusesNonOwner(t *testing.T) {
	t.Parallel()
	key := shieldTestKey(t)
	called := false
	h := NewPeerForwardHandler(func(*fasthttp.RequestCtx) { called = true },
		func(api.Key) bool { return false }, 0, 0, true, nil, nil)

	ctx := fwdCtx(fasthttp.MethodGet, key, nil)
	h.Handle(ctx)

	assert.Equal(t, fasthttp.StatusNotFound, ctx.Response.StatusCode(), "a non-owner answers 404 (D5)")
	assert.Equal(t, "refused", string(ctx.Response.Header.Peek(header.XBouineShieldResult)),
		"the refusal must be marked: an origin 404 relayed through the replay shares the status")
	assert.False(t, called, "the data plane must not run on a non-owner")
}

func TestPeerForwardHandler_FlagOffAnswers404(t *testing.T) {
	t.Parallel()
	key := shieldTestKey(t)
	h := NewPeerForwardHandler(echoDataPlane, nil, 0, 0, false, nil, nil)

	ctx := fwdCtx(fasthttp.MethodGet, key, nil)
	h.Handle(ctx)

	assert.Equal(t, fasthttp.StatusNotFound, ctx.Response.StatusCode(),
		"a flag-off node neither forwards nor serves forwards (D9)")
}

func TestPeerForwardHandler_MethodAndHopGates(t *testing.T) {
	t.Parallel()
	key := shieldTestKey(t)
	owner := func(api.Key) bool { return true }

	post := fwdCtx(fasthttp.MethodPost, key, nil)
	NewPeerForwardHandler(echoDataPlane, owner, 0, 0, true, nil, nil).Handle(post)
	assert.Equal(t, fasthttp.StatusMethodNotAllowed, post.Response.StatusCode(), "GET/HEAD only (D8)")

	// The key-only peer-fetch already consumed one hop; the forward is
	// hop 1 and a hopLimit of 1 refuses it (D5 loop bound).
	hop := fwdCtx(fasthttp.MethodGet, key, func(c *fasthttp.RequestCtx) {
		c.Request.Header.Set(BouineHopHeader, "1")
	})
	NewPeerForwardHandler(echoDataPlane, owner, 0, 1, true, nil, nil).Handle(hop)
	assert.Equal(t, fasthttp.StatusLoopDetected, hop.Response.StatusCode())
}

func TestPeerForwardHandler_MandatoryControlHeaders(t *testing.T) {
	t.Parallel()
	owner := func(api.Key) bool { return true }

	noDeadline := fwdCtx(fasthttp.MethodGet, shieldTestKey(t), func(c *fasthttp.RequestCtx) {
		c.Request.Header.Del(header.XBouineDeadline)
	})
	NewPeerForwardHandler(echoDataPlane, owner, 0, 0, true, nil, nil).Handle(noDeadline)
	assert.Equal(t, fasthttp.StatusBadRequest, noDeadline.Response.StatusCode(),
		"the deadline is mandatory (D4): an owner without a bound could outlive the requester")

	noKey := fwdCtx(fasthttp.MethodGet, api.Key{}, func(c *fasthttp.RequestCtx) {
		c.Request.Header.Del(header.XBouineShieldKey)
	})
	NewPeerForwardHandler(echoDataPlane, owner, 0, 0, true, nil, nil).Handle(noKey)
	assert.Equal(t, fasthttp.StatusBadRequest, noKey.Response.StatusCode(),
		"the shield key is mandatory (D5): the ownership gate cannot run without it")

	badKey := fwdCtx(fasthttp.MethodGet, api.Key{}, func(c *fasthttp.RequestCtx) {
		c.Request.Header.Set(header.XBouineShieldKey, "zz")
	})
	NewPeerForwardHandler(echoDataPlane, owner, 0, 0, true, nil, nil).Handle(badKey)
	assert.Equal(t, fasthttp.StatusBadRequest, badKey.Response.StatusCode())
}

func TestPeerForwardHandler_DeadlineClampAndExpiry(t *testing.T) {
	t.Parallel()
	key := shieldTestKey(t)
	owner := func(api.Key) bool { return true }

	// An already-expired deadline is refused without running the data
	// plane — the requester cannot use the answer anymore.
	past := fwdCtx(fasthttp.MethodGet, key, func(c *fasthttp.RequestCtx) {
		c.Request.Header.Set(header.XBouineDeadline, "1") // 1970-01-01
	})
	ran := false
	h := NewPeerForwardHandler(func(*fasthttp.RequestCtx) { ran = true }, owner, 0, 0, true, nil, nil)
	h.Handle(past)
	assert.Equal(t, fasthttp.StatusGatewayTimeout, past.Response.StatusCode())
	assert.False(t, ran)

	// parseShieldDeadline round-trips the absolute unix-nano form
	// ShieldForward sends.
	future := time.Now().Add(time.Hour).UnixNano()
	got, ok := parseShieldDeadline([]byte(strconv.FormatInt(future, 10)))
	require.True(t, ok)
	assert.Equal(t, future, got.UnixNano())
	_, ok = parseShieldDeadline([]byte("0"))
	assert.False(t, ok, "zero deadline is invalid")
	_, ok = parseShieldDeadline(nil)
	assert.False(t, ok, "absent deadline is invalid")
}

func TestPeerForwardHandler_HTTPSSchemeRestored(t *testing.T) {
	t.Parallel()
	key := shieldTestKey(t)
	h := NewPeerForwardHandler(echoDataPlane, func(api.Key) bool { return true }, 0, 0, true, nil, nil)

	ctx := fwdCtx(fasthttp.MethodGet, key, func(c *fasthttp.RequestCtx) {
		c.Request.Header.Set(header.XBouineScheme, "https")
	})
	h.Handle(ctx)

	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
	assert.Equal(t, "true", string(ctx.Response.Header.Peek("X-Forwarded-TLS")),
		"an https client request must be seen as TLS by the owner's replay (IsTLS feeds the cache key)")
	assert.Equal(t, "https", string(ctx.Response.Header.Peek("X-Forwarded-Scheme")))
}

func TestPeerForwardHandler_OwnerMetrics(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := RegisterMetrics(reg)
	key := shieldTestKey(t)

	served := NewPeerForwardHandler(echoDataPlane, func(api.Key) bool { return true }, 0, 0, true, nil, m)
	ctx := fwdCtx(fasthttp.MethodGet, key, func(c *fasthttp.RequestCtx) {
		c.Request.Header.Set(header.XBouineScheme, "http")
	})
	served.Handle(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())

	failed := NewPeerForwardHandler(fasthttp.RequestHandler(func(c *fasthttp.RequestCtx) {
		c.SetStatusCode(fasthttp.StatusServiceUnavailable)
	}), func(api.Key) bool { return true }, 0, 0, true, nil, m)
	failedCtx := fwdCtx(fasthttp.MethodGet, key, func(c *fasthttp.RequestCtx) {
		c.Request.Header.Set(header.XBouineScheme, "http")
	})
	failed.Handle(failedCtx)

	families, err := reg.Gather()
	require.NoError(t, err)
	counts := map[string]float64{}
	for _, f := range families {
		if f.GetName() != "bouine_shield_requests_total" {
			continue
		}
		for _, mt := range f.GetMetric() {
			for _, lp := range mt.GetLabel() {
				if lp.GetName() == "role" {
					counts[lp.GetValue()] = mt.GetCounter().GetValue()
				}
			}
		}
	}
	assert.Equal(t, 1.0, counts["owner"], "a 200 replay counts as served")
	assert.Equal(t, 1.0, counts["failure"],
		"a 5xx replay is the owner's miss path degrading — it must not hide inside the served count")
}

// shieldServer starts an in-process fasthttp server wrapping a forward
// handler and returns its address plus the fetcher-facing PeerInfo.
func shieldServer(t *testing.T, h *PeerForwardHandler) api.PeerInfo {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := &fasthttp.Server{Handler: h.Handle}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Shutdown()
		_ = ln.Close()
	})
	addr := ln.Addr().String()
	return api.PeerInfo{Name: "owner", Addr: addr, AdminAddr: addr}
}

func TestShieldForward_RequesterSide(t *testing.T) {
	t.Parallel()
	key := shieldTestKey(t)
	ownerPeer := shieldServer(t, NewPeerForwardHandler(echoDataPlane, func(api.Key) bool { return true }, 0, 0, true, nil, nil))

	f := NewPeerFetcherWithConfig(PeerFetcherConfig{}, nil, nil)
	t.Cleanup(func() { _ = f.Close(context.Background()) })

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI("/x")
	req.Header.SetHost("test")

	resp, err := f.ShieldForward(context.Background(), ownerPeer, req, key, false, time.Now().Add(5*time.Second))
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer fasthttp.ReleaseResponse(resp)
	assert.Equal(t, fasthttp.StatusOK, resp.StatusCode())
	assert.Equal(t, "origin-fill", string(resp.Body()))
	assert.Equal(t, "/x", string(resp.Header.Peek("X-Forwarded-Uri")))
}

func TestShieldForward_RefusedIsFallbackError(t *testing.T) {
	t.Parallel()
	key := shieldTestKey(t)
	nonOwnerPeer := shieldServer(t, NewPeerForwardHandler(echoDataPlane, func(api.Key) bool { return false }, 0, 0, true, nil, nil))

	f := NewPeerFetcherWithConfig(PeerFetcherConfig{}, nil, nil)
	t.Cleanup(func() { _ = f.Close(context.Background()) })

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI("/x")
	req.Header.SetHost("test")

	resp, err := f.ShieldForward(context.Background(), nonOwnerPeer, req, key, false, time.Now().Add(5*time.Second))
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.True(t, errors.Is(err, ErrShieldForward),
		"a refused forward is the origin-fallback signal, never a hard failure")
}

// bareServer answers like an endpoint-less old build: plain status,
// no shield result marker.
func bareServer(t *testing.T, status int) api.PeerInfo {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := &fasthttp.Server{Handler: func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(status)
	}}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Shutdown()
		_ = ln.Close()
	})
	return api.PeerInfo{Name: "bare", Addr: ln.Addr().String()}
}

// TestShieldForward_RelaysServedOriginStatus pins finding 1's fix: a
// replay-produced 404 (origin answer) is returned to the caller with
// its status intact — only the marker separates it from a refusal,
// which can share the status.
func TestShieldForward_RelaysServedOriginStatus(t *testing.T) {
	t.Parallel()
	key := shieldTestKey(t)
	notFound := fasthttp.RequestHandler(func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("Content-Type", "text/plain")
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		ctx.SetBodyString("origin says no")
	})
	peer := shieldServer(t, NewPeerForwardHandler(notFound, func(api.Key) bool { return true }, 0, 0, true, nil, nil))

	f := NewPeerFetcherWithConfig(PeerFetcherConfig{}, nil, nil)
	t.Cleanup(func() { _ = f.Close(context.Background()) })

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI("/x")
	req.Header.SetHost("test")

	resp, err := f.ShieldForward(context.Background(), peer, req, key, false, time.Now().Add(5*time.Second))
	require.NoError(t, err, "a served 404 is an origin answer, not a forward failure")
	require.NotNil(t, resp)
	defer fasthttp.ReleaseResponse(resp)
	assert.Equal(t, fasthttp.StatusNotFound, resp.StatusCode())
	assert.Equal(t, "origin says no", string(resp.Body()))
}

// TestShieldForward_UnmarkedReplyIsRefusal pins the mixed-version
// degrade: a reply without the result marker (endpoint-less old build,
// or a stray proxy answering for the owner) is the fallback signal,
// never content the client could be served.
func TestShieldForward_UnmarkedReplyIsRefusal(t *testing.T) {
	t.Parallel()
	key := shieldTestKey(t)
	peer := bareServer(t, fasthttp.StatusNotFound)

	f := NewPeerFetcherWithConfig(PeerFetcherConfig{}, nil, nil)
	t.Cleanup(func() { _ = f.Close(context.Background()) })

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI("/x")
	req.Header.SetHost("test")

	resp, err := f.ShieldForward(context.Background(), peer, req, key, false, time.Now().Add(5*time.Second))
	require.Error(t, err, "an unmarked 404 must not be relayed — it is not the shield's answer")
	assert.Nil(t, resp)
	assert.True(t, errors.Is(err, ErrShieldForward))
}

func TestShieldForward_ExpiredDeadlineIsError(t *testing.T) {
	t.Parallel()
	key := shieldTestKey(t)
	peer := shieldServer(t, NewPeerForwardHandler(echoDataPlane, nil, 0, 0, true, nil, nil))

	f := NewPeerFetcherWithConfig(PeerFetcherConfig{}, nil, nil)
	t.Cleanup(func() { _ = f.Close(context.Background()) })

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI("/x")
	req.Header.SetHost("test")

	resp, err := f.ShieldForward(context.Background(), peer, req, key, false, time.Now().Add(-time.Second))
	require.Error(t, err)
	assert.Nil(t, resp)
}

// TestShieldForward_LaneIsolation pins the two-lane contract: a shield
// forward in flight does not consume a key-only fetch slot — the slow
// fetch lane stays available for peer lookups (the reason the shield
// rides its own pipeline lane).
func TestShieldForward_LaneIsolation(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	var forwards atomic.Int64
	dataPlane := fasthttp.RequestHandler(func(ctx *fasthttp.RequestCtx) {
		forwards.Add(1)
		<-release
		ctx.SetStatusCode(fasthttp.StatusOK)
	})
	peer := shieldServer(t, NewPeerForwardHandler(dataPlane, func(api.Key) bool { return true }, 0, 0, true, nil, nil))

	f := NewPeerFetcherWithConfig(PeerFetcherConfig{FetchConcurrency: 1}, nil, nil)
	t.Cleanup(func() { _ = f.Close(context.Background()) })

	key := shieldTestKey(t)
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI("/x")
	req.Header.SetHost("test")

	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := f.ShieldForward(context.Background(), peer, req, key, false, time.Now().Add(5*time.Second))
		if err == nil {
			fasthttp.ReleaseResponse(resp)
		}
	}()

	// The forward is in flight (holding the shield lane); a key-only
	// Fetch to the same peer must not queue behind it. The peer serves
	// /v1/peer/fetch with 404 through its own fasthttp server here —
	// simulate a plain TCP refusal instead: the point is the semaphore,
	// and a refused dial completes immediately either way.
	require.Eventually(t, func() bool { return forwards.Load() > 0 }, 2*time.Second, 10*time.Millisecond)
	fetchStarted := make(chan error, 1)
	go func() {
		_, err := f.Fetch(context.Background(), peer, api.PeerFetchRequest{Key: key})
		fetchStarted <- err
	}()
	select {
	case err := <-fetchStarted:
		// Fetch may error (no fetch endpoint on this server) — the pin
		// is that it RETURNS rather than parking behind the forward.
		_ = err
	case <-time.After(2 * time.Second):
		t.Fatal("key-only fetch parked behind the in-flight shield forward — lanes are not isolated")
	}
	close(release)
	<-done
}

// parseShieldKey round-trip through api.Key.Hex.
func TestParseShieldKey_RoundTrip(t *testing.T) {
	t.Parallel()
	key := shieldTestKey(t)
	got, ok := parseShieldKey([]byte(key.Hex()))
	require.True(t, ok)
	assert.Equal(t, key, got)

	// The big.Int import stays exercised: guard against accidental
	// constant drift in the deadline parser's range checks.
	_, ok = parseShieldDeadline([]byte(new(big.Int).SetInt64(42).String()))
	assert.True(t, ok)
}
