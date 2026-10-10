package cache

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/origin"
	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// fwdRecord is a snapshot of the client-identity headers one origin
// request carried.
type fwdRecord struct {
	xff, xfp, xfh, via string
}

// fwdCapture records the forwarded headers of every request the fake
// origin received.
type fwdCapture struct {
	mu   sync.Mutex
	recs []fwdRecord
}

func (c *fwdCapture) add(ctx *fasthttp.RequestCtx) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recs = append(c.recs, fwdRecord{
		xff: string(ctx.Request.Header.Peek(header.XForwardedFor)),
		xfp: string(ctx.Request.Header.Peek(header.XForwardedProto)),
		xfh: string(ctx.Request.Header.Peek(header.XForwardedHost)),
		via: string(ctx.Request.Header.Peek(header.Via)),
	})
}

func (c *fwdCapture) records() []fwdRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]fwdRecord(nil), c.recs...)
}

// fwdOrigin returns a cacheable origin that records the forwarded
// headers of every request.
func fwdOrigin(cap *fwdCapture) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		cap.add(ctx)
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.ETag, `"fwd-v1"`)
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}
}

// fwdHandler builds a Handler with the given forwarded policy wired to a
// recording origin.
func fwdHandler(t *testing.T, policy ForwardedPolicy, origin fasthttp.RequestHandler) (*Handler, *fwdCapture) {
	t.Helper()
	cap := &fwdCapture{}
	if origin == nil {
		origin = fwdOrigin(cap)
	}
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	h := NewHandler(HandlerConfig{
		FastClient: &testFastClient{handler: origin},
		Store:      store,
		Forwarded:  policy,
	})
	t.Cleanup(func() { _ = h.Close(context.Background()) })
	return h, cap
}

// fullForwarded is the on-policy used by most tests: every header on,
// the config default chain cap.
func fullForwarded() ForwardedPolicy {
	return ForwardedPolicy{ClientIP: true, Proto: true, Host: true, Via: true, MaxAppend: defaultForwardedMaxAppend}
}

// fwdCtx builds a GET request against the recording origin with the
// peer address bouine would see (the edge) and, optionally, spoofed
// client-supplied forwarded headers.
func fwdCtx(url string) *fasthttp.RequestCtx {
	ctx := testCtx("GET", url)
	ctx.Request.Header.SetHost("test")
	ctx.SetRemoteAddr(&net.TCPAddr{IP: net.ParseIP("198.51.100.7"), Port: 4242})
	return ctx
}

// TestForwarded_MissAppendsAndSets pins the core contract on the miss
// path: X-Forwarded-For is appended to (the spoofed client chain is
// carried verbatim, bouine's peer address added at the right end),
// X-Forwarded-Proto and X-Forwarded-Host are set (replacing spoofed
// values — bouine is authoritative about what it received), and Via
// carries the bouine hop.
func TestForwarded_MissAppendsAndSets(t *testing.T) {
	t.Parallel()
	h, cap := fwdHandler(t, fullForwarded(), nil)

	ctx := fwdCtx("/miss")
	ctx.Request.Header.Set(header.XForwardedFor, "203.0.113.9")
	ctx.Request.Header.Set(header.XForwardedProto, "https")
	ctx.Request.Header.Set(header.XForwardedHost, "spoof.example")
	h.ServeRequest(ctx)

	require.Equal(t, "MISS", respHeader(ctx, header.XCache))
	recs := cap.records()
	require.Len(t, recs, 1)
	assert.Equal(t, "203.0.113.9, 198.51.100.7", recs[0].xff,
		"XFF must append the immediate peer (port stripped) to the carried chain")
	assert.Equal(t, "http", recs[0].xfp, "XFP must be replaced with the received scheme")
	assert.Equal(t, "test", recs[0].xfh, "XFH must be replaced with the received Host")
	assert.Equal(t, "1.1 bouine", recs[0].via)
}

// TestForwarded_EmptyExistingChain pins the no-edge case: no
// client-supplied XFF yields a single-entry chain.
func TestForwarded_EmptyExistingChain(t *testing.T) {
	t.Parallel()
	h, cap := fwdHandler(t, fullForwarded(), nil)

	h.ServeRequest(fwdCtx("/empty"))

	recs := cap.records()
	require.Len(t, recs, 1)
	assert.Equal(t, "198.51.100.7", recs[0].xff)
}

// TestForwarded_MaxAppendCapsChain pins the entry cap: an 8-entry
// spoofed chain keeps only the rightmost maxAppend-1 entries plus
// bouine's peer.
func TestForwarded_MaxAppendCapsChain(t *testing.T) {
	t.Parallel()
	h, cap := fwdHandler(t, ForwardedPolicy{ClientIP: true, MaxAppend: 5}, nil)

	ctx := fwdCtx("/cap")
	ctx.Request.Header.Set(header.XForwardedFor,
		"1.1.1.1, 2.2.2.2, 3.3.3.3, 4.4.4.4, 5.5.5.5, 6.6.6.6, 7.7.7.7, 8.8.8.8")
	h.ServeRequest(ctx)

	recs := cap.records()
	require.Len(t, recs, 1)
	assert.Equal(t, "5.5.5.5, 6.6.6.6, 7.7.7.7, 8.8.8.8, 198.51.100.7", recs[0].xff)
}

// TestForwarded_MaxAppendOneReplacesChain pins max_append: 1 — only
// bouine's own peer entry survives.
func TestForwarded_MaxAppendOneReplacesChain(t *testing.T) {
	t.Parallel()
	h, cap := fwdHandler(t, ForwardedPolicy{ClientIP: true, MaxAppend: 1}, nil)

	ctx := fwdCtx("/cap1")
	ctx.Request.Header.Set(header.XForwardedFor, "1.1.1.1, 2.2.2.2")
	h.ServeRequest(ctx)

	recs := cap.records()
	require.Len(t, recs, 1)
	assert.Equal(t, "198.51.100.7", recs[0].xff)
}

// TestForwarded_AppendJoinsMultipleLines pins that a client sending the
// chain as repeated header lines still gets one joined, capped chain.
func TestForwarded_AppendJoinsMultipleLines(t *testing.T) {
	t.Parallel()
	var got string
	origin := func(ctx *fasthttp.RequestCtx) {
		got = string(ctx.Request.Header.Peek(header.XForwardedFor))
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
	}
	h, _ := fwdHandler(t, ForwardedPolicy{ClientIP: true, MaxAppend: 5}, origin)

	ctx := fwdCtx("/lines")
	ctx.Request.Header.Add(header.XForwardedFor, "1.1.1.1")
	ctx.Request.Header.Add(header.XForwardedFor, "2.2.2.2")
	h.ServeRequest(ctx)

	assert.Equal(t, "1.1.1.1, 2.2.2.2, 198.51.100.7", got)
}

// TestForwarded_ByteCapDropsOversizedEntries pins the 8 KiB per-header
// budget (threat-model T37): entries are dropped oldest-first until the
// header fits; bouine's own entry always survives.
func TestForwarded_ByteCapDropsOversizedEntries(t *testing.T) {
	t.Parallel()
	hdr := &fasthttp.RequestHeader{}
	junk := strings.Repeat("x", forwardedMaxHeaderBytes+512)
	hdr.Set(header.XForwardedFor, junk)
	hdr.Add(header.XForwardedFor, "203.0.113.9")

	appendForwardedValue(hdr, header.XForwardedFor, "198.51.100.7", 64)

	got := string(hdr.Peek(header.XForwardedFor))
	assert.LessOrEqual(t, len(got), forwardedMaxHeaderBytes,
		"the joined chain must stay inside the per-header budget")
	assert.Equal(t, "203.0.113.9, 198.51.100.7", got,
		"the oversized spoofed entry is dropped, the sane entries kept")
}

// TestForwarded_NoKeyParticipation pins threat-model T06: neither the
// client-supplied forwarded headers nor bouine's peer address fragment
// the cache key — two requests differing only in identity context share
// one entry (second is a HIT, no second origin fetch).
func TestForwarded_NoKeyParticipation(t *testing.T) {
	t.Parallel()
	h, cap := fwdHandler(t, fullForwarded(), nil)

	first := fwdCtx("/shared")
	first.Request.Header.Set(header.XForwardedFor, "203.0.113.9")
	h.ServeRequest(first)
	require.Equal(t, "MISS", respHeader(first, header.XCache))

	second := fwdCtx("/shared")
	second.Request.Header.Set(header.XForwardedFor, "198.18.0.99")
	second.SetRemoteAddr(&net.TCPAddr{IP: net.ParseIP("198.51.100.99")})
	h.ServeRequest(second)

	assert.Equal(t, "HIT", respHeader(second, header.XCache),
		"identity context must not fragment the cache key")
	assert.Len(t, cap.records(), 1, "no second origin fetch")
}

// TestForwarded_HitPathUntouched pins the hit-path contract: after
// warm-up a hit adds no headers anywhere (no origin request exists) and
// the client response carries no forwarded artifacts.
func TestForwarded_HitPathUntouched(t *testing.T) {
	t.Parallel()
	h, cap := fwdHandler(t, fullForwarded(), nil)

	h.ServeRequest(fwdCtx("/warm"))

	hit := fwdCtx("/warm")
	h.ServeRequest(hit)

	require.Equal(t, "HIT", respHeader(hit, header.XCache))
	assert.Len(t, cap.records(), 1, "the hit must not reach the origin")
	for _, name := range []string{header.XForwardedFor, header.XForwardedProto, header.XForwardedHost, header.Via} {
		assert.Empty(t, respHeader(hit, name), "hit response must not carry %s", name)
	}
}

// TestForwarded_BypassPathInjects covers the BYPASS path (no-cache
// request → streamBypass → doFetchStream).
func TestForwarded_BypassPathInjects(t *testing.T) {
	t.Parallel()
	h, cap := fwdHandler(t, fullForwarded(), nil)

	ctx := fwdCtx("/bypass")
	ctx.Request.Header.Set(header.CacheControl, "no-store")
	h.ServeRequest(ctx)

	require.Equal(t, "BYPASS", respHeader(ctx, header.XCache))
	recs := cap.records()
	require.Len(t, recs, 1)
	assert.Equal(t, "198.51.100.7", recs[0].xff)
	assert.Equal(t, "http", recs[0].xfp)
	assert.Equal(t, "test", recs[0].xfh)
	assert.Equal(t, "1.1 bouine", recs[0].via)
}

// TestForwarded_InvalidatingProxyPathInjects covers the POST
// invalidating-proxy path (invalidateAndProxy).
func TestForwarded_InvalidatingProxyPathInjects(t *testing.T) {
	t.Parallel()
	h, cap := fwdHandler(t, fullForwarded(), nil)

	ctx := fwdCtx("/invalidate")
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetBody([]byte("payload"))
	h.ServeRequest(ctx)

	recs := cap.records()
	require.Len(t, recs, 1)
	assert.Equal(t, "198.51.100.7", recs[0].xff)
	assert.Equal(t, "http", recs[0].xfp)
	assert.Equal(t, "test", recs[0].xfh)
	assert.Equal(t, "1.1 bouine", recs[0].via)
}

// TestForwarded_RevalidatePathInjects covers the foreground
// revalidation path (stale hit → conditional request): the revalidation
// carries the same injected identity as the original miss.
func TestForwarded_RevalidatePathInjects(t *testing.T) {
	t.Parallel()
	cap := &fwdCapture{}
	origin := func(ctx *fasthttp.RequestCtx) {
		cap.add(ctx)
		if string(ctx.Request.Header.Peek(header.IfNoneMatch)) != "" {
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			ctx.SetStatusCode(304)
			return
		}
		ctx.Response.Header.Set(header.CacheControl, "max-age=0, must-revalidate")
		ctx.Response.Header.Set(header.ETag, `"fwd-v1"`)
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}
	h, _ := fwdHandler(t, fullForwarded(), origin)

	ctx := fwdCtx("/reval")
	ctx.Request.Header.Set(header.XForwardedFor, "203.0.113.9")
	h.ServeRequest(ctx)
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))

	ctx = fwdCtx("/reval")
	ctx.Request.Header.Set(header.XForwardedFor, "203.0.113.9")
	h.ServeRequest(ctx)
	assert.Equal(t, "REVALIDATED", respHeader(ctx, header.XCache))

	recs := cap.records()
	require.Len(t, recs, 2)
	for i, rec := range recs {
		assert.Equal(t, "203.0.113.9, 198.51.100.7", rec.xff, "request %d", i)
		assert.Equal(t, "http", rec.xfp, "request %d", i)
		assert.Equal(t, "test", rec.xfh, "request %d", i)
		assert.Equal(t, "1.1 bouine", rec.via, "request %d", i)
	}
}

// TestForwarded_BgRevalidateInjectsWithoutClientIP covers the SWR
// background revalidation path (doBackgroundRevalidate): proto, host,
// and Via are injected from the captured request info, but
// X-Forwarded-For is NOT appended — there is no live peer for a
// background fetch, and replaying the original requester's address
// would attribute one user's identity to an anonymous refresh.
func TestForwarded_BgRevalidateInjectsWithoutClientIP(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cap := &fwdCapture{}
		origin := func(ctx *fasthttp.RequestCtx) {
			cap.add(ctx)
			if string(ctx.Request.Header.Peek(header.IfNoneMatch)) != "" {
				ctx.Response.Header.Set(header.CacheControl, "max-age=120")
				ctx.SetStatusCode(304)
				return
			}
			ctx.Response.Header.Set(header.CacheControl, "max-age=60, stale-while-revalidate=3600")
			ctx.Response.Header.Set(header.ETag, `"fwd-v1"`)
			ctx.SetStatusCode(200)
			_, _ = ctx.Write([]byte("body"))
		}
		h, _ := fwdHandler(t, fullForwarded(), origin)

		ctx := fwdCtx("/swr")
		ctx.Request.Header.Set(header.XForwardedFor, "203.0.113.9")
		h.ServeRequest(ctx)
		require.Equal(t, "MISS", respHeader(ctx, header.XCache))

		time.Sleep(61 * time.Second)
		ctx = fwdCtx("/swr")
		ctx.Request.Header.Set(header.XForwardedFor, "203.0.113.9")
		h.ServeRequest(ctx)
		require.Equal(t, "STALE", respHeader(ctx, header.XCache))

		synctest.Wait()
		recs := cap.records()
		require.Len(t, recs, 2)
		assert.Equal(t, "203.0.113.9, 198.51.100.7", recs[0].xff, "the miss appends the peer")
		assert.Equal(t, "203.0.113.9", recs[1].xff,
			"the background revalidation carries the original client chain without a peer append")
		assert.Equal(t, "http", recs[1].xfp)
		assert.Equal(t, "test", recs[1].xfh)
		assert.Equal(t, "1.1 bouine", recs[1].via)
	})
}

// TestForwarded_HedgedFetchDoesNotDoubleAppend pins the hedged-fetch
// contract: the hedge duplicate is a CopyTo clone of the already-built
// origin request, so each attempt carries exactly one peer entry — the
// XFF chain is never double-appended. Uses a real origin pool with
// hedge_timeout against a slow recording origin so both attempts fire.
func TestForwarded_HedgedFetchDoesNotDoubleAppend(t *testing.T) {
	t.Parallel()
	cap := &fwdCapture{}
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		time.Sleep(50 * time.Millisecond)
		cap.add(ctx)
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	})
	defer srv.Close()

	p, err := origin.NewPool(origin.PoolConfig{
		Name:         "fwd-hedge",
		Targets:      []string{srv.Addr},
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		HedgeTimeout: 10 * time.Millisecond,
	})
	require.NoError(t, err)

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	defer store.Close(context.Background())
	h := NewHandler(HandlerConfig{
		FastClient: p.FastClient(),
		Store:      store,
		Forwarded:  fullForwarded(),
	})
	defer h.Close(context.Background())

	ctx := fwdCtx("/hedge")
	ctx.Request.Header.Set(header.XForwardedFor, "203.0.113.9")
	h.ServeRequest(ctx)
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))

	require.Eventually(t, func() bool {
		return len(cap.records()) == 2
	}, 2*time.Second, 10*time.Millisecond, "the slow primary must fire the hedge duplicate")

	for i, rec := range cap.records() {
		assert.Equal(t, "203.0.113.9, 198.51.100.7", rec.xff,
			"attempt %d must carry the peer address exactly once — no double append", i)
	}
}

// TestForwarded_DisabledIsNoOp pins the default: routes without
// request.forwarded forward client-supplied identity headers verbatim
// and add nothing.
func TestForwarded_DisabledIsNoOp(t *testing.T) {
	t.Parallel()
	h, cap := fwdHandler(t, ForwardedPolicy{}, nil)

	ctx := fwdCtx("/default")
	ctx.Request.Header.Set(header.XForwardedFor, "203.0.113.9")
	ctx.Request.Header.Set(header.XForwardedProto, "spoofed")
	h.ServeRequest(ctx)

	recs := cap.records()
	require.Len(t, recs, 1)
	assert.Equal(t, "203.0.113.9", recs[0].xff, "carried verbatim, nothing appended")
	assert.Equal(t, "spoofed", recs[0].xfp, "carried verbatim, nothing replaced")
	assert.Empty(t, recs[0].via)
}

// TestForwarded_PeerIP pins the peer-address extraction: TCP addresses
// yield the host part (port dropped), non-TCP addresses fall back to
// SplitHostPort, nil yields empty.
func TestForwarded_PeerIP(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "198.51.100.7", peerIP(&net.TCPAddr{IP: net.ParseIP("198.51.100.7"), Port: 4242}))
	assert.Equal(t, "2001:db8::1", peerIP(&net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 4242}))
	assert.Equal(t, "203.0.113.5", peerIP(&net.TCPAddr{IP: net.ParseIP("203.0.113.5")}), "portless TCP addr")
	assert.Equal(t, "198.51.100.8", peerIP(addrFunc(func() string { return "198.51.100.8:9999" })))
	assert.Equal(t, "", peerIP(nil))
}

// addrFunc is a minimal net.Addr for the non-TCP fallback branch.
type addrFunc func() string

func (f addrFunc) Network() string { return "custom" }
func (f addrFunc) String() string  { return f() }

// TestForwarded_InfoTLSAndHost pins applyForwardedInfo directly: proto
// comes from the captured TLS flag, host from the captured RequestInfo,
// Via is appended, and no XFF is ever appended (no live peer).
func TestForwarded_InfoTLSAndHost(t *testing.T) {
	t.Parallel()
	h, _ := fwdHandler(t, ForwardedPolicy{Proto: true, Host: true, Via: true, MaxAppend: 5}, nil)

	for _, tc := range []struct {
		tls  bool
		xfp  string
		host string
	}{
		{false, "http", "plain.example"},
		{true, "https", "secure.example"},
	} {
		hdr := &fasthttp.RequestHeader{}
		ri := RequestInfo{Host: tc.host, TLS: tc.tls}
		h.applyForwardedInfo(hdr, ri)
		assert.Equal(t, tc.xfp, string(hdr.Peek(header.XForwardedProto)))
		assert.Equal(t, tc.host, string(hdr.Peek(header.XForwardedHost)))
		assert.Equal(t, "1.1 bouine", string(hdr.Peek(header.Via)))
		assert.Empty(t, string(hdr.Peek(header.XForwardedFor)),
			"background fetches never append a client address")
	}
}
