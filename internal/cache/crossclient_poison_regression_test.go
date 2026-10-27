package cache

// crossclient_poison_regression_test.go holds the regression tests for
// the cross-client response-leakage audit
// (docs/security/reviews/crossclient-response-leakage-audit.md) whose
// findings are NOT yet fixed. Each test is proven to fail on main and
// is quarantined behind t.Skip with a reference to its tracking issue;
// remove the Skip line when the corresponding fix lands and the test
// must then pass unchanged. The tests for the two FIXED findings
// (AUD-01 SWR identity replay, AUD-04 key/origin target parity) live
// with their fixes: swr_identity_regression_test.go and
// key_origin_parity_regression_test.go.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// TestWarmFlight_Leader304ServedToUnconditionalFollower proves finding
// AUD-02 (issue #848): on a warm byte-handoff flight (a stored no-cache
// object without validators makes every request a Miss that keeps the
// object in scope), the leader's conditional fetch can settle as a 304,
// and publishFillVerdict hands that 304 to every follower — including
// followers that never sent a conditional. An unconditional client
// receives a bodiless 304 it cannot use (RFC 9110 §15.4: a 304 is only
// meaningful in reply to a conditional request).
//
// Expected failure on main: the follower parks (fetches == 1) and is
// served status=304, body empty. The fix direction is in issue #848.
func TestWarmFlight_Leader304ServedToUnconditionalFollower(t *testing.T) {
	t.Skip("quarantined: known failure, tracked by #848 (audit AUD-02) — remove when the fix lands")
	t.Parallel()

	origin := func(ctx *fasthttp.RequestCtx) {
		if len(ctx.Request.Header.Peek(header.IfNoneMatch)) > 0 {
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			ctx.Response.Header.Set(header.ETag, `"v1"`)
			ctx.SetStatusCode(fasthttp.StatusNotModified)
			return
		}
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.ETag, `"v1"`)
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("shared")
	}

	// Stored object: response no-cache without validators makes every
	// request a Miss decision with the object in scope, and
	// staleFallbackAllowed is false (response no-cache), so the miss
	// lands in fetchAndStore's warm byte-handoff flight.
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	defer store.Close(context.Background())
	h := NewHandler(HandlerConfig{
		Upstream:   origin,
		FastClient: &testFastClient{handler: origin},
		Store:      store,
	})
	defer h.Close(context.Background())
	key := BuildKey(requestInfoFromURL("GET", "http://warm.example.com/doc"), nil)
	_ = h.store.Put(context.Background(), key, &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     headerMap(header.CacheControl, "no-cache, max-age=60"),
		Body:       []byte("old"),
		BodySize:   3,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		// Pre-parsed flags (evaluateFast reads these, not the Header
		// map): no-cache without validators makes every request a Miss
		// that keeps the object in scope.
		CacheControl: "no-cache, max-age=60",
		RespNoCache:  true,
	})

	var fetches atomic.Int64
	release := make(chan struct{})
	gated := fasthttp.RequestHandler(func(ctx *fasthttp.RequestCtx) {
		if fetches.Add(1) == 1 {
			select {
			case <-release:
			case <-time.After(2 * time.Second):
			}
		}
		origin(ctx)
	})
	h.fastClient = &testFastClient{handler: gated}

	// The leader sends If-None-Match; the origin answers 304.
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		lctx := testCtx("GET", "http://warm.example.com/doc")
		lctx.Request.Header.Set(header.IfNoneMatch, `"v1"`)
		h.ServeRequest(lctx)
	}()
	for fetches.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}

	// The follower is unconditional and parks on the leader's flight.
	followerDone := make(chan *fasthttp.RequestCtx, 1)
	go func() {
		fctx := testCtx("GET", "http://warm.example.com/doc")
		h.ServeRequest(fctx)
		followerDone <- fctx
	}()
	time.Sleep(50 * time.Millisecond) // let the follower park
	close(release)
	<-leaderDone

	select {
	case fctx := <-followerDone:
		// The follower never sent a conditional: it must receive the
		// full 200 representation, never the leader's 304.
		require.Equal(t, 200, respCode(fctx),
			"unconditional follower received the conditional leader's 304")
		require.Equal(t, "shared", respBody(fctx))
	case <-time.After(5 * time.Second):
		t.Fatal("follower did not terminate")
	}
}

// TestRangeMiss_StoresPartial206ServedToUnconditionalClients proves
// finding AUD-03 (issue #849): on a miss, the client's Range header is
// forwarded to the origin verbatim, a 206 partial response with
// explicit freshness passes the store gate (206 is heuristically
// cacheable), and the partial object is stored under the PRIMARY key
// (Range is not part of the key). Every later unconditional client is
// served the stored 206 partial as a HIT — a truncated body under a
// Content-Range header the client never asked for.
//
// Expected failure on main: client B is served HIT 206 "01234". The
// fix direction is in issue #849.
func TestRangeMiss_StoresPartial206ServedToUnconditionalClients(t *testing.T) {
	t.Skip("quarantined: known failure, tracked by #849 (audit AUD-03) — remove when the fix lands")
	t.Parallel()

	origin := func(ctx *fasthttp.RequestCtx) {
		if len(ctx.Request.Header.Peek(header.Range)) > 0 {
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			ctx.Response.Header.Set(header.ContentRange, "bytes 0-4/10")
			ctx.SetStatusCode(fasthttp.StatusPartialContent)
			_, _ = ctx.WriteString("01234")
			return
		}
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("0123456789")
	}
	h := testHandler(t, origin)

	// Client A asks for a range on a cold miss; it correctly receives
	// the 206 partial.
	a := testCtxWithHeader("GET", "http://example.com/doc.bin", header.Range, "bytes=0-4")
	serveRequest(h, a)
	require.Equal(t, fasthttp.StatusPartialContent, respCode(a))
	require.Equal(t, "01234", respBody(a))

	// Client B asks for the same URL unconditionally. It must receive
	// the full 200 representation — never the stored partial 206.
	b := testCtx("GET", "http://example.com/doc.bin")
	serveRequest(h, b)
	require.Equal(t, "HIT", respHeader(b, header.XCache))
	assert.Equal(t, 200, respCode(b),
		"unconditional client was served the stored 206 partial as a HIT")
	assert.Equal(t, "0123456789", respBody(b),
		"unconditional client was served the stored partial body")
}
