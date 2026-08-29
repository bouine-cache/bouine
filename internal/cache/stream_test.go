package cache

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

func TestStreamBypass_ResponseBodyStreamed(t *testing.T) {
	t.Parallel()
	body := []byte("streamed bypass body")
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "no-store")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write(body)
	}
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  4 << 20,
			NumShards: 2,
		}),
	})

	ctx := testCtx("GET", "http://example.com/bypass")
	serveRequest(h, ctx)

	require.Equal(t, 200, respCode(ctx))
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))
	require.Equal(t, string(body), respBody(ctx))
}

func TestStreamMiss_CacheableResponseBodyStreamed(t *testing.T) {
	t.Parallel()
	body := []byte("streamed miss body")
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write(body)
	}
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  4 << 20,
			NumShards: 2,
		}),
	})

	ctx := testCtx("GET", "http://example.com/stream-miss")
	serveRequest(h, ctx)

	require.Equal(t, 200, respCode(ctx))
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))
	require.Equal(t, string(body), respBody(ctx))

	// Second request should be a cache hit.
	ctx2 := testCtx("GET", "http://example.com/stream-miss")
	serveRequest(h, ctx2)
	require.Equal(t, "HIT", respHeader(ctx2, header.XCache))
	require.Equal(t, string(body), respBody(ctx2))
}

func TestStreamMiss_NonCacheableStreamedNotStored(t *testing.T) {
	t.Parallel()
	body := []byte("non-cacheable streamed body")
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "no-store")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write(body)
	}
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  4 << 20,
			NumShards: 2,
		}),
	})

	ctx := testCtx("GET", "http://example.com/non-cacheable")
	serveRequest(h, ctx)

	require.Equal(t, 200, respCode(ctx))
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))
	require.Equal(t, string(body), respBody(ctx))

	// Second request should also be a MISS (not cached).
	ctx2 := testCtx("GET", "http://example.com/non-cacheable")
	serveRequest(h, ctx2)
	require.Equal(t, "MISS", respHeader(ctx2, header.XCache))
}

func TestStreamMiss_VaryStoredCorrectly(t *testing.T) {
	t.Parallel()
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.Response.Header.Set(header.Vary, "X-Test-Variant")
		_, _ = ctx.Write([]byte("variant-" + string(ctx.Request.Header.Peek("X-Test-Variant"))))
	}
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  4 << 20,
			NumShards: 2,
		}),
	})

	// First variant.
	ctxA := testCtx("GET", "http://example.com/vary-stream")
	ctxA.Request.Header.Set("X-Test-Variant", "a")
	serveRequest(h, ctxA)
	require.Equal(t, "MISS", respHeader(ctxA, header.XCache))
	require.Equal(t, "variant-a", respBody(ctxA))

	// Second variant.
	ctxB := testCtx("GET", "http://example.com/vary-stream")
	ctxB.Request.Header.Set("X-Test-Variant", "b")
	serveRequest(h, ctxB)
	require.Equal(t, "MISS", respHeader(ctxB, header.XCache))
	require.Equal(t, "variant-b", respBody(ctxB))

	// First variant should be a hit.
	ctxA2 := testCtx("GET", "http://example.com/vary-stream")
	ctxA2.Request.Header.Set("X-Test-Variant", "a")
	serveRequest(h, ctxA2)
	require.Equal(t, "HIT", respHeader(ctxA2, header.XCache))
	require.Equal(t, "variant-a", respBody(ctxA2))
}

func TestStreamMiss_MaxResponseBytesExceeded(t *testing.T) {
	t.Parallel()
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write(make([]byte, 10<<20))
	}
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  1 << 30,
			NumShards: 2,
		}),
		MaxResponseBytes: 1 << 20, // 1 MiB
	})

	ctx := testCtx("GET", "http://example.com/too-large")
	serveRequest(h, ctx)

	require.Equal(t, 502, respCode(ctx))
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))
}

func TestStreamMiss_InflightDedup(t *testing.T) {
	t.Parallel()
	body := []byte("dedup body")
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write(body)
	}
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  4 << 20,
			NumShards: 2,
		}),
	})

	// Two concurrent requests for the same key — the follower should
	// get the buffered result from the leader.
	ctx1 := testCtx("GET", "http://example.com/dedup")
	ctx2 := testCtx("GET", "http://example.com/dedup")

	serveRequest(h, ctx1)
	serveRequest(h, ctx2)

	require.Equal(t, 200, respCode(ctx1))
	require.Equal(t, string(body), respBody(ctx1))
	require.Equal(t, 200, respCode(ctx2))
	require.Equal(t, string(body), respBody(ctx2))
}

func TestStreamMiss_GlobalBufferCapStopsBuffering(t *testing.T) {
	t.Parallel()

	// Body is larger than the global cap so buffering must stop mid-stream.
	body := make([]byte, 64*1024)
	for i := range body {
		body[i] = byte(i % 256)
	}
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write(body)
	}

	var fallbackCount atomic.Int64
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &streamFastClient{handler: upstream},
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  4 << 20,
			NumShards: 2,
		}),
		StreamingFallback: &incCounter{n: &fallbackCount},
	})
	// Set a very low global cap (1 KiB) so the in-loop check triggers
	// after the first chunk. The response is 64 KiB, so the buffer cap
	// will be hit well before the body is fully read.
	h.maxStreamingBufferBytes = 1024

	ctx := testCtx("GET", "http://example.com/buffer-cap")
	serveRequest(h, ctx)

	// Client must still receive the full body.
	require.Equal(t, 200, respCode(ctx))
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))
	require.Equal(t, len(body), len(respBody(ctx)))
	require.Equal(t, body, []byte(respBody(ctx)))

	// The global cap must have triggered the fallback counter.
	require.GreaterOrEqual(t, fallbackCount.Load(), int64(1))

	// streamingBufferBytes must return to 0 after the stream completes.
	require.Equal(t, int64(0), h.streamingBufferBytes.Load())

	// Second request should still be a MISS — the response was not cached
	// because buffering was stopped by the global cap.
	ctx2 := testCtx("GET", "http://example.com/buffer-cap")
	serveRequest(h, ctx2)
	require.Equal(t, "MISS", respHeader(ctx2, header.XCache))
}

// streamFastClient is like testFastClient but sets the response body as
// a stream (via SetBodyStream) so that resp.IsBodyStream() returns true,
// exercising the tee streaming path instead of the buffered fallback.
type streamFastClient struct {
	handler fasthttp.RequestHandler
}

func (c *streamFastClient) Do(ctx context.Context, req *fasthttp.Request, resp *fasthttp.Response) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rctx := rctxPool.Get().(*fasthttp.RequestCtx)
	req.CopyTo(&rctx.Request)
	done := make(chan struct{}, 1)
	go func() {
		c.handler(rctx)
		done <- struct{}{}
	}()
	select {
	case <-done:
	case <-ctx.Done():
		rctx.Request.Reset()
		rctx.Response.Reset()
		rctx.ResetUserValues()
		rctxPool.Put(rctx)
		return ctx.Err()
	}
	// Copy headers and status, but set body as a stream without
	// Content-Length so the doFetchStream CL check is skipped and the
	// per-stream cap inside teeStreamToClient is exercised instead.
	rctx.Response.Header.CopyTo(&resp.Header)
	resp.SetStatusCode(rctx.Response.StatusCode())
	bodyBytes := append([]byte(nil), rctx.Response.Body()...)
	resp.SetBodyStream(bytes.NewReader(bodyBytes), -1)
	rctx.Request.Reset()
	rctx.Response.Reset()
	rctx.ResetUserValues()
	rctxPool.Put(rctx)
	return nil
}

// DoDeadline implements the deadline-based fetch path. The stream test
// client's handler returns immediately, so no deadline enforcement is
// needed; an already-passed deadline is rejected to preserve timeout
// semantics.
func (c *streamFastClient) DoDeadline(req *fasthttp.Request, resp *fasthttp.Response, deadline time.Time) error {
	if time.Until(deadline) <= 0 {
		return fasthttp.ErrTimeout
	}
	return c.Do(context.Background(), req, resp)
}

type incCounter struct {
	n *atomic.Int64
}

func (c *incCounter) Inc() {
	c.n.Add(1)
}

func TestStreamMiss_StreamingBufferBytesTrackedCorrectly(t *testing.T) {
	t.Parallel()

	// Body is small enough to stay under the cap, so it should be cached.
	body := []byte("tracked-buffer-body")
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write(body)
	}
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &streamFastClient{handler: upstream},
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  4 << 20,
			NumShards: 2,
		}),
	})

	ctx := testCtx("GET", "http://example.com/tracked")
	serveRequest(h, ctx)

	require.Equal(t, 200, respCode(ctx))
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))
	require.Equal(t, string(body), respBody(ctx))

	// After the stream completes, the counter must be back to 0.
	// This catches the original bug where the defer captured buf.Len()
	// at 0 and the counter never moved.
	require.Equal(t, int64(0), h.streamingBufferBytes.Load())

	// Second request should be a HIT — the body was cached because
	// the cap was not exceeded.
	ctx2 := testCtx("GET", "http://example.com/tracked")
	serveRequest(h, ctx2)
	require.Equal(t, "HIT", respHeader(ctx2, header.XCache))
	require.Equal(t, string(body), respBody(ctx2))
}

func TestStreamMiss_PerStreamCapStopsBuffering(t *testing.T) {
	t.Parallel()

	// Body is 64 KiB but per-stream cap is 1 KiB, so buffering must stop
	// after the first chunk. The global cap is set high so only the
	// per-stream cap triggers.
	body := make([]byte, 64*1024)
	for i := range body {
		body[i] = byte(i % 256)
	}
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write(body)
	}
	h := NewHandler(HandlerConfig{
		Upstream:         upstream,
		FastClient:       &streamFastClient{handler: upstream},
		MaxResponseBytes: 1 << 10, // 1 KiB per-stream cap
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  4 << 20,
			NumShards: 2,
		}),
	})
	h.maxStreamingBufferBytes = 1 << 30 // 1 GiB — effectively unlimited

	ctx := testCtx("GET", "http://example.com/per-stream-cap")
	serveRequest(h, ctx)

	// Client must receive the full body.
	require.Equal(t, 200, respCode(ctx))
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))
	require.Equal(t, len(body), len(respBody(ctx)))
	require.Equal(t, body, []byte(respBody(ctx)))

	// streamingBufferBytes must return to 0 after the stream completes.
	require.Equal(t, int64(0), h.streamingBufferBytes.Load())

	// Second request should be a MISS — the response was not cached
	// because the per-stream cap stopped buffering before the full body
	// was captured.
	ctx2 := testCtx("GET", "http://example.com/per-stream-cap")
	serveRequest(h, ctx2)
	require.Equal(t, "MISS", respHeader(ctx2, header.XCache))
}

// TestTakeResponseBody_GivesOwnership pins the body-ownership-transfer
// contract used by the transient (non-stored) fetch paths:
//  1. The returned slice carries the full body and remains valid after
//     the response is released.
//  2. The response is left with an empty body — the pooled response
//     returns to fasthttp's pool without pinning the body buffer.
//  3. Both body representations fasthttp's client read path produces
//     (pooled buffer and stream) are handled.
func TestTakeResponseBody_GivesOwnership(t *testing.T) {
	t.Parallel()

	const payload = "streamed-body-payload"
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString(payload)
	}

	// Pooled-buffer representation.
	resp := fasthttp.AcquireResponse()
	c := &benchFastClient{handler: upstream}
	if err := c.Do(context.Background(), fasthttp.AcquireRequest(), resp); err != nil {
		t.Fatal(err)
	}
	body := takeResponseBody(resp)
	require.Equal(t, payload, string(body))
	require.Empty(t, resp.Body(), "response must be empty after ownership transfer")
	fasthttp.ReleaseResponse(resp)
	// The taken slice must still be intact after release.
	require.Equal(t, payload, string(body))

	// bodyStream representation: SwapBody must drain the stream.
	resp2 := fasthttp.AcquireResponse()
	resp2.SetBodyStream(bytes.NewReader([]byte(payload)), -1)
	body2 := takeResponseBody(resp2)
	require.Equal(t, payload, string(body2))
	require.Empty(t, resp2.Body())
	fasthttp.ReleaseResponse(resp2)
}

// TestTakeResponseBody_NilResponse pins the nil guard.
func TestTakeResponseBody_NilResponse(t *testing.T) {
	t.Parallel()
	require.Nil(t, takeResponseBody(nil))
}

// TestStreamBypass_BufferedTakesOwnership verifies that a buffered
// BYPASS response serves the stolen buffer (no extra body copy) and
// releases the origin response without retaining the body.
func TestStreamBypass_BufferedTakesOwnership(t *testing.T) {
	t.Parallel()
	const payload = "bypass-body"
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "no-store")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString(payload)
	}
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  1 << 20,
			NumShards: 2,
		}),
	})

	ctx := testCtx("GET", "http://example.com/bypass-no-store")
	serveRequest(h, ctx)

	require.Equal(t, 200, respCode(ctx))
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))
	require.Equal(t, payload, respBody(ctx))
}

// TestStreamMissNonCacheable_ServesBody verifies the non-cacheable miss
// path (no-store origin response) still serves the full body via the
// ownership-transfer path, and singleflight followers see the same body.
func TestStreamMissNonCacheable_ServesBody(t *testing.T) {
	t.Parallel()
	const payload = "no-store-miss-body"
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "no-store")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString(payload)
	}
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &benchFastClient{handler: upstream},
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  1 << 20,
			NumShards: 2,
		}),
	})

	ctx := testCtx("GET", "http://example.com/no-store")
	serveRequest(h, ctx)

	require.Equal(t, 200, respCode(ctx))
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))
	require.Equal(t, payload, respBody(ctx))
}
