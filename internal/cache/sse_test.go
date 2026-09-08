package cache

import (
	"bufio"
	"bytes"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// sseHandler returns a streaming origin handler that emits SSE headers and
// the given event body. cacheable adds a Cache-Control freshness directive
// that would make the response storable without the SSE routing.
func sseHandler(events string, cacheable bool) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.ContentType, "text/event-stream; charset=utf-8")
		if cacheable {
			ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		}
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString(events)
	}
}

// newSSEHandler builds a Handler wired with a streaming test client (real
// body streams, like production transports).
func newSSEHandler(t *testing.T, origin fasthttp.RequestHandler) *Handler {
	t.Helper()
	return NewHandler(HandlerConfig{
		Upstream:   origin,
		FastClient: &streamFastClient{handler: origin},
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  4 << 20,
			NumShards: 2,
		}),
	})
}

const sseEvents = "event: message\ndata: {\"token\":\"hel\"}\n\n" +
	"event: message\ndata: {\"token\":\"lo\"}\n\n" +
	"event: done\ndata: [DONE]\n\n"

// TestSSE_HintedGet_StreamsUncached pins the core hinted-SSE contract: a
// request announcing Accept: text/event-stream is served as a live stream
// (BYPASS), never stored, and releases its fetch slot before the body
// streams — even when the origin claims the response is cacheable.
func TestSSE_HintedGet_StreamsUncached(t *testing.T) {
	t.Parallel()

	h := newSSEHandler(t, sseHandler(sseEvents, true))

	ctx := testCtx("GET", "http://example.com/chat")
	ctx.Request.Header.Set(header.Accept, "text/event-stream")
	serveRequest(h, ctx)

	require.Equal(t, 200, respCode(ctx))
	require.Equal(t, "BYPASS", respHeader(ctx, header.XCache))
	require.Equal(t, sseEvents, respBody(ctx))

	// The fetch slot must be released as soon as the headers are in hand:
	// a long-lived stream must not consume fetch concurrency.
	require.Equal(t, 0, len(h.fetchSem), "SSE stream must not hold a fetch slot")

	// Nothing stored: the second hinted request must fetch again.
	ctx2 := testCtx("GET", "http://example.com/chat")
	ctx2.Request.Header.Set(header.Accept, "text/event-stream")
	serveRequest(h, ctx2)
	require.Equal(t, "BYPASS", respHeader(ctx2, header.XCache))
	require.Equal(t, sseEvents, respBody(ctx2))

	// No tee buffer was ever allocated for the stream.
	require.Equal(t, int64(0), h.streamingBufferBytes.Load())
}

// TestSSE_HintedRequestNotCollapsed proves hinted SSE requests are excluded
// from request collapsing: each client must get its own origin stream (an
// event stream cannot be shared by buffering — followers would wait for a
// stream that never ends). The origin handler blocks until two concurrent
// fetches are in flight; a collapsed second request would observe only one.
func TestSSE_HintedRequestNotCollapsed(t *testing.T) {
	t.Parallel()

	var fetches atomic.Int64
	release := make(chan struct{})
	origin := func(ctx *fasthttp.RequestCtx) {
		if fetches.Add(1) == 1 {
			// Wait for the second concurrent fetch, with an escape so a
			// collapsed (broken) run fails on the fetch-count assertion
			// instead of hanging the suite.
			select {
			case <-release:
			case <-time.After(2 * time.Second):
			}
		}
		ctx.Response.Header.Set(header.ContentType, "text/event-stream")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("data: x\n\n")
	}
	h := newSSEHandler(t, origin)

	var wg sync.WaitGroup
	results := make(chan string, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := testCtx("GET", "http://example.com/feed")
			ctx.Request.Header.Set(header.Accept, "text/event-stream")
			serveRequest(h, ctx)
			results <- respBody(ctx)
		}()
	}
	wg.Wait()
	close(release)

	require.Equal(t, int64(2), fetches.Load(), "each SSE client must get its own origin fetch")
	for range 2 {
		require.Equal(t, "data: x\n\n", <-results)
	}
}

// TestSSE_HintedPost_StreamsAndPurges pins the POST SSE path (AI streaming
// APIs): the response streams instead of being buffered to EOF, the request
// body is forwarded to the origin, and the invalidation semantics are
// preserved — a cached GET representation of the URI is purged.
func TestSSE_HintedPost_StreamsAndPurges(t *testing.T) {
	t.Parallel()

	var gotBody atomic.Value
	origin := func(ctx *fasthttp.RequestCtx) {
		if string(ctx.Method()) == "POST" {
			gotBody.Store(string(ctx.Request.Body()))
			ctx.Response.Header.Set(header.ContentType, "text/event-stream")
			ctx.SetStatusCode(200)
			_, _ = ctx.WriteString(sseEvents)
			return
		}
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("cached-representation")
	}
	h := newSSEHandler(t, origin)

	// Seed the cache with a stored GET representation. The body must be
	// drained: the tee stream writer (which stores the object) runs during
	// the response write.
	seed := testCtx("GET", "http://example.com/v1/chat")
	serveRequest(h, seed)
	require.Equal(t, "MISS", respHeader(seed, header.XCache))
	require.Equal(t, "cached-representation", respBody(seed))

	// POST with SSE intent: the response must stream, not buffer.
	post := testCtxWithBody("POST", "http://example.com/v1/chat", []byte(`{"stream":true}`))
	post.Request.Header.Set(header.Accept, "text/event-stream")
	serveRequest(h, post)

	require.Equal(t, 200, respCode(post))
	require.Equal(t, "BYPASS", respHeader(post, header.XCache))
	require.Equal(t, sseEvents, respBody(post))
	require.Equal(t, `{"stream":true}`, gotBody.Load(), "POST body must reach the origin")

	// The stored GET representation must have been purged (RFC 9111
	// invalidation on successful POST).
	get := testCtx("GET", "http://example.com/v1/chat")
	serveRequest(h, get)
	require.Equal(t, "MISS", respHeader(get, header.XCache))
	require.Equal(t, "cached-representation", respBody(get))
}

// TestSSE_HintedNonStreamResponse pins the fallback: a hinted request whose
// origin answers with a finite non-stream response is still streamed
// through (same body, unbuffered) and never stored.
func TestSSE_HintedNonStreamResponse(t *testing.T) {
	t.Parallel()

	origin := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.ContentType, "application/json")
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString(`{"error":"stream unavailable"}`)
	}
	h := newSSEHandler(t, origin)

	ctx := testCtx("GET", "http://example.com/complete")
	ctx.Request.Header.Set(header.Accept, "text/event-stream")
	serveRequest(h, ctx)

	require.Equal(t, 200, respCode(ctx))
	require.Equal(t, "BYPASS", respHeader(ctx, header.XCache))
	require.Equal(t, `{"error":"stream unavailable"}`, respBody(ctx))

	ctx2 := testCtx("GET", "http://example.com/complete")
	serveRequest(h, ctx2)
	require.Equal(t, "MISS", respHeader(ctx2, header.XCache), "hinted responses are never stored")
}

// TestSSE_HintDoesNotAffectHits pins that SSE intent never changes stored
// hits: a cached response is complete and served normally, and the hit
// path pays no detection cost.
func TestSSE_HintDoesNotAffectHits(t *testing.T) {
	t.Parallel()

	origin := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("ordinary")
	}
	h := newSSEHandler(t, origin)

	fill := testCtx("GET", "http://example.com/doc")
	serveRequest(h, fill)
	require.Equal(t, "MISS", respHeader(fill, header.XCache))
	require.Equal(t, "ordinary", respBody(fill)) // drain: the store happens in the stream writer

	hit := testCtx("GET", "http://example.com/doc")
	hit.Request.Header.Set(header.Accept, "text/event-stream")
	serveRequest(h, hit)
	require.Equal(t, "HIT", respHeader(hit, header.XCache))
	require.Equal(t, "ordinary", respBody(hit))
}

// TestSSE_NonHintedOriginStream_StreamsUncached pins the best-effort path:
// an SSE response arriving WITHOUT the request hint is routed to the
// unbuffered stream path instead of the fully-buffered fallback (which
// would read the endless body to EOF and stall the request). It is never
// stored, even when the origin claims freshness.
func TestSSE_NonHintedOriginStream_StreamsUncached(t *testing.T) {
	t.Parallel()

	h := newSSEHandler(t, sseHandler(sseEvents, true))

	ctx := testCtx("GET", "http://example.com/feed")
	serveRequest(h, ctx)

	require.Equal(t, 200, respCode(ctx))
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))
	require.Equal(t, sseEvents, respBody(ctx))
	require.Equal(t, int64(0), h.streamingBufferBytes.Load(), "SSE must not tee-buffer")

	ctx2 := testCtx("GET", "http://example.com/feed")
	serveRequest(h, ctx2)
	require.Equal(t, "MISS", respHeader(ctx2, header.XCache), "SSE responses are never stored")
}

// TestSSE_NonHintedFollowersFetchOwnStream pins the singleflight contract
// for NON-hinted SSE: the leader streams unbuffered, so no body is ever
// buffered to share. Followers must be released at header time with
// ErrStreamUnshareable and fetch their own response — not wait for the
// leader's stream to end and then receive a 200 with an empty body (the
// bug this test guards against).
func TestSSE_NonHintedFollowersFetchOwnStream(t *testing.T) {
	t.Parallel()

	var fetches atomic.Int64
	origin := func(ctx *fasthttp.RequestCtx) {
		if fetches.Add(1) == 1 {
			// Hold the leader's fetch open briefly so the second request
			// reliably parks as a singleflight follower. Escape keeps a
			// broken run failing on assertions instead of hanging: with
			// the fix, the follower is released at header time — after
			// this handler returns — so the wait always ends via escape.
			time.Sleep(300 * time.Millisecond)
		}
		ctx.Response.Header.Set(header.ContentType, "text/event-stream")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("data: own-stream\n\n")
	}
	h := newSSEHandler(t, origin)

	var wg sync.WaitGroup
	bodies := make([]string, 2)
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := testCtx("GET", "http://example.com/follower-feed")
			serveRequest(h, ctx)
			bodies[i] = respBody(ctx)
		}()
	}
	wg.Wait()

	require.Equal(t, int64(2), fetches.Load(),
		"the follower must fetch its own response, not share the leader's unbuffered stream")
	for i, b := range bodies {
		require.Equal(t, "data: own-stream\n\n", b,
			"client %d must receive the full event body", i)
	}
}

// TestSSE_ShedReturns503 pins the fetch-shed mapping on the SSE path: when
// the fetch queue is full for fetchWaitTimeout, the client gets 503 +
// Retry-After, not a hung stream.
func TestSSE_ShedReturns503(t *testing.T) {
	t.Parallel()

	h := newSSEHandler(t, sseHandler(sseEvents, false))
	h.fetchWaitTimeout = 10 * time.Millisecond
	// Exhaust all fetch slots so the hinted fetch sheds.
	for range cap(h.fetchSem) {
		h.fetchSem <- struct{}{}
	}
	defer func() {
		for range cap(h.fetchSem) {
			<-h.fetchSem
		}
	}()

	ctx := testCtx("GET", "http://example.com/feed")
	ctx.Request.Header.Set(header.Accept, "text/event-stream")
	serveRequest(h, ctx)

	require.Equal(t, 503, respCode(ctx))
	require.Equal(t, "BYPASS", respHeader(ctx, header.XCache))
	require.Equal(t, "1", respHeader(ctx, header.RetryAfter))
}

// TestStreamCopyFlush_FlushesPerChunk pins the per-event delivery contract:
// every chunk written by the origin crosses the bufio writer immediately.
// Without the per-write flush, sub-buffer events accumulate in the 4 KiB
// bufio pipe buffer and the client sees them in multi-kilobyte batches.
func TestStreamCopyFlush_FlushesPerChunk(t *testing.T) {
	t.Parallel()

	var writes []int
	var mu sync.Mutex
	rec := &recordingWriter{onWrite: func(n int) {
		mu.Lock()
		writes = append(writes, n)
		mu.Unlock()
	}}
	bw := bufio.NewWriterSize(rec, 64*1024)

	r := io.MultiReader(
		bytes.NewReader([]byte("data: one\n\n")),
		bytes.NewReader([]byte("data: two\n\n")),
		bytes.NewReader([]byte("data: three\n\n")),
	)
	require.NoError(t, streamCopyFlush(bw, r))

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(writes), 3,
		"each event chunk must be flushed to the underlying writer immediately")
	require.Equal(t, "data: one\n\ndata: two\n\ndata: three\n\n", rec.buf.String())
}

// TestStreamCopyFlush_WriteErrorStopsCopy pins that a failed client write
// (disconnect) aborts the copy instead of draining the origin forever.
func TestStreamCopyFlush_WriteErrorStopsCopy(t *testing.T) {
	t.Parallel()

	failing := &failingWriter{}
	bw := bufio.NewWriterSize(failing, 16)

	var readCalls atomic.Int64
	r := &countingReader{inner: bytes.NewReader(bytes.Repeat([]byte("x"), 1024)), reads: &readCalls}

	err := streamCopyFlush(bw, r)
	require.Error(t, err)
	require.Less(t, readCalls.Load(), int64(1024),
		"copy must stop after the client write fails, not drain the origin")
}

type recordingWriter struct {
	buf     bytes.Buffer
	onWrite func(n int)
}

func (w *recordingWriter) Write(p []byte) (int, error) {
	n, err := w.buf.Write(p)
	if w.onWrite != nil {
		w.onWrite(n)
	}
	return n, err
}

type failingWriter struct{}

func (w *failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

type countingReader struct {
	inner io.Reader
	reads *atomic.Int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	r.reads.Add(1)
	return r.inner.Read(p)
}
