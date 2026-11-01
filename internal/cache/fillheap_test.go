package cache

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

// gatedStore parks every store admission (Put and the owned-body Put
// variant) until the test releases it, so a test can assert what is —
// and is not — still held while the store attempt is in flight.
type gatedStore struct {
	storage.Store
	entered chan struct{}
	release chan struct{}
}

func newGatedStore(inner storage.Store) *gatedStore {
	return &gatedStore{
		Store:   inner,
		entered: make(chan struct{}, 8),
		release: make(chan struct{}),
	}
}

func (g *gatedStore) park() {
	// No ctx arm: storeObject threads the fasthttp RequestCtx here, and
	// its Done() panics on a test-built ctx; tests always close release.
	g.entered <- struct{}{}
	<-g.release
}

func (g *gatedStore) Put(ctx context.Context, key api.Key, obj *api.Object) error {
	g.park()
	return g.Store.Put(ctx, key, obj)
}

func (g *gatedStore) PutOwned(ctx context.Context, key api.Key, obj *api.Object) error {
	// Timing helper only: park exactly like Put, then store through the
	// cloning Put. The aliasing semantics of the real capability are
	// pinned against the real HotStore (TestTeeFill_StoresTheOwnedBody,
	// hot_putowned_test.go), not against this wrapper.
	g.park()
	return g.Store.Put(ctx, key, obj)
}

func (g *gatedStore) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("store admission never started: the fill did not reach the store")
	}
}

// TestTeeFill_StoresTheOwnedBody pins the store-fill ownership handoff:
// the tee already builds an exact-size, cache-owned copy of the origin
// body for the flight (followers share it read-only), so the store must
// take that copy as the stored body instead of heap-cloning it a second
// time. The rc1 5krps pprof run attributed 84.7% of inuse heap (479.86
// MB) to the second copy inside api.Object.CloneForStorage on this path.
func TestTeeFill_StoresTheOwnedBody(t *testing.T) {
	t.Parallel()

	var client bytes.Buffer
	w := bufio.NewWriter(&client)
	body := bytes.Repeat([]byte("the-owned-fill-body-"), 100)
	h, inflight, key := runTee(t, w, bytes.NewReader(body))
	_ = w.Flush()

	require.NoError(t, inflight.res.Err, "a clean-EOF fill publishes its body")
	require.Equal(t, body, inflight.res.Body)

	stored, _, err := h.store.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, body, stored.Body)
	require.True(t, &stored.Body[0] == &inflight.res.Body[0],
		"the tee's exact-size fill copy must become the stored body, not a second heap copy")
}

// TestTeeFill_FetchSlotReleasedBeforeStore pins the fetch-semaphore hold
// window on the tee fill: the slot bounds concurrent origin fetches and
// their tee buffers, so it must be back once the origin body reached EOF
// — before the settle/store tail. Holding it through the store admission
// (clone + inline SIEVE eviction under the shard lock) stretches slot
// hold exactly when TTL-expiry waves saturate the fetch queue and GC is
// churning on the fill heap — the rc1 knee turned that into 503 sheds.
func TestTeeFill_FetchSlotReleasedBeforeStore(t *testing.T) {
	t.Parallel()

	inner := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	gs := newGatedStore(inner)
	h := NewHandler(HandlerConfig{
		Upstream:   func(*fasthttp.RequestCtx) {},
		FastClient: &testFastClient{handler: func(*fasthttp.RequestCtx) {}},
		Store:      gs,
	})
	h.fetchSem = make(chan struct{}, 1)
	h.fetchWaitTimeout = 50 * time.Millisecond

	key := BuildKeyFromURL("http://example.com/tee-slot", nil)
	rh := &fasthttp.ResponseHeader{}
	rh.Set(header.CacheControl, "max-age=60")
	sf := &streamFetchResult{
		StatusCode: 200,
		Header:     fromFastHTTPHeader(rh),
		sem:        h.fetchSem,
	}
	h.fetchSem <- struct{}{} // the slot fetchStream would be holding

	inflight := newInflightStream()
	buf := streamBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	ri := requestInfoFromCtx(testCtx("GET", "http://example.com/tee-slot"))
	resMap := headerMap(header.CacheControl, "max-age=60")

	var client bytes.Buffer
	w := bufio.NewWriter(&client)
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.teeStreamToClient(w, bytes.NewReader([]byte("slot-proof-body")), buf, sf, inflight, key, key, ri, resMap, nil)
	}()

	gs.waitEntered(t) // the store admission is parked

	err := h.acquireFetchSlot()
	if err == nil {
		<-h.fetchSem // return the slot the probe just took
	}
	require.NoError(t, err,
		"the fetch slot must be back before the store admission parks: the origin body is complete")

	close(gs.release)
	<-done
	_ = w.Flush()

	stored, _, err := h.store.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, "slot-proof-body", string(stored.Body))
}

// TestBufferedFill_FetchSlotReleasedBeforeStore is the buffered-fallback
// twin of TestTeeFill_FetchSlotReleasedBeforeStore, driven through the
// full ServeRequest pipeline (the in-process test client answers with
// buffered responses, so cacheable fills take streamMissBuffered).
func TestBufferedFill_FetchSlotReleasedBeforeStore(t *testing.T) {
	t.Parallel()

	inner := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	gs := newGatedStore(inner)
	upstream := func(ctx *fasthttp.RequestCtx) {
		if bytes.Equal(ctx.Path(), []byte("/fill-b")) {
			// Uncacheable: this request must never touch the gated store.
			ctx.Response.Header.Set(header.CacheControl, "no-store")
			ctx.SetStatusCode(fasthttp.StatusOK)
			_, _ = ctx.WriteString("uncacheable-body")
			return
		}
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(fasthttp.StatusOK)
		_, _ = ctx.WriteString("buffered-fill-body")
	}
	h := NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store:      gs,
	})
	h.fetchSem = make(chan struct{}, 1)
	h.fetchWaitTimeout = 50 * time.Millisecond

	first := testCtx("GET", "http://example.com/fill-a")
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		h.ServeRequest(first)
	}()
	gs.waitEntered(t) // the first fill's store admission is parked

	// The second request's origin fetch must get the slot back while the
	// first fill's store attempt is still parked.
	second := testCtx("GET", "http://example.com/fill-b")
	h.ServeRequest(second)
	require.Equal(t, fasthttp.StatusOK, respCode(second),
		"the fetch slot must be back before the first fill's store attempt parks")
	require.Equal(t, "uncacheable-body", respBody(second))

	close(gs.release)
	<-firstDone

	stored, _, err := h.store.Get(context.Background(), BuildKeyFromURL("http://example.com/fill-a", nil))
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, "buffered-fill-body", string(stored.Body))
}

// TestTeeFill_ConcurrentPooledChunkIsolation pins the safety of pooling
// the tee's 32 KiB read scratch: concurrent fills with distinct bodies
// must never observe each other's chunk contents, in the client stream
// or in the stored bodies.
func TestTeeFill_ConcurrentPooledChunkIsolation(t *testing.T) {
	t.Parallel()

	h := testHandler(t, func(*fasthttp.RequestCtx) {})
	const fills = 16

	var wg sync.WaitGroup
	for i := range fills {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pattern := fmt.Sprintf("fill-%02d-", i)
			body := bytes.Repeat([]byte(pattern), 3000) // ~90 KB: several chunks

			key := BuildKeyFromURL(fmt.Sprintf("http://example.com/iso/%d", i), nil)
			rh := &fasthttp.ResponseHeader{}
			rh.Set(header.CacheControl, "max-age=60")
			sf := &streamFetchResult{
				StatusCode: 200,
				Header:     fromFastHTTPHeader(rh),
			}
			inflight := newInflightStream()
			buf := streamBufPool.Get().(*bytes.Buffer)
			buf.Reset()
			ri := requestInfoFromCtx(testCtx("GET", fmt.Sprintf("http://example.com/iso/%d", i)))
			resMap := headerMap(header.CacheControl, "max-age=60")

			var client bytes.Buffer
			w := bufio.NewWriter(&client)
			h.teeStreamToClient(w, bytes.NewReader(body), buf, sf, inflight, key, key, ri, resMap, nil)
			_ = w.Flush()

			require.NoError(t, inflight.res.Err)
			require.Equal(t, body, client.Bytes(),
				"concurrent fill %d: the client stream must carry this fill's body only", i)
			stored, _, err := h.store.Get(context.Background(), key)
			require.NoError(t, err)
			require.NotNil(t, stored)
			require.Equal(t, body, stored.Body,
				"concurrent fill %d: the stored body must carry this fill's body only", i)
		}(i)
	}
	wg.Wait()
}
