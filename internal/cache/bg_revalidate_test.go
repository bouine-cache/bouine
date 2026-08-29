package cache

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// TestTriggerBgRevalidate_RequestCtxReset is the regression test for
// the background-revalidation buffer aliasing bug: triggerBgRevalidate
// captured a RequestInfo whose []byte fields alias the RequestCtx's
// internal buffers. The goroutine read them after the handler had
// returned and the next keep-alive request Reset the ctx — reading the
// next request's method/URI/host bytes.
//
// The test simulates the exact interleaving: serve the stale hit (which
// spawns the background revalidation), then mutate the same ctx to a
// *different* request (as fasthttp's worker loop does), then run the
// background goroutine to completion under synctest. The revalidation
// must use the ORIGINAL request, not the reset one.
func TestTriggerBgRevalidate_RequestCtxReset(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var gotMethod, gotURI, gotHost string
		revalStarted := make(chan struct{})
		origin := func(ctx *fasthttp.RequestCtx) {
			// The background revalidation sends a conditional request
			// (If-None-Match from the stale object's ETag).
			if string(ctx.Request.Header.Peek(header.IfNoneMatch)) != "" {
				gotMethod = string(ctx.Method())
				gotURI = string(ctx.RequestURI())
				gotHost = string(ctx.Host())
				close(revalStarted)
				ctx.Response.Header.Set(header.CacheControl, "max-age=120")
				ctx.SetStatusCode(304)
				return
			}
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			ctx.Response.Header.Set(header.ETag, `"v1"`)
			ctx.SetStatusCode(200)
			_, _ = ctx.Write([]byte("body"))
		}
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		defer store.Close(context.Background())
		h := NewHandler(HandlerConfig{
			Upstream:   origin,
			FastClient: &testFastClient{handler: origin},
			Store:      store,
		})
		defer h.Close(context.Background())

		key := BuildKey(requestInfoFromURL("GET", "http://swr.example.com/original"), nil)
		stale := &api.Object{
			Key:        key,
			StatusCode: 200,
			Header:     headerMap(header.CacheControl, "max-age=60", header.ETag, `"v1"`),
			Body:       []byte("body"),
			BodySize:   4,
			StoredAt:   time.Now().Add(-70 * time.Second),
			TTL:        60 * time.Second,
			ETag:       `"v1"`,
			// SWR window still open → ServeRequest spawns background
			// revalidation on the stale hit.
			StaleWhileRevalidate: 60 * time.Second,
		}
		_ = h.store.Put(context.Background(), key, stale)

		// Serve the stale hit. This returns after writing the response;
		// the revalidation goroutine is now running detached.
		rr := testCtx("GET", "http://swr.example.com/original")
		h.ServeRequest(rr)
		assert.Equal(t, "STALE", respHeader(rr, header.XCache))

		// Simulate fasthttp's worker loop recycling the connection for
		// the next request: the same ctx buffers now hold different data.
		rr.Request.Reset()
		rr.Request.Header.SetMethod("PUT")
		rr.Request.SetRequestURI("http://attacker.example.com/overwritten")
		rr.Request.Header.SetHost("attacker.example.com")

		// Drain the background revalidation. Under synctest this is
		// deterministic: sleep lets the goroutine run to completion.
		synctest.Sleep(200 * time.Millisecond)

		select {
		case <-revalStarted:
		default:
			t.Fatal("background revalidation never sent its conditional fetch")
		}
		// The revalidation must have used the ORIGINAL request captured
		// before the handler returned — not the recycled buffer content.
		require.Equal(t, "GET", gotMethod, "revalidation used stale method bytes")
		require.Equal(t, "http://swr.example.com/original", gotURI, "revalidation used stale URI bytes")
		require.Equal(t, "swr.example.com", gotHost, "revalidation used stale host bytes")

		// The refreshed object must have replaced the stale one.
		updated, _, _ := h.store.Get(context.Background(), key)
		require.NotNil(t, updated)
		assert.Equal(t, 120*time.Second, updated.TTL)
	})
}

// TestTriggerBgRevalidate_MaterializesStrings pins the contract
// directly: triggerBgRevalidate must not let the background goroutine
// read the caller's RequestCtx byte fields. The test corrupts the ctx
// immediately after the trigger and asserts the background fetch
// observes the original URI, not the corrupted one.
func TestTriggerBgRevalidate_MaterializesStrings(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var gotURI string
		revalStarted := make(chan struct{})
		origin := func(ctx *fasthttp.RequestCtx) {
			if string(ctx.Request.Header.Peek(header.IfNoneMatch)) != "" {
				gotURI = string(ctx.RequestURI())
				close(revalStarted)
				ctx.SetStatusCode(304)
				return
			}
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			ctx.Response.Header.Set(header.ETag, `"v1"`)
			ctx.SetStatusCode(200)
			_, _ = ctx.Write([]byte("body"))
		}
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		defer store.Close(context.Background())
		h := NewHandler(HandlerConfig{
			Upstream:   origin,
			FastClient: &testFastClient{handler: origin},
			Store:      store,
		})
		defer h.Close(context.Background())

		key := testkey.Key(4242)
		stale := &api.Object{
			Key:                  key,
			StatusCode:           200,
			Header:               headerMap(header.CacheControl, "max-age=60", header.ETag, `"v1"`),
			Body:                 []byte("old"),
			BodySize:             3,
			StoredAt:             time.Now().Add(-70 * time.Second),
			TTL:                  60 * time.Second,
			ETag:                 `"v1"`,
			StaleWhileRevalidate: 60 * time.Second,
		}
		_ = h.store.Put(context.Background(), key, stale)

		// Build the ctx and trigger; then immediately corrupt the ctx
		// buffers before letting the goroutine run.
		rr := testCtx("GET", "http://alias.example.com/aliased")
		ri := requestInfoFromCtx(rr)
		h.triggerBgRevalidate(ri, key, stale)

		rr.Request.Reset()
		rr.Request.Header.SetMethod("DELETE")
		rr.Request.SetRequestURI("http://corrupted.example.com/pwned")
		rr.Request.Header.SetHost("corrupted.example.com")

		synctest.Sleep(100 * time.Millisecond)
		select {
		case <-revalStarted:
		default:
			t.Fatal("background revalidation never sent its conditional fetch")
		}
		require.Equal(t, "http://alias.example.com/aliased", gotURI,
			"revalidation must use the URI captured at trigger time, not the recycled buffer")
	})
}
