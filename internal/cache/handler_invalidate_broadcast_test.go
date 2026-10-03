package cache

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// purgeRecorder captures every key handed to the PurgeBroadcast hook.
type purgeRecorder struct {
	mu   sync.Mutex
	keys []api.Key
}

func (p *purgeRecorder) record(key api.Key) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.keys = append(p.keys, key)
}

func (p *purgeRecorder) snapshot() []api.Key {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]api.Key(nil), p.keys...)
}

// newBroadcastHandler builds a Handler wired with a PurgeBroadcast hook
// that records every fan-out call (issue #753).
func newBroadcastHandler(t *testing.T, upstream fasthttp.RequestHandler, rec *purgeRecorder) *Handler {
	t.Helper()
	return NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  1 << 20,
			NumShards: 2,
		}),
		PurgeBroadcast: rec.record,
	})
}

// TestHandler_InvalidateBroadcastsKey pins the core fix for #753: a
// data-plane invalidating method (POST) fans the purged GET key out to
// the cluster hook, not just the local store.
func TestHandler_InvalidateBroadcastsKey(t *testing.T) {
	t.Parallel()
	rec := &purgeRecorder{}
	h := newBroadcastHandler(t, origin200("body"), rec)

	rr := testCtx("POST", "http://example.com/item")
	h.ServeRequest(rr)
	require.Equal(t, 200, respCode(rr))

	keys := rec.snapshot()
	require.Len(t, keys, 1)

	ri := requestInfoFromURL("GET", "http://example.com/item")
	require.Equal(t, BuildKey(ri, nil), keys[0], "broadcast must carry the GET-equivalent cache key")
}

// TestHandler_InvalidateBroadcastFiresOnLocalMiss pins the open question
// from #753: the fan-out fires even when the local purge reports
// owned=false. On a non-owner the local store is empty, but the owner
// may hold the object — owned=false must not suppress the broadcast.
func TestHandler_InvalidateBroadcastFiresOnLocalMiss(t *testing.T) {
	t.Parallel()
	rec := &purgeRecorder{}
	h := newBroadcastHandler(t, origin200("body"), rec)

	// No warm-up: the local store has nothing for this key.
	rr := testCtx("POST", "http://example.com/never-cached")
	h.ServeRequest(rr)
	require.Equal(t, 200, respCode(rr))

	require.Len(t, rec.snapshot(), 1, "broadcast must fire even when the local store does not hold the key")
}

// TestHandler_InvalidateBroadcastsLocationKeys pins RFC 9111 §4.4
// Location/Content-Location eviction on the fan-out: each derived key
// is broadcast once, in addition to the request key.
func TestHandler_InvalidateBroadcastsLocationKeys(t *testing.T) {
	t.Parallel()
	rec := &purgeRecorder{}
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.Location, "/elsewhere")
		ctx.SetStatusCode(201)
		_, _ = ctx.Write([]byte("created"))
	}
	h := newBroadcastHandler(t, upstream, rec)

	rr := testCtx("POST", "http://example.com/create")
	h.ServeRequest(rr)
	require.Equal(t, 201, respCode(rr))

	keys := rec.snapshot()
	require.Len(t, keys, 2)

	reqKey := BuildKey(requestInfoFromURL("GET", "http://example.com/create"), nil)
	locKey := BuildKey(requestInfoFromURL("GET", "http://example.com/elsewhere"), nil)
	require.Contains(t, keys, reqKey)
	require.Contains(t, keys, locKey)
}

// TestHandler_InvalidateBroadcastNotOn5xx pins that a failed origin
// exchange never fans an invalidation out (issue #753: only 2xx/3xx
// trigger RFC 9111 §4.4 invalidation).
func TestHandler_InvalidateBroadcastNotOn5xx(t *testing.T) {
	t.Parallel()
	rec := &purgeRecorder{}
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(500)
		_, _ = ctx.Write([]byte("boom"))
	}
	h := newBroadcastHandler(t, upstream, rec)

	rr := testCtx("POST", "http://example.com/failing")
	h.ServeRequest(rr)
	require.Equal(t, 500, respCode(rr))

	require.Empty(t, rec.snapshot(), "5xx origin response must not broadcast a purge")
}

// TestHandler_InvalidateBroadcastPerMethod covers PUT and DELETE, the
// other unsafe methods that route through the same invalidation path.
func TestHandler_InvalidateBroadcastPerMethod(t *testing.T) {
	t.Parallel()
	for _, method := range []string{"PUT", "DELETE"} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()
			rec := &purgeRecorder{}
			h := newBroadcastHandler(t, origin200("body"), rec)

			rr := testCtx(method, "http://example.com/item")
			h.ServeRequest(rr)
			require.Equal(t, 200, respCode(rr))
			require.Len(t, rec.snapshot(), 1)
		})
	}
}
