package cache

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
)

func chunkedOrigin(bodySize, chunkSize int) fasthttp.RequestHandler {
	payload := make([]byte, bodySize)
	return func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.Response.Header.Set(header.ETag, `"footprint"`)
		ctx.SetStatusCode(200)
		for off := 0; off < len(payload); off += chunkSize {
			_, _ = ctx.Write(payload[off:min(off+chunkSize, len(payload))])
		}
	}
}

func TestFetchStoresRightSizedBody(t *testing.T) {
	t.Parallel()
	const bodySize = 100_000
	h := testHandler(t, chunkedOrigin(bodySize, 8<<10))

	ctx := testCtx("GET", "http://example.com/right-sized")
	serveRequest(h, ctx)
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))

	key := BuildKey(requestInfoFromCtx(ctx), nil)
	obj, _, err := h.store.Get(context.Background(), key)
	if err != nil || obj == nil {
		t.Fatalf("stored object not found: obj=%v err=%v", obj, err)
	}
	require.Len(t, obj.Body, bodySize)
	slack := cap(obj.Body) - len(obj.Body)
	t.Logf("stored body: len=%d cap=%d slack=%d bytes (%.1f%%)",
		len(obj.Body), cap(obj.Body), slack, 100*float64(slack)/float64(len(obj.Body)))
	assert.Len(t, obj.Body, cap(obj.Body))
}

func TestFetchProducesRightSizedBody(t *testing.T) {
	t.Parallel()
	const bodySize = 100_000
	h := testHandler(t, chunkedOrigin(bodySize, 8<<10))

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://example.com/transfer")
	res := h.doFetch(ctx)
	require.Nil(t, res.Err)
	require.Len(t, res.Body, bodySize)
	t.Logf("body: len=%d cap=%d", len(res.Body), cap(res.Body))
}

func TestWriteHeaderPreSizesBuffer(t *testing.T) {
	t.Skip("responseRecorder removed in fasthttp migration")
}

func BenchmarkStoreFootprint(b *testing.B) {
	const (
		bodySize  = 64 << 10
		chunkSize = 16 << 10
	)
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  256 << 20,
		NumShards: 16,
	})
	h := NewHandler(HandlerConfig{
		Upstream: chunkedOrigin(bodySize, chunkSize),
		Store:    store,
	})

	i := 0
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		ctx := &fasthttp.RequestCtx{}
		ctx.Request.Header.SetMethod("GET")
		ctx.Request.SetRequestURI("http://example.com/obj/" + itoa(i))
		h.ServeRequest(ctx)
		i++
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	n := len(buf)
	for i > 0 {
		n--
		buf[n] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[n:])
}

func BenchmarkStoreFootprint_Interned(b *testing.B) {
	const (
		bodySize  = 64 << 10
		chunkSize = 16 << 10
		numObjs   = 1000
	)
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  256 << 20,
		NumShards: 16,
	})
	h := NewHandler(HandlerConfig{
		Upstream: chunkedOrigin(bodySize, chunkSize),
		Store:    store,
	})

	b.ResetTimer()
	b.ReportAllocs()
	for i := range numObjs {
		ctx := &fasthttp.RequestCtx{}
		ctx.Request.Header.SetMethod("GET")
		ctx.Request.SetRequestURI("http://example.com/obj/" + itoa(i))
		h.ServeRequest(ctx)
	}
}
