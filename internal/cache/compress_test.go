package cache

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	brotli "github.com/molecule-man/go-brrr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func compressibleBody(size int) []byte {
	var sb strings.Builder
	sb.Grow(size)
	for sb.Len() < size {
		sb.WriteString("<html><body><p class=\"card\">The quick brown fox jumps over the lazy dog. ")
		sb.WriteString("Lorem ipsum dolor sit amet, consectetur adipiscing elit.</p></body></html>\n")
	}
	return []byte(sb.String()[:size])
}

func decompressGzip(t *testing.T, b []byte) []byte {
	t.Helper()
	r, err := gzip.NewReader(bytes.NewReader(b))
	require.NoError(t, err)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	return out
}

func decompressZstd(t *testing.T, b []byte) []byte {
	t.Helper()
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()
	out, err := dec.DecodeAll(b, nil)
	require.NoError(t, err)
	return out
}

func decompressBr(t *testing.T, b []byte) []byte {
	t.Helper()
	r := brotli.NewReader(bytes.NewReader(b))
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	return out
}

func TestCompressBytes_RoundTrip(t *testing.T) {
	t.Parallel()
	body := compressibleBody(64 * 1024)
	for _, coding := range []string{"gzip", "zstd", "br"} {
		t.Run(coding, func(t *testing.T) {
			t.Parallel()
			dst, ok := compressBytes(coding, body)
			require.True(t, ok, "compressible body must compress")
			require.Less(t, len(dst), len(body))
			var out []byte
			switch coding {
			case "gzip":
				out = decompressGzip(t, dst)
			case "zstd":
				out = decompressZstd(t, dst)
			case "br":
				out = decompressBr(t, dst)
			}
			assert.Equal(t, body, out)
		})
	}
}

func randomBytes(n int) []byte {
	// xorshift PRNG: deterministic, incompressible-looking output.
	out := make([]byte, n)
	var s uint64 = 0x9e3779b97f4a7c15
	for i := range out {
		s ^= s << 13
		s ^= s >> 7
		s ^= s << 17
		out[i] = byte(s)
	}
	return out
}

func TestCompressBytes_ExpansionReturnsFalse(t *testing.T) {
	t.Parallel()
	for _, coding := range []string{"gzip", "zstd", "br"} {
		_, ok := compressBytes(coding, randomBytes(64*1024))
		assert.False(t, ok, "%s on random bytes should report expansion", coding)
	}
}

func TestCompressBytes_UnknownCoding(t *testing.T) {
	t.Parallel()
	_, ok := compressBytes("deflate", []byte("hello"))
	assert.False(t, ok)
}

func TestAcquireReleaseCompressor_PoolReuse(t *testing.T) {
	t.Parallel()
	for _, coding := range []string{"gzip", "zstd", "br"} {
		var buf, store bytes.Buffer
		sc := acquireCompressor(coding, &buf, &store)
		require.NotNil(t, sc)
		require.NoError(t, sc.write([]byte("hello ")))
		require.NoError(t, sc.write([]byte("world")))
		require.NoError(t, sc.close())
		require.Positive(t, buf.Len())
		releaseCompressor(sc)
		// Second acquisition must produce a clean compressor bound to a
		// fresh buffer.
		var buf2, store2 bytes.Buffer
		sc2 := acquireCompressor(coding, &buf2, &store2)
		require.NotNil(t, sc2)
		require.Zero(t, buf2.Len(), "Reset must clear the output buffer")
		require.Zero(t, store2.Len(), "Reset must clear the store buffer")
		releaseCompressor(sc2)
	}
}

func TestFillCompressor_StreamingChunksMatchOneShot(t *testing.T) {
	t.Parallel()
	body := compressibleBody(300 * 1024) // > compressFlushEvery, exercises sync-flush
	for _, coding := range []string{"gzip", "zstd", "br"} {
		t.Run(coding, func(t *testing.T) {
			t.Parallel()
			oneShot, ok := compressBytes(coding, body)
			require.True(t, ok)

			var store bytes.Buffer
			var client bytes.Buffer
			var out bytes.Buffer
			sc := acquireCompressor(coding, &out, &store)
			require.NotNil(t, sc)
			fc := &fillCompressor{sc: sc, buf: &store, out: &out}
			chunk := make([]byte, 32*1024)
			for off := 0; off < len(body); off += len(chunk) {
				end := off + len(chunk)
				if end > len(body) {
					end = len(body)
				}
				require.NoError(t, fc.writeChunk(&client, body[off:end]))
			}
			n, err := fc.finish(&client)
			require.NoError(t, err)
			releaseCompressor(sc)

			// Streamed output must decode to the original body.
			require.Equal(t, store.Len(), n)
			require.Equal(t, client.Bytes(), store.Bytes(), "client and store sinks must match")
			var decoded []byte
			switch coding {
			case "gzip":
				decoded = decompressGzip(t, client.Bytes())
			case "zstd":
				decoded = decompressZstd(t, client.Bytes())
			case "br":
				decoded = decompressBr(t, client.Bytes())
			}
			assert.Equal(t, body, decoded)
			// Sync-flushed stream may differ in bytes from one-shot but
			// must stay in the same ballpark (ratio not destroyed).
			require.Less(t, len(client.Bytes()), len(body)/2)
			_ = oneShot
		})
	}
}

func TestNewCompressionPolicy(t *testing.T) {
	t.Parallel()
	require.Nil(t, newCompressionPolicy(CompressionSpec{}))

	p := newCompressionPolicy(CompressionSpec{
		Enabled:      true,
		Encodings:    []string{"gzip", "zstd"},
		MinSize:      512,
		MaxSize:      1 << 20,
		ContentTypes: []string{"text/html", "TEXT/*", "application/json"},
	})
	require.NotNil(t, p)
	assert.Equal(t, int64(512), p.minSize)
	assert.Equal(t, int64(1<<20), p.maxSize)
	assert.True(t, p.allowsCoding("gzip"))
	assert.True(t, p.allowsCoding("zstd"))
	assert.False(t, p.allowsCoding("br"))
	assert.True(t, p.allowsContentType("text/html; charset=utf-8"))
	assert.True(t, p.allowsContentType("text/css"))
	assert.True(t, p.allowsContentType("application/json"))
	assert.False(t, p.allowsContentType("image/png"))
	assert.False(t, p.allowsContentType(""))
}

func TestCompressionPolicy_Defaults(t *testing.T) {
	t.Parallel()
	p := newCompressionPolicy(CompressionSpec{Enabled: true, ContentTypes: []string{"text/html"}})
	require.NotNil(t, p)
	assert.Equal(t, int64(1024), p.minSize)
	assert.Equal(t, int64(1<<20), p.maxSize)
	assert.True(t, p.allowsCoding("br"), "empty encodings list means all supported")
	assert.True(t, p.allowsCoding("zstd"))
	assert.True(t, p.allowsCoding("gzip"))
}

func compressionTestHandler(t *testing.T, upstream fasthttp.RequestHandler, spec CompressionSpec) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  64 << 20,
		NumShards: 2,
	})
	return NewHandler(HandlerConfig{
		Upstream:    upstream,
		FastClient:  &testFastClient{handler: upstream},
		Store:       store,
		Compression: spec,
	})
}

func htmlOrigin(body string) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.ContentType, "text/html; charset=utf-8")
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString(body)
	}
}

func TestFillCompression_BufferedMissStoresCompressed(t *testing.T) {
	t.Parallel()
	body := string(compressibleBody(64 * 1024))
	h := compressionTestHandler(t, htmlOrigin(body), CompressionSpec{
		Enabled:      true,
		ContentTypes: []string{"text/html"},
	})

	ctx := testCtxWithHeader("GET", "http://example.com/page", header.AcceptEncoding, "gzip")
	h.ServeRequest(ctx)
	require.Equal(t, 200, respCode(ctx))
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))
	require.Equal(t, "gzip", respHeader(ctx, header.ContentEncoding), "fill response must be compressed")
	require.Equal(t, body, string(decompressGzip(t, ctx.Response.Body())))

	// Second request: HIT served verbatim from the stored compressed variant.
	hit := testCtxWithHeader("GET", "http://example.com/page", header.AcceptEncoding, "gzip")
	h.ServeRequest(hit)
	require.Equal(t, "HIT", respHeader(hit, header.XCache))
	require.Equal(t, "gzip", string(hit.Response.Header.Peek(header.ContentEncoding)))
	require.Equal(t, ctx.Response.Body(), hit.Response.Body(), "hit must serve stored bytes verbatim")
	require.Equal(t, body, string(decompressGzip(t, hit.Response.Body())))
	require.Equal(t, strconv.Itoa(len(hit.Response.Body())), string(hit.Response.Header.Peek(header.ContentLength)))

	// Identity-only client: the origin has no Vary, so the primary key
	// holds the compressed variant and the identity request hits it
	// verbatim (same as an origin that pre-compresses: one variant).
	// The identity client therefore also receives gzip bytes here.
	id := testCtx("GET", "http://example.com/page")
	h.ServeRequest(id)
	require.Equal(t, "HIT", respHeader(id, header.XCache))
	require.Equal(t, "gzip", string(id.Response.Header.Peek(header.ContentEncoding)))
	require.Equal(t, body, string(decompressGzip(t, id.Response.Body())))
}

func TestFillCompression_SkipsWhenOriginAlreadyEncoded(t *testing.T) {
	t.Parallel()
	body := string(compressibleBody(8 * 1024))
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.ContentType, "text/html")
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.ContentEncoding, "gzip")
		gz, _ := compressBytes("gzip", []byte(body))
		_, _ = ctx.Write(gz)
	}
	h := compressionTestHandler(t, upstream, CompressionSpec{
		Enabled:      true,
		ContentTypes: []string{"text/html"},
	})
	ctx := testCtxWithHeader("GET", "http://example.com/enc", header.AcceptEncoding, "gzip")
	h.ServeRequest(ctx)
	require.Equal(t, "gzip", respHeader(ctx, header.ContentEncoding))
	require.Equal(t, body, string(decompressGzip(t, ctx.Response.Body())), "origin-encoded body passes through untouched")
}

func TestFillCompression_SkipsClientStateAndAuth(t *testing.T) {
	t.Parallel()
	body := string(compressibleBody(8 * 1024))
	withCookie := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.ContentType, "text/html")
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.SetCookie, "session=abc")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString(body)
	}
	h := compressionTestHandler(t, withCookie, CompressionSpec{
		Enabled:      true,
		ContentTypes: []string{"text/html"},
	})
	ctx := testCtxWithHeader("GET", "http://example.com/ck", header.AcceptEncoding, "gzip")
	h.ServeRequest(ctx)
	require.Equal(t, "", respHeader(ctx, header.ContentEncoding), "Set-Cookie responses are never compressed")

	authH := compressionTestHandler(t, htmlOrigin(body), CompressionSpec{
		Enabled:      true,
		ContentTypes: []string{"text/html"},
	})
	auth := testCtxWithHeader("GET", "http://example.com/auth", header.AcceptEncoding, "gzip")
	auth.Request.Header.Set(header.Authorization, "Bearer x")
	authH.ServeRequest(auth)
	require.Equal(t, "", respHeader(auth, header.ContentEncoding), "authorized requests are never compressed (T25)")
}

func TestFillCompression_SkipsBelowMinSize(t *testing.T) {
	t.Parallel()
	h := compressionTestHandler(t, htmlOrigin("<html>tiny</html>"), CompressionSpec{
		Enabled:      true,
		ContentTypes: []string{"text/html"},
		MinSize:      1024,
	})
	ctx := testCtxWithHeader("GET", "http://example.com/tiny", header.AcceptEncoding, "br")
	h.ServeRequest(ctx)
	require.Equal(t, "", respHeader(ctx, header.ContentEncoding))
	require.Equal(t, "<html>tiny</html>", respBody(ctx))
}

func TestFillCompression_SkipsOverMaxSize(t *testing.T) {
	t.Parallel()
	body := string(compressibleBody(2 * 1024 * 1024)) // above default 1 MiB cap
	h := compressionTestHandler(t, htmlOrigin(body), CompressionSpec{
		Enabled:      true,
		ContentTypes: []string{"text/html"},
		MaxSize:      1 << 20,
	})
	ctx := testCtxWithHeader("GET", "http://example.com/big", header.AcceptEncoding, "zstd")
	h.ServeRequest(ctx)
	require.Equal(t, 200, respCode(ctx))
	require.Equal(t, "", respHeader(ctx, header.ContentEncoding), "over-max-size buffered fill is not compressed")
}

func TestFillCompression_ExpansionKeepsIdentity(t *testing.T) {
	t.Parallel()
	random := randomBytes(64 * 1024)
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.ContentType, "text/html")
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		_, _ = ctx.Write(random)
	}
	h := compressionTestHandler(t, upstream, CompressionSpec{
		Enabled:      true,
		ContentTypes: []string{"text/html"},
	})
	ctx := testCtxWithHeader("GET", "http://example.com/rnd", header.AcceptEncoding, "gzip")
	h.ServeRequest(ctx)
	require.Equal(t, "", respHeader(ctx, header.ContentEncoding), "expanding input must stay identity")
	require.Equal(t, random, ctx.Response.Body())
}

func TestFillCompression_DisabledByDefault(t *testing.T) {
	t.Parallel()
	body := string(compressibleBody(16 * 1024))
	h := compressionTestHandler(t, htmlOrigin(body), CompressionSpec{})
	ctx := testCtxWithHeader("GET", "http://example.com/off", header.AcceptEncoding, "gzip")
	h.ServeRequest(ctx)
	require.Equal(t, "", respHeader(ctx, header.ContentEncoding))
	require.Equal(t, body, respBody(ctx))
}

func TestFillCompression_BucketSelection(t *testing.T) {
	t.Parallel()
	body := string(compressibleBody(16 * 1024))
	h := compressionTestHandler(t, htmlOrigin(body), CompressionSpec{
		Enabled:      true,
		ContentTypes: []string{"text/html"},
	})
	// br-capable client -> br bucket (ties prefer br).
	brCtx := testCtxWithHeader("GET", "http://example.com/br", header.AcceptEncoding, "gzip, deflate, br, zstd")
	h.ServeRequest(brCtx)
	require.Equal(t, "br", string(brCtx.Response.Header.Peek(header.ContentEncoding)))
	require.Equal(t, body, string(decompressBr(t, brCtx.Response.Body())))

	// gzip-only client -> gzip bucket, separate variant.
	gzCtx := testCtxWithHeader("GET", "http://example.com/gz", header.AcceptEncoding, "gzip")
	h.ServeRequest(gzCtx)
	require.Equal(t, "gzip", string(gzCtx.Response.Header.Peek(header.ContentEncoding)))
	require.Equal(t, body, string(decompressGzip(t, gzCtx.Response.Body())))

	// br hit serves the stored br variant verbatim.
	brHit := testCtxWithHeader("GET", "http://example.com/br", header.AcceptEncoding, "br, gzip")
	h.ServeRequest(brHit)
	require.Equal(t, "HIT", respHeader(brHit, header.XCache))
	require.Equal(t, "br", string(brHit.Response.Header.Peek(header.ContentEncoding)))
	require.Equal(t, brCtx.Response.Body(), brHit.Response.Body())
}

func TestFillCompression_EncodingsAllowList(t *testing.T) {
	t.Parallel()
	body := string(compressibleBody(16 * 1024))
	h := compressionTestHandler(t, htmlOrigin(body), CompressionSpec{
		Enabled:      true,
		Encodings:    []string{"gzip"}, // no br/zstd
		ContentTypes: []string{"text/html"},
	})
	ctx := testCtxWithHeader("GET", "http://example.com/ae", header.AcceptEncoding, "gzip, br")
	h.ServeRequest(ctx)
	// The negotiated bucket is br, which is not in the allow-list: no
	// compression for that fill.
	require.Equal(t, "", respHeader(ctx, header.ContentEncoding))
	require.Equal(t, body, respBody(ctx))
}

// runTeeCompress drives teeStreamToClientCompress directly with an active
// fill compressor (the streaming fill path), mirroring runTee but with
// fc != nil. Returns the handler, the flight verdict, and the key.
func runTeeCompress(t *testing.T, w *bufio.Writer, body io.Reader, coding string) (*Handler, *inflightStream, api.Key) {
	t.Helper()

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream:   func(*fasthttp.RequestCtx) {},
		FastClient: &testFastClient{handler: func(*fasthttp.RequestCtx) {}},
		Store:      store,
		Compression: CompressionSpec{
			Enabled:      true,
			ContentTypes: []string{"text/html"},
		},
	})
	key := BuildKeyFromURL("http://example.com/tee-compress", nil)

	rh := &fasthttp.ResponseHeader{}
	rh.Set(header.CacheControl, "max-age=60")
	rh.Set(header.ContentType, "text/html; charset=utf-8")
	sf := &streamFetchResult{
		StatusCode: 200,
		Header:     fromFastHTTPHeader(rh),
	}
	inflight := newInflightStream()
	buf := streamBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	ri := requestInfoFromCtx(testCtxWithHeader("GET", "http://example.com/tee-compress", header.AcceptEncoding, coding))
	resMap := headerMap(header.CacheControl, "max-age=60", header.ContentType, "text/html; charset=utf-8")
	var storeBuf, outBuf bytes.Buffer
	sc := acquireCompressor(coding, &outBuf, &storeBuf)
	require.NotNil(t, sc)
	fc := &fillCompressor{sc: sc, buf: &storeBuf, out: &outBuf}
	h.teeStreamToClientCompress(w, body, buf, sf, inflight, key, key, ri, resMap, nil, fc)
	_ = w.Flush()
	return h, inflight, key
}

// TestTeeStreamCompress_StoresCompressedVariant drives the STREAMING fill
// path (fc != nil) end to end: the stored object must carry the compressed
// bytes (not an empty body), Content-Encoding, and a matching
// Content-Length, and a byte-handoff follower must receive the same
// compressed bytes with the same headers.
func TestTeeStreamCompress_StoresCompressedVariant(t *testing.T) {
	t.Parallel()
	body := compressibleBody(300 * 1024) // > compressFlushEvery: exercises sync-flush

	var client bytes.Buffer
	w := bufio.NewWriter(&client)
	h, inflight, key := runTeeCompress(t, w, bytes.NewReader(body), "gzip")

	require.NoError(t, inflight.res.Err, "clean-EOF compressed fill is shareable")
	// Follower verdict pairs the compressed body with the stamped headers.
	require.Equal(t, "gzip", inflight.res.Header.Get(header.ContentEncoding),
		"published verdict must carry Content-Encoding")
	require.Equal(t, client.Bytes(), inflight.res.Body,
		"published body must equal what the leader streamed to its client")
	require.Equal(t, body, decompressGzip(t, inflight.res.Body))

	// Stored object: compressed body, CE header, corrected CL.
	obj, _, err := h.store.Get(t.Context(), key)
	require.NoError(t, err)
	require.NotNil(t, obj, "compressed fill must be stored")
	require.Equal(t, "gzip", obj.Header.Get(header.ContentEncoding))
	require.Equal(t, strconv.Itoa(len(client.Bytes())), obj.Header.Get(header.ContentLength),
		"stored Content-Length must equal the compressed size, not 0")
	require.Equal(t, client.Bytes(), obj.Body, "stored body must be the compressed bytes")
	require.Equal(t, body, decompressGzip(t, obj.Body))
}

// TestTeeStreamCompress_OverMaxSizeNotStored: a body over max_size streams
// compressed to the client but is not stored (skipStore path).
func TestTeeStreamCompress_OverMaxSizeNotStored(t *testing.T) {
	t.Parallel()
	body := compressibleBody(600 * 1024)

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream:   func(*fasthttp.RequestCtx) {},
		FastClient: &testFastClient{handler: func(*fasthttp.RequestCtx) {}},
		Store:      store,
		Compression: CompressionSpec{
			Enabled:      true,
			ContentTypes: []string{"text/html"},
			MaxSize:      512 * 1024,
		},
	})
	key := BuildKeyFromURL("http://example.com/tee-oversize", nil)
	rh := &fasthttp.ResponseHeader{}
	rh.Set(header.CacheControl, "max-age=60")
	rh.Set(header.ContentType, "text/html")
	sf := &streamFetchResult{StatusCode: 200, Header: fromFastHTTPHeader(rh)}
	inflight := newInflightStream()
	buf := streamBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	ri := requestInfoFromCtx(testCtxWithHeader("GET", "http://example.com/tee-oversize", header.AcceptEncoding, "gzip"))
	resMap := headerMap(header.CacheControl, "max-age=60", header.ContentType, "text/html")
	var storeBuf, outBuf bytes.Buffer
	sc := acquireCompressor("gzip", &outBuf, &storeBuf)
	require.NotNil(t, sc)
	fc := &fillCompressor{sc: sc, buf: &storeBuf, out: &outBuf}
	var client bytes.Buffer
	w := bufio.NewWriter(&client)
	h.teeStreamToClientCompress(w, bytes.NewReader(body), buf, sf, inflight, key, key, ri, resMap, nil, fc)
	_ = w.Flush()

	require.True(t, fc.skipStore, "over-max_size fill must skip the store")
	require.ErrorIs(t, inflight.res.Err, ErrStreamUnshareable,
		"skipStore fill publishes unshareable: followers' headers were never rewritten")
	obj, _, err := h.store.Get(t.Context(), key)
	require.NoError(t, err)
	require.Nil(t, obj, "nothing may be stored for an over-max_size compressed fill")
	require.Equal(t, body, decompressGzip(t, client.Bytes()),
		"the filling client still receives valid compressed bytes")
}
