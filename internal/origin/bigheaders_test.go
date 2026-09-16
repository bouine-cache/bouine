package origin

import (
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/valyala/fasthttp"
)

// bigHeaderHandler returns a fasthttp.RequestHandler emitting a single
// Cache-Tag header whose value is headerLen bytes, mirroring
// product-page's /compare/ responses: one header carrying one product
// UUID per variant, ~4-5 KB in total. fasthttp's default client
// ReadBufferSize is 4096, so a response header block larger than that
// fails to parse (ErrSmallBuffer) and the fetch surfaces as a 502 after
// exhausting the idempotent retries.
func bigHeaderHandler(headerLen int) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		var sb strings.Builder
		for sb.Len() < headerLen {
			fmt.Fprintf(&sb, "prod-eu-%08d,", ctx.ID())
		}
		value := sb.String()
		ctx.Response.Header.Set("Cache-Tag", value[:headerLen])
		ctx.SetStatusCode(fasthttp.StatusOK)
		_, _ = ctx.Write([]byte("ok"))
	}
}

// bigHeaderOrigin starts a plain fasthttp origin (no aggressive
// IdleTimeout) serving bigHeaderHandler(headerLen), and returns its dial
// address. The shared fasthttptest.NewServer helper arms a 50 ms
// IdleTimeout: under the -race detector with the whole package running
// in parallel, that races request handling and turns this regression
// test into an intermittent keepalive failure unrelated to what it
// guards. newSSEOrigin (sse_test.go) uses the same dedicated-server
// pattern.
func bigHeaderOrigin(t *testing.T, headerLen int) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	srv := &fasthttp.Server{Handler: bigHeaderHandler(headerLen)}
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().String()
}

// TestPool_FetchesOversizedResponseHeaders guards the origin client's
// ReadBufferSize against fasthttp's 4 KiB default: an origin response
// whose header block exceeds it must fetch successfully instead of
// failing header parse (prod-eu /product-page/compare/* 502s caused by
// a ~4 KB Cache-Tag header).
func TestPool_FetchesOversizedResponseHeaders(t *testing.T) {
	t.Parallel()
	addr := bigHeaderOrigin(t, 8192)

	p := pool(t, addr)
	h := p.FastHandler(0)

	code, got, _ := serveHandler(t, h, "GET", "/compare/iphone-13/iphone-14", "")

	require.Equal(t, fasthttp.StatusOK, code, "origin fetch with >4KB response headers must succeed, not 502")
	require.Equal(t, "ok", got)
}
