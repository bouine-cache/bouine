package origin

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"

	"github.com/valyala/fasthttp"
)

// BenchmarkPoolFastClient_Get measures the full origin-fetch write+read
// round trip (request serialization onto the pooled conn, URI rewrite,
// response parse) against a local echo origin. It is the bench to watch for
// origin-write-path work: uriScratch pooling, hop-by-hop header skipping,
// write buffer sizing, and TCP_NODELAY all land here.
func BenchmarkPoolFastClient_Get(b *testing.B) {
	srv := fasthttptest.NewServer(b, newEchoHandler())
	defer srv.Close()
	p, err := NewPool(PoolConfig{
		Name:    "bench",
		Targets: []string{srv.Addr},
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		b.Fatalf("NewPool: %v", err)
	}
	defer func() { _ = p.Close(context.Background()) }()
	fc := p.FastClient()

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI("/bench/path?item=1234")
	req.Header.SetHost("bench.local")
	req.Header.Set("Accept", "text/html")
	req.Header.Set("Accept-Encoding", "gzip, br")
	req.Header.Set("User-Agent", "bouine-bench/1.0")
	req.Header.Set("Cookie", "session=abcdef0123456789")

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := fc.DoDeadline(req, resp, time.Now().Add(5*time.Second)); err != nil {
			b.Fatalf("fetch: %v", err)
		}
	}
}
