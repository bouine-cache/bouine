package h1parser

import (
	"bufio"
	"bytes"
	"net"
	"testing"

	"github.com/valyala/fasthttp"
)

// BenchmarkPipelinedHits measures 8 pipelined GETs delivered in one
// write. After the pipelining change every request in the batch loops the
// fast path; before, each request after the first fell through to the slow
// fallback handler. The baseline worktree needs the same file with the
// old parseRequest signature — see bench note in the PR description.
func BenchmarkPipelinedHits(b *testing.B) {
	const depth = 8
	one := []byte("GET /api/v1/users/42 HTTP/1.1\r\nHost: example.com\r\nAccept: application/json\r\nUser-Agent: Bouine-Test/1.0\r\n\r\n")
	pipelined := bytes.Repeat(one, depth)

	slowCalls := 0
	handler := func(ctx *fasthttp.RequestCtx) {
		slowCalls++
		ctx.SetBodyString("slow")
	}

	p := New(&mockFastPathHit{}, handler)
	client, server := net.Pipe()
	defer client.Close()
	go func() {
		for {
			if _, err := client.Write(pipelined); err != nil {
				return
			}
		}
	}()
	done := make(chan struct{})
	go func() { _ = p.Serve(server); close(done) }()
	br := bufio.NewReader(client)
	resp := &fasthttp.Response{}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for i := 0; i < depth; i++ {
			resp.Reset()
			if err := resp.Read(br); err != nil {
				b.Fatalf("read resp %d: %v", i, err)
			}
		}
	}
}
