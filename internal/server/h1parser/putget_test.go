package h1parser

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/valyala/fasthttp"
)

// REGRESSION: a fallback-served request with a body (PUT) followed by a
// request on the same keep-alive connection must not corrupt the second
// request. The first request's excess (its body bytes) must NOT leak into
// the second parse.
func TestFallbackPUTWithBodyThenGET(t *testing.T) {
	puts, gets := 0, 0
	p := New(&mockFastPathHandler{}, func(ctx *fasthttp.RequestCtx) {
		if string(ctx.Method()) == "PUT" {
			puts++
			ctx.SetStatusCode(201)
			ctx.SetBodyString("ok")
		} else {
			gets++
			ctx.SetBodyString("slow")
		}
	})
	client, server := net.Pipe()
	defer client.Close()
	go func() { _ = p.Serve(server) }()

	go func() {
		body := `{"id":"x"}`
		_, _ = client.Write([]byte(fmt.Sprintf("PUT /c HTTP/1.1\r\nHost: h\r\nContent-Length: %d\r\n\r\n%s", len(body), body)))
		time.Sleep(100 * time.Millisecond)
		_, _ = client.Write([]byte("GET /g HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n"))
	}()

	br := bufio.NewReader(client)
	client.SetReadDeadline(time.Now().Add(3 * time.Second))
	n := 0
	for {
		resp := &fasthttp.Response{}
		if err := resp.Read(br); err != nil {
			break
		}
		n++
	}
	if n != 2 || puts != 1 || gets != 1 {
		t.Errorf("want 2 responses / 1 put / 1 get, got %d / %d / %d", n, puts, gets)
	}
}

// REGRESSION: same but the fallback request and its pipelined successor are
// in ONE write; the successor must be served too (leftover capture).
func TestFallbackPUTWithBodyPipelinedGET(t *testing.T) {
	hits := 0
	puts := 0
	fp := &putGetHit{hits: &hits}
	p := New(fp, func(ctx *fasthttp.RequestCtx) {
		if string(ctx.Method()) == "PUT" {
			puts++
			ctx.SetStatusCode(201)
			ctx.SetBodyString("ok")
			return
		}
		ctx.SetBodyString("slow")
	})
	client, server := net.Pipe()
	defer client.Close()
	go func() { _ = p.Serve(server) }()

	go func() {
		body := `{"id":"x"}`
		_, _ = client.Write([]byte(fmt.Sprintf("PUT /c HTTP/1.1\r\nHost: h\r\nContent-Length: %d\r\n\r\n%s", len(body), body) +
			"GET /h HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n"))
	}()

	br := bufio.NewReader(client)
	client.SetReadDeadline(time.Now().Add(3 * time.Second))
	n := 0
	for {
		resp := &fasthttp.Response{}
		if err := resp.Read(br); err != nil {
			break
		}
		n++
	}
	if puts != 1 || hits != 1 || n != 2 {
		t.Errorf("want puts=1 hits=1 n=2, got puts=%d hits=%d n=%d", puts, hits, n)
	}
}

type putGetHit struct{ hits *int }

func (c *putGetHit) TryHit(req *api.RawRequest, _ time.Time) (*api.FastPathResponse, bool) {
	if req.Method != "GET" && req.Method != "HEAD" {
		return nil, false
	}
	*c.hits++
	resp := &api.FastPathResponse{
		BuffersArr: [3][]byte{
			[]byte("HTTP/1.1 200 OK\r\n"),
			[]byte("Content-Length: 5\r\nConnection: keep-alive\r\n\r\n"),
			[]byte("hello"),
		},
		CloseConn: req.ConnectionClose,
	}
	resp.Buffers = resp.BuffersArr[:]
	return resp, true
}
func (c *putGetHit) Release(_ *api.FastPathResponse) {}
