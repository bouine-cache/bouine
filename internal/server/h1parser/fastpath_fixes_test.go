package h1parser

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

// TestFallThrough_BodyAcrossReads verifies that a request body delivered
// after the buffered bytes (still arriving on the socket) is not
// truncated: the fallback handler must see the complete body. This is
// the regression test for the SetBodyRaw truncation bug — the first
// Read returned only part of the body and the rest was discarded.
func TestFallThrough_BodyAcrossReads(t *testing.T) {
	t.Parallel()

	var gotBody []byte
	var wg sync.WaitGroup
	wg.Add(1)

	handler := func(ctx *fasthttp.RequestCtx) {
		gotBody = append([]byte(nil), ctx.Request.Body()...)
		wg.Done()
		ctx.SetStatusCode(fasthttp.StatusOK)
	}

	parser := New(nil, handler)

	clientConn, serverConn := dialTCPPair(t)
	defer clientConn.Close()

	// The body is larger than the fall-through buffer so the remainder
	// is still on the wire when handleFallThrough runs.
	body := bytes.Repeat([]byte("x"), 64*1024)

	req := &api.RawRequest{
		Method:      "POST",
		Path:        "/upload",
		HTTPVersion: "HTTP/1.1",
		Host:        "example.com",
		Headers: [api.MaxRawHeaders]api.RawHeader{
			{Key: "Host", Value: "example.com"},
			{Key: "Content-Type", Value: "application/octet-stream"},
			{Key: "Content-Length", Value: "65536"},
		},
		NHeaders: 3,
	}

	// Feed the first 4 KiB of the body via excess; the remaining 61 KiB
	// arrives on the socket from a concurrent writer.
	excess := body[:4096]

	done := make(chan struct{}, 1)
	go func() {
		// Write the rest of the body after a short delay so the
		// fall-through read starts with only the buffered prefix.
		time.Sleep(50 * time.Millisecond)
		_, _ = clientConn.Write(body[4096:])
		done <- struct{}{}
	}()

	_, ftErr := parser.handleFallThrough(serverConn, req, excess)
	require.NoError(t, ftErr)
	_ = serverConn.Close()
	<-done

	wg.Wait()
	require.Len(t, gotBody, len(body), "full body must be delivered, not truncated")
	assert.True(t, bytes.Equal(gotBody, body))
}

// TestServe_PipelinedBytesAfterHit verifies that bytes pipelined after
// a cache-hit request's headers (the start of the next request) are not
// discarded: the connection must process them as the next request.
func TestServe_PipelinedBytesAfterHit(t *testing.T) {
	t.Parallel()

	fp := &mockFastPathHit{}
	var handlerCalls int
	handler := func(ctx *fasthttp.RequestCtx) {
		handlerCalls++
		ctx.SetBodyString("miss")
		ctx.SetStatusCode(fasthttp.StatusOK)
	}
	p := New(fp, handler)

	client, server := dialTCPPair(t)
	defer client.Close()

	// Send both requests in a single write — the fast path serves the
	// first (hit), and the second request's bytes are already buffered
	// as excess.
	pipelined := "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n" +
		"GET /next HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"

	go func() {
		_, _ = client.Write([]byte(pipelined))
	}()

	done := make(chan error, 1)
	go func() { done <- p.Serve(server) }()

	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(client)
	resp1 := &fasthttp.Response{}
	require.NoError(t, resp1.Read(reader))
	assert.Equal(t, 200, resp1.StatusCode())
	// The second (miss) response must also arrive — its pipelined bytes
	// were buffered after the first hit.
	resp2 := &fasthttp.Response{}
	require.NoError(t, resp2.Read(reader))
	assert.Equal(t, 200, resp2.StatusCode())
	assert.Equal(t, "miss", string(resp2.Body()))
	require.Equal(t, 1, handlerCalls, "the pipelined miss must reach the fallback handler")

	_ = client.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after client close")
	}
}

// TestServe_OversizeHeadersStillServed verifies a request whose headers
// exceed the 16 KiB read buffer is served via the fallback handler
// instead of being silently dropped.
func TestServe_OversizeHeadersStillServed(t *testing.T) {
	t.Parallel()

	var handlerCalled bool
	handler := func(ctx *fasthttp.RequestCtx) {
		handlerCalled = true
		ctx.SetStatusCode(fasthttp.StatusOK)
	}
	p := New(nil, handler)

	client, server := dialTCPPair(t)
	defer client.Close()

	// Build a request with ~200 small headers so the total header block
	// exceeds readBufferSize (16 KiB) without breaking the per-header cap.
	var raw bytes.Buffer
	raw.WriteString("GET /big HTTP/1.1\r\nHost: localhost\r\n")
	for i := range 200 {
		fmt.Fprintf(&raw, "X-Test-%03d: %s\r\n", i, strings.Repeat("v", 100))
	}
	raw.WriteString("Connection: close\r\n\r\n")

	go func() {
		_, _ = client.Write(raw.Bytes())
	}()

	done := make(chan error, 1)
	go func() { done <- p.Serve(server) }()

	client.SetReadDeadline(time.Now().Add(10 * time.Second))
	resp := &fasthttp.Response{}
	err := resp.Read(bufio.NewReader(client))
	require.NoError(t, err, "the oversize-header request must be served, not dropped")
	assert.Equal(t, 200, resp.StatusCode())
	assert.True(t, handlerCalled)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = server.Close()
	}
}

// TestServe_SmugglingRejectedWith400 asserts the client-visible
// behavior: an ambiguous CL+TE request receives a 400 response, not a
// normal (potentially smuggled) response from the fallback handler.
func TestServe_SmugglingRejectedWith400(t *testing.T) {
	t.Parallel()

	var handlerCalled bool
	handler := func(ctx *fasthttp.RequestCtx) {
		handlerCalled = true
		ctx.SetStatusCode(fasthttp.StatusOK)
	}
	p := New(nil, handler)

	client, server := dialTCPPair(t)
	defer client.Close()

	go func() {
		_, _ = client.Write([]byte(
			"POST / HTTP/1.1\r\nHost: localhost\r\n" +
				"Content-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n" +
				"0\r\n\r\nGET /smuggled HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	}()

	done := make(chan error, 1)
	go func() { done <- p.Serve(server) }()

	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp := &fasthttp.Response{}
	err := resp.Read(bufio.NewReader(client))
	require.NoError(t, err)
	assert.Equal(t, 400, resp.StatusCode(), "smuggling must be rejected with 400")
	assert.False(t, handlerCalled, "the fallback handler must not see the smuggled request")

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("connection must be closed after a 400")
	}
}

// (helpers shared with options_test.go: mockFastPathHit, dialTCPPair, mockConn)
