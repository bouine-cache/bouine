package h1parser

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

func TestHandleFallThrough_ServesResponse(t *testing.T) {
	responseBody := []byte("Hello from origin")

	handler := func(ctx *fasthttp.RequestCtx) {
		ctx.SetBody(responseBody)
		ctx.SetStatusCode(fasthttp.StatusOK)
	}

	parser := New(nil, handler, WithNowFunc(time.Now))

	clientConn, serverConn := dialTCPPair(t)
	defer clientConn.Close()

	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/miss",
		HTTPVersion: "HTTP/1.1",
		Host:        "example.com",
		Headers: [api.MaxRawHeaders]api.RawHeader{
			{Key: "Host", Value: "example.com"},
			{Key: "Connection", Value: "close"},
		},
		NHeaders: 2,
	}

	done := make(chan error, 1)
	go func() {
		err := parser.handleFallThrough(serverConn, req, nil)
		_ = serverConn.Close()
		done <- err
	}()

	clientConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(clientConn), nil)
	require.NoError(t, err, "ReadResponse")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	assert.Equal(t, 200, resp.StatusCode)
	assert.True(t, bytes.Equal(body, responseBody))

	select {
	case err := <-done:
		assert.Nil(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("handleFallThrough did not return within 5s")
	}
}

func TestHandleFallThrough_PreservesHeaders(t *testing.T) {
	var gotAccept, gotCustom string
	var wg sync.WaitGroup
	wg.Add(1)

	handler := func(ctx *fasthttp.RequestCtx) {
		gotAccept = string(ctx.Request.Header.Peek("Accept"))
		gotCustom = string(ctx.Request.Header.Peek("X-Custom"))
		wg.Done()
		ctx.SetStatusCode(fasthttp.StatusOK)
	}

	parser := New(nil, handler, WithNowFunc(time.Now))

	clientConn, serverConn := dialTCPPair(t)
	defer clientConn.Close()

	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/api",
		HTTPVersion: "HTTP/1.1",
		Host:        "example.com",
		Headers: [api.MaxRawHeaders]api.RawHeader{
			{Key: "Host", Value: "example.com"},
			{Key: "Accept", Value: "text/html"},
			{Key: "X-Custom", Value: "custom-value"},
			{Key: "Connection", Value: "close"},
		},
		NHeaders: 4,
	}

	done := make(chan error, 1)
	go func() {
		err := parser.handleFallThrough(serverConn, req, nil)
		_ = serverConn.Close()
		done <- err
	}()

	clientConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(clientConn), nil)
	require.NoError(t, err, "ReadResponse")
	resp.Body.Close()

	select {
	case err := <-done:
		assert.Nil(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("handleFallThrough did not return within 5s")
	}

	wg.Wait()
	assert.Equal(t, "text/html", gotAccept)
	assert.Equal(t, "custom-value", gotCustom)
}

func TestHandleFallThrough_PassesExcessBody(t *testing.T) {
	var gotBody []byte
	var wg sync.WaitGroup
	wg.Add(1)

	handler := func(ctx *fasthttp.RequestCtx) {
		gotBody = append(gotBody[:0], ctx.Request.Body()...)
		wg.Done()
		ctx.SetStatusCode(fasthttp.StatusOK)
	}

	parser := New(nil, handler, WithNowFunc(time.Now))

	clientConn, serverConn := dialTCPPair(t)
	defer clientConn.Close()

	excess := []byte(`{"key":"value!!"}`)

	req := &api.RawRequest{
		Method:      "POST",
		Path:        "/submit",
		HTTPVersion: "HTTP/1.1",
		Host:        "example.com",
		Headers: [api.MaxRawHeaders]api.RawHeader{
			{Key: "Host", Value: "example.com"},
			{Key: "Content-Type", Value: "application/json"},
			{Key: "Content-Length", Value: "17"},
			{Key: "Connection", Value: "close"},
		},
		NHeaders: 4,
	}

	done := make(chan error, 1)
	go func() {
		err := parser.handleFallThrough(serverConn, req, excess)
		_ = serverConn.Close()
		done <- err
	}()

	clientConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(clientConn), nil)
	require.NoError(t, err, "ReadResponse")
	resp.Body.Close()

	select {
	case err := <-done:
		assert.Nil(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("handleFallThrough did not return within 5s")
	}

	wg.Wait()
	assert.True(t, bytes.Equal(gotBody, excess))
}

func dialTCPPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "Listen")
	t.Cleanup(func() { ln.Close() })

	type acceptResult struct {
		conn net.Conn
		err  error
	}
	ch := make(chan acceptResult, 1)
	go func() {
		c, err := ln.Accept()
		ch <- acceptResult{c, err}
	}()

	c, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err, "Dial")

	res := <-ch
	require.Nil(t, res.err)

	return c, res.conn
}

type mockConn struct {
	r      io.Reader
	w      io.Writer
	mu     sync.Mutex
	closed bool
}

func (m *mockConn) Read(b []byte) (int, error) {
	if m.r != nil {
		return m.r.Read(b)
	}
	return 0, io.EOF
}

func (m *mockConn) Write(b []byte) (int, error) {
	if m.w != nil {
		return m.w.Write(b)
	}
	return len(b), nil
}

func (m *mockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr                { return mockAddr{} }
func (m *mockConn) RemoteAddr() net.Addr               { return mockAddr{} }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

type mockAddr struct{}

func (mockAddr) Network() string { return "mock" }
func (mockAddr) String() string  { return "mock-addr" }
