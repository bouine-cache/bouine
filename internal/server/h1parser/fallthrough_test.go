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
)

func TestReconstructRawRequest(t *testing.T) {
	req := &api.RawRequest{
		Method:      "POST",
		Path:        "/api/v1/users",
		Query:       "active=true",
		HTTPVersion: "HTTP/1.1",
		Host:        "example.com",
		Headers: [api.MaxRawHeaders]api.RawHeader{
			{Key: "Host", Value: "example.com"},
			{Key: "Content-Type", Value: "application/json"},
			{Key: "Content-Length", Value: "18"},
		},
		NHeaders: 3,
	}

	raw := reconstructRawRequest(req)
	want := "POST /api/v1/users?active=true HTTP/1.1\r\n" +
		"Host: example.com\r\n" +
		"Content-Type: application/json\r\n" +
		"Content-Length: 18\r\n" +
		"\r\n"
	assert.Equal(t, want, string(raw))
}

func TestReconstructRawRequest_NoQuery(t *testing.T) {
	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/healthz",
		HTTPVersion: "HTTP/1.0",
		Headers: [api.MaxRawHeaders]api.RawHeader{
			{Key: "Host", Value: "localhost:8080"},
		},
		NHeaders: 1,
	}

	raw := reconstructRawRequest(req)
	want := "GET /healthz HTTP/1.0\r\nHost: localhost:8080\r\n\r\n"
	assert.Equal(t, want, string(raw))
}

func TestPrefixedConn_Read(t *testing.T) {
	prefix := []byte("PREFIX_DATA")
	backend := &bytes.Buffer{}
	backend.WriteString("_BACKEND")

	conn := &mockConn{r: backend}
	pc := &prefixedConn{Conn: conn, prefix: prefix}

	buf := make([]byte, 32)
	n, err := pc.Read(buf)
	require.NoErrorf(t, err, "first Read: %v", err)
	require.Len(t, prefix, n)
	assert.Equal(t, "PREFIX_DATA", string(buf[:n]))

	n, err = pc.Read(buf)
	require.NoErrorf(t, err, "second Read: %v", err)
	assert.Equal(t, "_BACKEND", string(buf[:n]))
}

// dialTCPPair creates a real TCP connection pair for testing.
func dialTCPPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoErrorf(t, err, "Listen: %v", err)
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
	require.NoErrorf(t, err, "Dial: %v", err)

	res := <-ch
	require.Nil(t, res.err)

	return c, res.conn
}

func TestHandleFallThrough_ServesResponseBeforeClose(t *testing.T) {
	responseBody := []byte("Hello from origin")

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(responseBody)
	})

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
	resp, err := http.ReadResponse(bufio.NewReader(clientConn), &http.Request{Method: "GET"})
	require.NoErrorf(t, err, "ReadResponse: %v", err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, bytes.Equal(body, responseBody))

	select {
	case err := <-done:
		assert.Nil(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("handleFallThrough did not return within 5s — closeNotifyConn race fix not working")
	}
}

func TestHandleFallThrough_PreservesHeaders(t *testing.T) {
	var gotHeaders http.Header
	var wg sync.WaitGroup
	wg.Add(1)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		wg.Done()
		w.WriteHeader(http.StatusOK)
	})

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
	resp, err := http.ReadResponse(bufio.NewReader(clientConn), &http.Request{Method: "GET"})
	require.NoErrorf(t, err, "ReadResponse: %v", err)
	resp.Body.Close()

	select {
	case err := <-done:
		assert.Nil(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("handleFallThrough did not return within 5s")
	}

	wg.Wait()
	got := gotHeaders.Get("Accept")
	assert.Equal(t, "text/html", got)
	got = gotHeaders.Get("X-Custom")
	assert.Equal(t, "custom-value", got)
}

func TestHandleFallThrough_PassesExcessBody(t *testing.T) {
	var gotBody []byte
	var wg sync.WaitGroup
	wg.Add(1)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		wg.Done()
		w.WriteHeader(http.StatusOK)
	})

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
	resp, err := http.ReadResponse(bufio.NewReader(clientConn), &http.Request{Method: "POST"})
	require.NoErrorf(t, err, "ReadResponse: %v", err)
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

func TestCloseNotifyConn_CloseSignalsOnce(t *testing.T) {
	c := newCloseNotifyConn(&mockConn{})

	select {
	case <-c.done:
		t.Fatal("done channel should not be closed before Close")
	default:
	}

	err := c.Close()
	require.NoErrorf(t, err, "Close: %v", err)

	select {
	case <-c.done:
	case <-time.After(1 * time.Second):
		t.Fatal("done channel not closed after Close")
	}

	err = c.Close()
	require.NoErrorf(t, err, "second Close: %v", err)
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
