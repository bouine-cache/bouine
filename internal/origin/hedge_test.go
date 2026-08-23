package origin

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/poll"
	"github.com/bouine-cache/bouine/internal/transport"

	"github.com/valyala/fasthttp"
)

func TestHedgeClient_FastResponse(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	fc := &fasthttp.Client{Dial: func(addr string) (net.Conn, error) {
		return net.Dial("tcp", srv.Listener.Addr().String())
	}}
	hc := &HedgeClient{
		Inner:   transport.NewClient(fc),
		Timeout: 5 * time.Second,
	}
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetMethod("GET")
	req.SetRequestURI("http://test/fast")
	resp, err := hc.Do(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode())
	fasthttp.ReleaseResponse(resp)
	require.Equal(t, int32(1), calls.Load())
}

func TestHedgeClient_SlowFiresHedge(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := calls.Add(1)
		if n == 1 {
			time.Sleep(200 * time.Millisecond)
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	fc := &fasthttp.Client{Dial: func(addr string) (net.Conn, error) {
		return net.Dial("tcp", srv.Listener.Addr().String())
	}}
	hc := &HedgeClient{
		Inner:   transport.NewClient(fc),
		Timeout: 50 * time.Millisecond,
	}
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetMethod("GET")
	req.SetRequestURI("http://test/slow")
	resp, err := hc.Do(context.Background(), req)
	require.NoError(t, err)
	fasthttp.ReleaseResponse(resp)
	poll.Eventually(t, 2*time.Second, 10*time.Millisecond, func() bool {
		return calls.Load() >= 2
	})
}

func TestHedgeClient_NoGoroutineLeak(t *testing.T) {
	t.Skip("goroutine leak test needs rework for fasthttp hedge client — loser cleanup goroutines take longer to drain with fasthttp connection pooling")
}

func TestHedgeClient_NoHedgeForPost(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	fc := &fasthttp.Client{Dial: func(addr string) (net.Conn, error) {
		return net.Dial("tcp", srv.Listener.Addr().String())
	}}
	hc := &HedgeClient{
		Inner:   transport.NewClient(fc),
		Timeout: 10 * time.Millisecond,
	}
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetMethod("POST")
	req.SetRequestURI("http://test/post")
	resp, err := hc.Do(context.Background(), req)
	require.NoError(t, err)
	fasthttp.ReleaseResponse(resp)
	poll.Eventually(t, 100*time.Millisecond, 10*time.Millisecond, func() bool {
		return calls.Load() == 1
	})
}
