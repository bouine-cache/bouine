package origin

import (
	"bufio"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/valyala/fasthttp"
)

// newHostEchoOrigin serves a listener that reads one request head and
// replies 200 with the Host header it received on the wire, so the
// caller can assert which Host the origin-bound request carried.
func newHostEchoOrigin(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				br := bufio.NewReader(c)
				var host string
				for {
					line, err := br.ReadString('\n')
					if err != nil {
						return
					}
					if line == "\r\n" || line == "\n" {
						break
					}
					if strings.HasPrefix(strings.ToLower(line), "host:") {
						host = strings.TrimSpace(line[len("host:"):])
					}
				}
				body := "host=" + host
				_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: "+itoa(len(body))+"\r\n\r\n"+body)
			}(conn)
		}
	}()
	return ln.Addr().String()
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// TestPoolFastClient_DefaultPoolTargetAsHost pins the historical
// behaviour: on a pool without preserve_host, fasthttp's absolute-form
// request URI makes Request.Write replace the outbound Host header with
// the pool target, whatever Host the request carried.
func TestPoolFastClient_DefaultPoolTargetAsHost(t *testing.T) {
	t.Parallel()

	addr := newHostEchoOrigin(t)
	p, err := NewPool(PoolConfig{
		Name:    "default-host",
		Targets: []string{"http://" + addr},
		Logger:  newDiscardLogger(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close(t.Context()) })
	client := p.FastClient()

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	req.Header.SetMethod("GET")
	req.SetRequestURI("/v2/products/?pid=1")
	req.Header.SetHost("www.example.com")

	require.NoError(t, client.Do(t.Context(), req, resp))
	require.Equal(t, 200, resp.StatusCode())
	require.Equal(t, "host="+addr, string(resp.Body()), "default pools must send the pool target as Host")

	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
}

// TestPoolFastClient_PreserveHostKeepsRequestHost pins the fix: with
// preserve_host, the origin-bound request keeps the request's own Host
// header (the cache handler sets it to the client's Host), while the
// dial target remains the pool target.
func TestPoolFastClient_PreserveHostKeepsRequestHost(t *testing.T) {
	t.Parallel()

	addr := newHostEchoOrigin(t)
	p, err := NewPool(PoolConfig{
		Name:         "preserve-host",
		Targets:      []string{"http://" + addr},
		Logger:       newDiscardLogger(),
		PreserveHost: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close(t.Context()) })
	client := p.FastClient()

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	req.Header.SetMethod("GET")
	req.SetRequestURI("/v2/products/?pid=1")
	req.Header.SetHost("www.example.com")

	require.NoError(t, client.Do(t.Context(), req, resp))
	require.Equal(t, 200, resp.StatusCode(), "the dial target must remain the pool target")
	require.Equal(t, "host=www.example.com", string(resp.Body()), "the origin must see the request's own Host")

	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
}

// TestPoolFastClient_PreserveHostHedgeDuplicate pins the hedge path:
// doHedgedFetch duplicates the request via CopyTo, which carries
// UseHostHeader, so a hedged fetch on a preserve_host pool must keep
// the client Host too (the duplicate wins as often as the primary).
func TestPoolFastClient_PreserveHostHedgeDuplicate(t *testing.T) {
	t.Parallel()

	addr := newHostEchoOrigin(t)
	p, err := NewPool(PoolConfig{
		Name:         "preserve-host-hedged",
		Targets:      []string{"http://" + addr},
		Logger:       newDiscardLogger(),
		PreserveHost: true,
		HedgeTimeout: 20 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close(t.Context()) })
	client := p.FastClient()

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	req.Header.SetMethod("GET")
	req.SetRequestURI("/v2/products/?pid=1")
	req.Header.SetHost("www.example.de")

	require.NoError(t, client.Do(t.Context(), req, resp))
	require.Equal(t, 200, resp.StatusCode())
	require.Equal(t, "host=www.example.de", string(resp.Body()), "hedged duplicates must keep the client Host")

	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
}

// TestPoolFastHandler_PreserveHost pins the proxy bypass path
// (FastHandler): preserve_host pools forward the client's Host there
// too, instead of the pool target FastHandler itself sets.
func TestPoolFastHandler_PreserveHost(t *testing.T) {
	t.Parallel()

	addr := newHostEchoOrigin(t)
	p, err := NewPool(PoolConfig{
		Name:         "preserve-host-proxy",
		Targets:      []string{"http://" + addr},
		Logger:       newDiscardLogger(),
		PreserveHost: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close(t.Context()) })
	handler := p.FastHandler(0)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("/v2/products/?pid=1")
	ctx.Request.Header.SetHost("www.example.org")

	// FastHandler reads from the ctx; invoke it directly.
	handler(ctx)

	require.Equal(t, 200, ctx.Response.StatusCode())
	require.Equal(t, "host=www.example.org", string(ctx.Response.Body()), "the proxy path must keep the client Host")
}
