// sse.go — origin fetch path for Server-Sent Events requests. A request
// that announced SSE intent (Accept: text/event-stream) is fetched through
// a dedicated pool client whose connections convert fasthttp's absolute
// read deadline into a per-read idle deadline: fasthttp arms
// min(fetch_timeout, response_header_timeout) once, before the response
// headers, and that deadline persists into the streamed body — which would
// cut every event stream after at most a few minutes. With idle semantics
// the stream lives as long as the origin keeps sending bytes within the
// idle budget, matching how streaming proxies (nginx proxy_read_timeout,
// Varnish between_bytes_timeout) treat long-lived bodies.
//
// See ADR-0042 for the full contract.
package origin

import (
	"net"
	"time"

	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// sseReadIdle is the per-read idle budget for hinted (Accept:
// text/event-stream) origin fetches: every read must make progress within
// this window, resetting on each byte received. Ten minutes tolerates
// sparse event feeds and long model "thinking" pauses while still cutting
// dead connections. TCP keep-alive (connect.keep_alive) remains the
// backstop for silently-dead peers.
//
// Trade-off: the headers phase of a hinted fetch is also idle-bounded
// instead of the absolute response_header_timeout — a hung origin can pin
// one fetch slot for up to this budget instead of 30s. Only requests that
// explicitly announce stream intent take this path.
const sseReadIdle = 10 * time.Minute

// sseReadConn converts the absolute read deadline armed by fasthttp's
// transport into per-read idle semantics. fasthttp sets the deadline once
// (before reading the response headers) and it persists for the whole
// streamed body; re-arming before every Read bounds each read by the idle
// budget instead, so a live stream is never cut by an absolute wall clock.
//
// The wrapper sits below TLS (fasthttp wraps the dialed conn), which
// composes: deadline calls propagate from the tls.Conn through the
// wrapper, and re-arms on the raw conn bound TLS reads.
type sseReadConn struct {
	net.Conn
	idle time.Duration
}

func (c *sseReadConn) Read(p []byte) (int, error) {
	// sseReadConn does not override SetReadDeadline, so the selector-free
	// call resolves to the embedded conn's method — no recursion.
	_ = c.SetReadDeadline(time.Now().Add(c.idle))
	return c.Conn.Read(p)
}

// newOriginStreamClient builds the fasthttp client used for hinted SSE
// fetches. It shares the pool's resolved connect settings (targets, dial
// timeout, connection caps) but its Dial wraps every connection in
// sseReadConn. Conns from this client are pooled independently from the
// general-purpose client, so the idle-deadline semantics never leak into
// ordinary fetches.
func newOriginStreamClient(cc clientConfig) *fasthttp.Client {
	dialer := &net.Dialer{Timeout: cc.dialTimeout, KeepAlive: cc.keepAlive}
	c := &fasthttp.Client{
		MaxConnsPerHost:     cc.maxConnsPerHost,
		MaxIdleConnDuration: cc.maxIdleConnDuration,
		ReadTimeout:         0,
		WriteTimeout:        5 * time.Minute,
		Dial: func(addr string) (net.Conn, error) {
			conn, err := dialer.Dial("tcp", addr)
			if err != nil {
				return nil, err
			}
			return &sseReadConn{Conn: conn, idle: sseReadIdle}, nil
		},
	}
	return c
}

// isSSERequest reports whether the outbound request announced SSE intent.
// PoolFastClient routes such requests to the stream client.
func isSSERequest(req *fasthttp.Request) bool {
	return header.AcceptsEventStream(req.Header.Peek(header.Accept))
}
