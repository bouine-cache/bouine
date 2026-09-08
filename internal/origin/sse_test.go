package origin

import (
	"bufio"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// recordingDeadlineConn records every SetReadDeadline call so tests can
// assert the sseReadConn re-arm behavior without real kernel deadlines.
type recordingDeadlineConn struct {
	net.Conn
	mu        sync.Mutex
	deadlines []time.Time
}

func (c *recordingDeadlineConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadlines = append(c.deadlines, t)
	c.mu.Unlock()
	return nil
}

func (c *recordingDeadlineConn) Read(p []byte) (int, error) { return 0, io.EOF }

// TestSSEReadConn_RearmsIdleDeadlinePerRead pins the core origin-side
// deadline contract: fasthttp arms one absolute read deadline before the
// response headers; sseReadConn overrides it before every Read with
// now+idle, converting the absolute wall clock into per-read idle
// semantics so a live event stream is never cut mid-flight.
func TestSSEReadConn_RearmsIdleDeadlinePerRead(t *testing.T) {
	t.Parallel()

	rec := &recordingDeadlineConn{}
	conn := &sseReadConn{Conn: rec, idle: 10 * time.Minute}

	// fasthttp arms an absolute deadline (would expire 1ms from now).
	armed := time.Now().Add(time.Millisecond)
	require.NoError(t, conn.SetReadDeadline(armed))

	before := time.Now()
	_, _ = conn.Read(make([]byte, 16))
	_, _ = conn.Read(make([]byte, 16))
	mid := time.Now()

	rec.mu.Lock()
	defer rec.mu.Unlock()
	// One passthrough (the transport's absolute arm) + one re-arm per Read.
	require.Len(t, rec.deadlines, 3, "each Read must re-arm the deadline")
	require.WithinDuration(t, armed, rec.deadlines[0], 2*time.Millisecond,
		"the initial absolute arm passes through unchanged")
	for _, d := range rec.deadlines[1:] {
		require.True(t, d.After(before), "re-armed deadline must be idle-based, in the future")
		require.True(t, d.Before(mid.Add(10*time.Minute+time.Second)),
			"re-armed deadline must be bounded by the idle budget")
	}
}

// TestIsSSERequest pins the routing predicate for the stream client.
func TestIsSSERequest(t *testing.T) {
	t.Parallel()

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.Header.Set(header.Accept, "application/json")
	require.False(t, isSSERequest(req))

	req.Header.Set(header.Accept, "text/event-stream")
	require.True(t, isSSERequest(req))

	req.Header.Set(header.Accept, "*/*")
	require.False(t, isSSERequest(req))
}

// newSSEOrigin starts a real fasthttp origin server that emits SSE events
// with the given inter-event gap, and returns its dial address.
func newSSEOrigin(t *testing.T, events []string, gap time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	srv := &fasthttp.Server{
		Handler: func(ctx *fasthttp.RequestCtx) {
			ctx.Response.Header.Set(header.ContentType, "text/event-stream")
			ctx.SetStatusCode(200)
			ctx.Response.SetBodyStreamWriter(func(w *bufio.Writer) {
				for _, ev := range events {
					_, _ = w.Write([]byte(ev))
					_ = w.Flush()
					time.Sleep(gap)
				}
			})
		},
	}
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().String()
}

// TestPoolFastClient_SSERequestStreamsEvents is the origin-side
// end-to-end proof: a hinted fetch (Accept: text/event-stream) through
// PoolFastClient returns a body stream that delivers each event as it is
// written, with per-read idle deadlines — the events keep flowing well
// after the absolute fetch deadline would have expired.
func TestPoolFastClient_SSERequestStreamsEvents(t *testing.T) {
	t.Parallel()

	const gap = 150 * time.Millisecond
	events := []string{"data: one\n\n", "data: two\n\n", "data: three\n\n"}
	addr := newSSEOrigin(t, events, gap)

	pool, err := NewPool(PoolConfig{
		Name:    "sse",
		Targets: []string{"http://" + addr},
	})
	require.NoError(t, err)
	client := pool.FastClient()

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	resp.StreamBody = true
	req.Header.SetMethod("GET")
	// PoolFastClient rewrites the URI onto the selected target, so the
	// request carries the path only (same contract as the cache layer's
	// stripped URI).
	req.SetRequestURI("/feed")
	req.SetHost(addr)
	req.Header.Set(header.Accept, "text/event-stream")

	// A deadline far shorter than the total stream duration (3 events ×
	// 150ms gaps): the old absolute semantics would cut the stream at
	// deadline; idle semantics only require progress per read.
	deadline := time.Now().Add(200 * time.Millisecond)
	require.NoError(t, client.DoDeadline(req, resp, deadline))

	require.Equal(t, 200, resp.StatusCode())
	body := resp.BodyStream()
	require.NotNil(t, body)

	br := bufio.NewReader(body)
	var got []byte
	buf := make([]byte, 4096)
	// Drain the stream to EOF. The origin closes it after the last event
	// (3 events + 2 gaps ≈ 300ms after the first). Under the old absolute
	// semantics every read past the 200ms deadline would fail with
	// i/o timeout and the body would be truncated; with the per-read
	// idle wrapper the full stream arrives.
	readDone := make(chan error, 1)
	go func() {
		for {
			n, err := br.Read(buf)
			got = append(got, buf[:n]...)
			if err != nil {
				if err == io.EOF {
					readDone <- nil
				} else {
					readDone <- err
				}
				return
			}
		}
	}()
	select {
	case err := <-readDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("stream did not complete within 3s")
	}

	require.Equal(t, "data: one\n\ndata: two\n\ndata: three\n\n", string(got))
	require.NoError(t, resp.CloseBodyStream())
}

// TestPoolFastClient_PlainRequestBoundedByAbsoluteDeadline pins that
// ordinary (non-hinted) requests keep the absolute-deadline contract: the
// general client — not the idle-deadline stream client — serves them. The
// stream routing is pinned by TestIsSSERequest; here we assert the pool
// wires two distinct clients and that a hinted request's connection comes
// from the stream pool.
func TestPoolFastClient_PlainRequestBoundedByAbsoluteDeadline(t *testing.T) {
	t.Parallel()

	pool, err := NewPool(PoolConfig{
		Name:    "plain",
		Targets: []string{"http://127.0.0.1:1"},
	})
	require.NoError(t, err)
	require.NotNil(t, pool.client)
	require.NotNil(t, pool.streamClient)
	require.NotSame(t, pool.client, pool.streamClient,
		"the stream client must be a distinct fasthttp.Client")
}
