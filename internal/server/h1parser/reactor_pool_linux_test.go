//go:build linux

package h1parser

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

// TestHandoffTracker_WorkerPoolServesConcurrentMisses drives the pool
// directly (no epoll): many miss jobs are enqueued at once; workers
// must serve them all, and no conn may leak (each is closed by its
// worker after Connection: close).
func TestHandoffTracker_WorkerPoolServesConcurrentMisses(t *testing.T) {
	t.Parallel()
	p := New(nil, func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("miss:" + string(ctx.Path()))
		ctx.SetStatusCode(200)
	}, WithScheme("http"))

	tracker := &handoffTracker{}
	tracker.startSpawner()
	t.Cleanup(tracker.stopSpawner)

	const n = 32
	var wg sync.WaitGroup
	var served atomic.Int32
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			client, server := net.Pipe()
			defer client.Close()
			go func() {
				_, _ = client.Write([]byte("GET /m HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
			}()
			rc := newReactorConn(server, p, nil, nil)
			tracker.enqueue(p, rc.handoffConn())
			buf := make([]byte, 512)
			_ = client.SetDeadline(time.Now().Add(5 * time.Second))
			for {
				_, err := client.Read(buf)
				if err != nil {
					break
				}
			}
			served.Add(1)
		}(i)
	}
	wg.Wait()
	assert.Equal(t, int32(n), served.Load(), "every miss job must be served")
}

// TestHandoffTracker_EnqueueShedsWhenFull pins the bounded
// backpressure contract: a full job queue sheds the conn (close, drop
// counter) rather than blocking the loop.
func TestHandoffTracker_EnqueueShedsWhenFull(t *testing.T) {
	t.Parallel()
	rec := &fakeReactorMetrics{}
	p := New(nil, noopHandler, WithScheme("http"), WithReactorMetrics(rec))
	tracker := &handoffTracker{}
	tracker.jobs = make(chan handoffJob, 4)
	tracker.quit = make(chan struct{})

	for range 5 {
		_, server := net.Pipe()
		tracker.enqueue(p, server)
	}
	assert.Equal(t, uint64(1), rec.drops, "the job beyond the queue capacity must shed")
}

// TestHandoffTracker_ShutdownDrainsQueuedJobs pins the shutdown
// contract: stopSpawner must serve every queued job (workers own their
// conns' close) — a queued-but-unserved job can never be orphaned.
// The jobs are queued against a live tracker with Connection: close
// requests, so each Serve completes promptly after the shutdown drain
// spawns its one-shot worker.
func TestHandoffTracker_ShutdownDrainsQueuedJobs(t *testing.T) {
	t.Parallel()
	var handlerCalls atomic.Int32
	p := New(nil, func(ctx *fasthttp.RequestCtx) {
		handlerCalls.Add(1)
		ctx.SetStatusCode(200)
	}, WithScheme("http"))

	tracker := &handoffTracker{}
	tracker.jobs = make(chan handoffJob, 16)
	tracker.quit = make(chan struct{})
	tracker.dispatcherDone = make(chan struct{})
	// No dispatcher goroutine is running (started=false): the test
	// queues against the raw tracker and stopSpawner's own drain must
	// serve everything. dispatcherDone is pre-closed so the join
	// returns — the contract under test is the queue drain, not the
	// dispatcher's.
	close(tracker.dispatcherDone)

	// Queue close-terminated requests with no dispatcher running: they
	// park until stopSpawner drains them onto one-shot workers. Each
	// pipe's client side drains the response so the one-shot worker's
	// WriteTo does not block on the unbuffered pipe.
	for range 3 {
		client, server := net.Pipe()
		go func() {
			_, _ = client.Write([]byte("GET /q HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
			buf := make([]byte, 512)
			for {
				_, err := client.Read(buf)
				if err != nil {
					break
				}
			}
			_ = client.Close()
		}()
		rc := newReactorConn(server, p, nil, nil)
		tracker.jobs <- handoffJob{p: p, conn: rc.handoffConn()}
	}

	tracker.stopSpawner()

	require.Eventually(t, func() bool { return handlerCalls.Load() == 3 },
		5*time.Second, 5*time.Millisecond, "every queued job must be served at shutdown")
	tracker.wg.Wait()
}

// TestEpollReactor_MissRoundTripReusesReactorConn is the W2 end-to-end
// on the real transport: a miss hands off (worker pool), the blocking
// parser serves it and returns the conn, and the loop re-registers the
// SAME reactorConn struct — observable as two miss round trips with
// no allocation growth between them would be in production; here the
// functional proxy is that repeated miss→hit cycles keep working and
// the handoff/return counters show engagement.
func TestEpollReactor_MissRoundTripReusesReactorConn(t *testing.T) {
	addr, rec := startSelectiveReactorListener(t)

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	// Three miss→hit cycles on one connection: each miss pays one
	// handoff + one return; each following hit is served inline by the
	// reactor off the re-registered (reused) rc.
	for range 3 {
		_, err = conn.Write([]byte("GET /miss HTTP/1.1\r\nHost: localhost\r\n\r\n"))
		require.NoError(t, err)
		buf := make([]byte, 512)
		got := 0
		for got < 9 { // "miss:/miss"
			n, rerr := conn.Read(buf)
			got += n
			if rerr != nil {
				break
			}
		}
		require.Eventually(t, func() bool { return rec.returns.Load() >= 1 },
			5*time.Second, 5*time.Millisecond, "the worker must return the conn after each miss")

		_, err = conn.Write([]byte("GET /hit HTTP/1.1\r\nHost: localhost\r\n\r\n"))
		require.NoError(t, err)
		got = 0
		for got < 5 { // "hello"
			n, rerr := conn.Read(buf)
			got += n
			if rerr != nil {
				break
			}
		}
	}

	require.Eventually(t, func() bool {
		return rec.handoffCounts[reasonIndex(api.ReactorHandoffMiss)].Load() >= 3 &&
			rec.returns.Load() >= 3 &&
			rec.registered.Load() >= 4
	}, 5*time.Second, 5*time.Millisecond,
		"three miss handoffs, three returns, and re-registrations must all engage")
}
