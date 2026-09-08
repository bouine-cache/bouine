package cmd

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"

	"github.com/valyala/fasthttp"
)

// TestSSEEndToEnd proves the full SSE contract through a real bouine
// daemon: a client announcing Accept: text/event-stream receives each
// event as the origin emits it — before the stream completes — with
// nothing buffered, nothing cached, and the connection staying open for
// the whole (slow) stream. A buffering proxy would deliver nothing until
// the origin closed the stream; the per-event read deadlines here would
// time out instead.
func TestSSEEndToEnd(t *testing.T) {
	const eventGap = 300 * time.Millisecond
	const events = 3

	eventWritten := make(chan int, events)
	originSrv := fasthttptest.NewServer(t, sseOriginHandler(eventGap, events, eventWritten))
	defer originSrv.Close()

	dir := t.TempDir()
	cfg := fmt.Sprintf(`
listen:
  http:  "127.0.0.1:18095"
  admin: "127.0.0.1:18096"
upstream_pools:
  - name: ai
    targets: [%q]
routes:
  - match: {}
    pool: ai
`, originSrv.Addr)
	cfgPath := filepath.Join(dir, "bouine.yaml")
	err := os.WriteFile(cfgPath, []byte(cfg), 0o600)
	require.NoError(t, err)

	root := Root()
	root.SetArgs([]string{"serve", "--config", cfgPath, "--log-format", "text"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- root.ExecuteContext(ctx) }()
	waitForPort(t, "127.0.0.1:18095")
	waitForPort(t, "127.0.0.1:18096")

	// Raw SSE client: one connection, events read incrementally.
	conn, err := net.DialTimeout("tcp", "127.0.0.1:18095", 2*time.Second)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	_, err = conn.Write([]byte("GET /v1/stream HTTP/1.1\r\nHost: ai.local\r\nAccept: text/event-stream\r\n\r\n"))
	require.NoError(t, err)

	br := bufio.NewReader(conn)
	readLineWithDeadline := func(d time.Duration) string {
		_ = conn.SetReadDeadline(time.Now().Add(d))
		line, err := br.ReadString('\n')
		require.NoError(t, err, "event line must arrive while the stream is open")
		return line
	}

	// Headers first.
	status := readLineWithDeadline(2 * time.Second)
	require.Contains(t, status, "200")
	var sawContentType, sawXCache bool
	for range 20 {
		line := readLineWithDeadline(2 * time.Second)
		if line == "\r\n" {
			break
		}
		if strings.HasPrefix(line, "Content-Type: text/event-stream") {
			sawContentType = true
		}
		if line == "X-Cache: BYPASS\r\n" {
			sawXCache = true
		}
	}
	require.True(t, sawContentType, "Content-Type must pass through")
	require.True(t, sawXCache, "SSE responses are served as BYPASS")

	// Each event must arrive as the origin emits it. The first event is
	// the strict proof: its data line must be readable within 200ms of the
	// origin writing it, while the origin will not even emit event 2
	// before another eventGap (300ms) and the whole stream runs ~900ms — a
	// buffering proxy would deliver nothing until the stream ended and
	// this deadline would expire.
	readEventData := func(i int, d time.Duration) {
		select {
		case <-eventWritten:
		case <-time.After(3 * time.Second):
			t.Fatalf("origin did not emit event %d in time", i+1)
		}
		for {
			line := readLineWithDeadline(d)
			if strings.Contains(line, fmt.Sprintf("data: {\"n\":%d}", i+1)) {
				return
			}
			// Chunk-size and framing lines are skipped.
		}
	}

	readEventData(0, 200*time.Millisecond) // strict: no buffering possible
	for i := 1; i < events; i++ {
		readEventData(i, 2*time.Second)
	}

	// Drain the framing tail (terminal chunk / EOF-delimited close).
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		_, err := br.ReadByte()
		if err != nil {
			break // stream closed after the last event
		}
	}

	cancel()
	select {
	case err := <-errCh:
		require.NoError(t, err, "serve")
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not shut down")
	}
}

// sseOriginHandler returns a streaming origin that emits n SSE events,
// one every gap, flushing each so the wire carries it immediately.
func sseOriginHandler(gap time.Duration, events int, written chan int) func(*fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("Content-Type", "text/event-stream")
		ctx.Response.Header.Set("Cache-Control", "no-store")
		ctx.SetStatusCode(200)
		ctx.Response.SetBodyStreamWriter(func(w *bufio.Writer) {
			for i := range events {
				_, _ = fmt.Fprintf(w, "data: {\"n\":%d}\n\n", i+1)
				_ = w.Flush()
				written <- i
				time.Sleep(gap)
			}
		})
	}
}
