package h1parser

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
)

// deadlineCountingConn counts SetWriteDeadline calls.
type deadlineCountingConn struct {
	net.Conn
	writeDeadlineCalls atomic.Int64
}

func (c *deadlineCountingConn) SetWriteDeadline(t time.Time) error {
	c.writeDeadlineCalls.Add(1)
	return c.Conn.SetWriteDeadline(t)
}

// TestServeHit_LazyWriteDeadlineRefresh verifies that consecutive hits
// within the refresh threshold do not re-arm the write deadline: the
// syscall count stays at one, and the tracked deadline is in the future.
func TestServeHit_LazyWriteDeadlineRefresh(t *testing.T) {
	t.Parallel()

	fp := &mockFastPathHit{}
	p := New(fp, noopHandler)

	client, server := dialTCPPair(t)
	defer func() { _ = client.Close() }()

	counting := &deadlineCountingConn{Conn: server}
	done := make(chan error, 1)
	go func() { done <- p.Serve(counting) }()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 512)
		have := make([]byte, 0, 512)
		for range 3 {
			_, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
			require.NoError(t, err)
			// Read the full response: status + headers + body, until the
			// "hello" body terminator arrives. The exact byte count is not
			// pinned — the mock's Connection trailer is part of the header
			// block, and leaving response bytes unread would turn the
			// client's graceful close into an RST.
			have = have[:0]
			for {
				n, err := client.Read(buf)
				require.NoError(t, err)
				have = append(have, buf[:n]...)
				if len(have) >= 5 && string(have[len(have)-5:]) == "hello" {
					break
				}
			}
		}
		_ = client.Close()
	}()
	wg.Wait()
	select {
	case err := <-done:
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return within 5s")
	}

	assert.Equal(t, int64(1), counting.writeDeadlineCalls.Load(),
		"three hits within the refresh window must arm the write deadline exactly once")
}

// TestServeHit_WriteDeadlineReArmsAfterThreshold verifies that when the
// remaining write-deadline window drops below the threshold, serveHit
// re-arms it (a second SetWriteDeadline call happens).
func TestServeHit_WriteDeadlineReArmsAfterThreshold(t *testing.T) {
	t.Parallel()

	fp := &mockFastPathHit{}
	// Shrink the write safety net to the threshold so the second hit
	// must re-arm.
	p := New(fp, noopHandler, WithWriteTimeout(writeRefreshThreshold))

	client, server := dialTCPPair(t)
	defer func() { _ = client.Close() }()

	counting := &deadlineCountingConn{Conn: server}
	done := make(chan error, 1)
	go func() { done <- p.Serve(counting) }()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 2 {
			_, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
			require.NoError(t, err)
			buf := make([]byte, 256)
			read := 0
			for read < len("HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Type: text/plain\r\n\r\nhello") {
				n, err := client.Read(buf)
				require.NoError(t, err)
				read += n
			}
			// Advance past the threshold: the remaining window after the
			// first hit is now < writeRefreshThreshold.
			if i == 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}
		_ = client.Close()
	}()
	wg.Wait()
	select {
	case err := <-done:
		if err != nil {
			require.ErrorIs(t, err, io.EOF)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return within 5s")
	}

	assert.GreaterOrEqual(t, counting.writeDeadlineCalls.Load(), int64(2),
		"a hit after the write window dropped below the threshold must re-arm the deadline")
}

// TestServeHit_WriteDeadlineTrackedInFuture asserts the tracked
// deadline is refreshed to a future value, not left at the zero time.
func TestServeHit_WriteDeadlineTrackedInFuture(t *testing.T) {
	t.Parallel()

	p := New(nil, noopHandler)
	resp := &api.FastPathResponse{
		BuffersArr: [3][]byte{
			[]byte("HTTP/1.1 200 OK\r\n"),
			[]byte("Content-Length: 0\r\n\r\n"),
			nil,
		},
	}
	resp.Buffers = resp.BuffersArr[:3]

	var wd time.Time
	now := time.Now()
	err := p.serveHit(&mockConn{}, resp, now, &wd)
	require.NoError(t, err)
	assert.True(t, wd.After(now), "tracked write deadline must be in the future")
}
