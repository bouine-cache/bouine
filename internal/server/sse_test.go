package server

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// TestSSEHeaderReceived pins the per-request write-deadline override on the
// non-fast-path serving mode: a request announcing SSE intent gets the
// extended (1h) write deadline so an event stream is not cut by the 5-minute
// safety net; every other request keeps the server defaults (a zero
// RequestConfig falls back to the server-level WriteTimeout).
func TestSSEHeaderReceived(t *testing.T) {
	t.Parallel()

	sse := &fasthttp.RequestHeader{}
	sse.Set(header.Accept, "application/json, text/event-stream")
	got := sseHeaderReceived(sse)
	require.Equal(t, sseWriteTimeout, got.WriteTimeout)
	require.Positive(t, got.WriteTimeout)

	plain := &fasthttp.RequestHeader{}
	plain.Set(header.Accept, "text/html,application/xhtml+xml,*/*;q=0.8")
	got = sseHeaderReceived(plain)
	require.Zero(t, got.WriteTimeout, "non-SSE requests keep the server default")

	browser := &fasthttp.RequestHeader{}
	browser.Set(header.Accept, "*/*")
	got = sseHeaderReceived(browser)
	require.Zero(t, got.WriteTimeout, "a bare */* is not SSE intent")
}

// TestSSEWriteTimeoutExceedsSafetyNet pins the invariant that the SSE
// write horizon comfortably exceeds the 5-minute safety net it overrides.
func TestSSEWriteTimeoutExceedsSafetyNet(t *testing.T) {
	t.Parallel()
	require.Greater(t, sseWriteTimeout, 10*safetyNetWriteTimeout)
}
