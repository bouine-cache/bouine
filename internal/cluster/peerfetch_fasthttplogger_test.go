package cluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/testutil/poll"

	"github.com/valyala/fasthttp"
)

// TestPeerFetcher_PipelineClientLoggerWired pins the fix for the prod-eu
// log export (2026-09-16): every entry was "error in PipelineClient(...)"
// emitted by fasthttp's pipeline worker through its stderr defaultLogger,
// bypassing slog entirely. The per-peer PipelineClient must carry the
// client-side adapter so those diagnostics reach the structured pipeline.
func TestPeerFetcher_PipelineClientLoggerWired(t *testing.T) {
	t.Parallel()

	f := NewPeerFetcherWithConfig(PeerFetcherConfig{}, nil, nil)
	defer f.Close(context.Background())

	pc := f.getPipelineClient("10.0.0.99:9000")
	require.NotNil(t, pc)
	assert.Equal(t, observability.NewFastHTTPClientLogger(f.logger, "cluster"), pc.Logger)
}

// TestPeerFetcher_NilLoggerResolvesToNoop mirrors the other constructors:
// a fetcher built without a logger must mint PipelineClients whose logger
// is the noop adapter, never a nil fasthttp.Logger (which would fall back
// to fasthttp's stderr defaultLogger).
func TestPeerFetcher_NilLoggerResolvesToNoop(t *testing.T) {
	t.Parallel()

	f := NewPeerFetcherWithConfig(PeerFetcherConfig{}, nil, nil)
	defer f.Close(context.Background())

	pc := f.getPipelineClient("10.0.0.99:9000")
	require.NotNil(t, pc)
	assert.Equal(t, observability.NewFastHTTPClientLogger(observability.NoopLogger{}, "cluster"), pc.Logger)
	require.NotPanics(t, func() {
		pc.Logger.Printf(`error in PipelineClient(%q): %v`, "10.0.0.99:9000", "EOF")
	})
}

// TestPeerFetcher_PipelineWorkerLogsThroughSlog proves the wiring
// end-to-end: a pipeline worker whose peer refuses dials emits its
// diagnostic through the structured pipeline at WARN (degraded peer)
// with the cluster component tag, not to fasthttp's stderr
// defaultLogger.
//
// The Do call never returns once the dial fails (the worker drains its
// queue only after a successful dial), so it is abandoned on a
// goroutine, and the worker is parked afterwards by retiring the
// address: the same production mechanism for a peer that leaves the
// ring. The abandoned goroutine then parks on the retired mark until
// fetcher close releases it, matching the by-design retirement
// lifecycle (bounded at one goroutine per retired address).
func TestPeerFetcher_PipelineWorkerLogsThroughSlog(t *testing.T) {
	t.Parallel()

	logger, mu, buf := captureLogger(t)
	f := NewPeerFetcherWithConfig(PeerFetcherConfig{}, nil, logger)
	defer f.Close(context.Background())

	// Nothing listens on 127.0.0.1:1, so the worker logs
	// "dial tcp ... connect: connection refused" through our adapter.
	const addr = "127.0.0.1:1"
	pc := f.getPipelineClient(addr)
	req := fasthttp.AcquireRequest()
	req.SetRequestURI("http://127.0.0.1/x")
	resp := fasthttp.AcquireResponse()
	go func() { _ = pc.Do(req, resp) }()

	poll.Eventually(t, 3*time.Second, 5*time.Millisecond, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return strings.Contains(buf.String(), "error in PipelineClient")
	})

	// Park the worker instead of letting it re-dial a refused port in
	// a hot loop (refused dials are not net timeouts, so fasthttp does
	// not throttle its restart) for the life of the test binary.
	f.RetireAddress(addr)

	mu.Lock()
	out := buf.String()
	mu.Unlock()
	assert.Contains(t, out, "error in PipelineClient")
	assert.Contains(t, out, "connection refused")
	assert.Contains(t, out, `"component":"cluster"`)
	for line := range strings.SplitSeq(out, "\n") {
		if strings.Contains(line, "error in PipelineClient") {
			assert.Contains(t, line, `"level":"WARN"`)
		}
	}
}
