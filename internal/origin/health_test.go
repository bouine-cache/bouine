package origin

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"

	"github.com/valyala/fasthttp"
)

func TestActiveHealth_RecoversTarget(t *testing.T) {
	t.Parallel()
	healthy := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(200)
	})
	defer healthy.Close()

	p := pool(t, healthy.Addr)
	p.targets[0].healthy.Store(false)
	p.targets[0].probeErrors.Store(10)

	require.Len(t, p.Healthy(), 0)

	hc := NewActiveHealthChecker(p, ActiveHealthConfig{
		Path:               "/",
		Interval:           10 * time.Millisecond,
		Timeout:            1 * time.Second,
		UnhealthyThreshold: 3,
		ExpectedCodes:      []int{200},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_ = hc.Run(ctx)

	require.Len(t, p.Healthy(), 1)
}

func TestActiveHealth_EjectsUnhealthy(t *testing.T) {
	t.Parallel()
	bad := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(500)
	})
	defer bad.Close()

	p := pool(t, bad.Addr)

	hc := NewActiveHealthChecker(p, ActiveHealthConfig{
		Path:               "/",
		Interval:           10 * time.Millisecond,
		Timeout:            1 * time.Second,
		UnhealthyThreshold: 2,
		ExpectedCodes:      []int{200},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_ = hc.Run(ctx)

	require.Len(t, p.Healthy(), 0)
}

type countingListener struct {
	net.Listener
	accepts *atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.accepts.Add(1)
	}
	return c, err
}

// TestActiveHealth_ProbeDoesNotReuseIdleConn pins issue #912: a probe
// connection sits idle for a full interval between probes, and origins
// that close idle keep-alives earlier turn that reuse into a RST. Every
// probe must dial a fresh connection and send Connection: close.
func TestActiveHealth_ProbeDoesNotReuseIdleConn(t *testing.T) {
	t.Parallel()

	var accepts atomic.Int64
	var sawConnClose atomic.Bool

	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	counting := &countingListener{Listener: ln, accepts: &accepts}

	srv := &fasthttp.Server{
		Handler: func(ctx *fasthttp.RequestCtx) {
			sawConnClose.Store(ctx.Request.Header.ConnectionClose())
			ctx.SetStatusCode(200)
		},
		IdleTimeout: 50 * time.Millisecond,
	}
	go func() { _ = srv.Serve(counting) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.ShutdownWithContext(ctx)
		_ = ln.Close()
	}()

	p := pool(t, counting.Addr().String())
	hc := NewActiveHealthChecker(p, ActiveHealthConfig{
		Path:               "/",
		Interval:           time.Hour,
		Timeout:            1 * time.Second,
		UnhealthyThreshold: 3,
		ExpectedCodes:      []int{200},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	hc.probeAll(context.Background())
	hc.probeAll(context.Background())

	require.True(t, sawConnClose.Load(), "probe request must carry Connection: close")
	require.Equal(t, int64(2), accepts.Load(), "each probe must dial a fresh connection")
	require.Len(t, p.Healthy(), 1)
}

// TestActiveHealth_ProbeFailuresExcusedByRecentSuccess pins issue #914:
// against a multi-endpoint target (Service VIP, one random pod per probe
// connection), a probe failure is excused while any probe succeeded
// within the window — a minority of sick pods must not eject the target.
func TestActiveHealth_ProbeFailuresExcusedByRecentSuccess(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	p := pool(t, "127.0.0.1:1")
	target := p.targets[0]
	const window = 10 * time.Second

	t.Run("never probed successfully ejects by streak alone", func(t *testing.T) {
		t.Parallel()
		for range 3 {
			target.recordProbeError(3, window, logger, "test")
		}
		require.False(t, target.healthy.Load(), "zero last-probe-success never gates")
	})

	t.Run("recent success excuses the failure streak", func(t *testing.T) {
		t.Parallel()
		target2 := pool(t, "127.0.0.1:1").targets[0]
		target2.recordProbeSuccess(2, logger, "test")
		for range 10 {
			target2.recordProbeError(3, window, logger, "test")
		}
		require.True(t, target2.healthy.Load(),
			"successes within the window excuse every probe failure")
		require.Zero(t, target2.probeErrors.Load(), "excused failures never reach the streak")
	})

	t.Run("stale success no longer excuses", func(t *testing.T) {
		t.Parallel()
		target3 := pool(t, "127.0.0.1:1").targets[0]
		target3.recordProbeSuccess(2, logger, "test")
		target3.lastProbeSuccessUnix.Store(time.Now().Add(-window - time.Second).UnixNano())
		for range 3 {
			target3.recordProbeError(3, window, logger, "test")
		}
		require.False(t, target3.healthy.Load(),
			"once probe successes stop for the window, threshold-many failures eject")
	})
}
