package origin

import (
	"context"
	"io"
	"log/slog"
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
