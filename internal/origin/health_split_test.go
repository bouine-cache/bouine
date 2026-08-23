package origin

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"

	"github.com/valyala/fasthttp"
)

func TestActiveHealth_AccumulatesDespitePassiveTraffic(t *testing.T) {
	t.Parallel()
	bad := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(500)
	})
	defer bad.Close()
	good := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(200)
	})
	defer good.Close()

	p := pool(t, bad.Addr, good.Addr)
	h := p.FastHandler(100)

	hc := NewActiveHealthChecker(p, ActiveHealthConfig{
		Path:               "/",
		Interval:           10 * time.Millisecond,
		Timeout:            1 * time.Second,
		UnhealthyThreshold: 3,
		ExpectedCodes:      []int{200},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = hc.Run(ctx)
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				ctx2 := &fasthttp.RequestCtx{}
				ctx2.Request.Header.SetMethod("GET")
				ctx2.Request.SetRequestURI("http://test/")
				h(ctx2)
			}
		}
	}()

	wg.Wait()

	healthy := p.Healthy()
	require.Len(t, healthy, 1)
	require.Equal(t, good.Addr, healthy[0])
}

func TestPassiveHealth_ErrorHandlerEjects(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(200)
	})
	addr := srv.Addr
	srv.Close()

	p := pool(t, addr)
	h := p.FastHandler(3)

	for range 5 {
		ctx := &fasthttp.RequestCtx{}
		ctx.Request.Header.SetMethod("GET")
		ctx.Request.SetRequestURI("http://test/")
		h(ctx)
		require.Equal(t, fasthttp.StatusBadGateway, ctx.Response.StatusCode())
	}

	require.Len(t, p.Healthy(), 0)
}

func TestPassiveHealth_DisabledDoesNotZeroCounters(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(200)
	})
	defer srv.Close()

	p := pool(t, srv.Addr)
	h := p.FastHandler(0)

	for range 3 {
		ctx := &fasthttp.RequestCtx{}
		ctx.Request.Header.SetMethod("GET")
		ctx.Request.SetRequestURI("http://test/")
		h(ctx)
	}

	p.targets[0].passiveErrors.Store(42)
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/")
	h(ctx)

	got := p.targets[0].passiveErrors.Load()
	require.Equal(t, int64(42), got)
}

func TestMarkHealthy_CAS(t *testing.T) {
	t.Parallel()
	bad := fivexxServer(t)
	defer bad.Close()

	p := pool(t, bad.Addr)
	h := p.FastHandler(1)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/")
	h(ctx)
	require.Len(t, p.Healthy(), 0)

	p.targets[0].passiveErrors.Store(5)
	p.targets[0].probeErrors.Store(3)
	p.targets[0].successes.Store(2)

	p.MarkHealthy(bad.Addr)
	require.Len(t, p.Healthy(), 1)

	got := p.targets[0].passiveErrors.Load()
	require.Equal(t, int64(0), got)
	got = p.targets[0].probeErrors.Load()
	require.Equal(t, int64(0), got)
	got = p.targets[0].successes.Load()
	require.Equal(t, int64(0), got)
}

func TestConcurrent_ActiveAndPassive(t *testing.T) {
	t.Parallel()
	bad := fivexxServer(t)
	defer bad.Close()

	p := pool(t, bad.Addr)
	h := p.FastHandler(2)

	hc := NewActiveHealthChecker(p, ActiveHealthConfig{
		Path:               "/",
		Interval:           5 * time.Millisecond,
		Timeout:            1 * time.Second,
		UnhealthyThreshold: 5,
		ExpectedCodes:      []int{200},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				c := &fasthttp.RequestCtx{}
				c.Request.Header.SetMethod("GET")
				c.Request.SetRequestURI("http://test/")
				h(c)
			}
		}
	}()

	go func() {
		defer wg.Done()
		_ = hc.Run(ctx)
	}()

	wg.Wait()
}
