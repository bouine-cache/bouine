package origin

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"

	"github.com/valyala/fasthttp"
)

// newMixedHandler answers 200 on /ok and 503 on every other path — an
// origin that is healthy overall but deliberately returns 5xx on some
// paths (rate limiting, an error endpoint), the issue #839 shape.
func newMixedHandler() fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		if string(ctx.Path()) == "/ok" {
			ctx.SetStatusCode(200)
			_, _ = ctx.WriteString("ok")
			return
		}
		ctx.SetStatusCode(503)
		_, _ = ctx.WriteString("err")
	}
}

// TestRecordPassive5xx_SuccessWindowGate pins the issue #839 semantics at
// the accounting level: an origin 5xx response only drives passive
// ejection once successful fetches have stopped for the success window.
// A recent success excuses any 5xx burst no matter how the completions
// interleave; a target that has never answered, or whose last success is
// older than the window, still ejects by the consecutive count; and a
// connection error — the origin not answering at all — is never excused
// by a recent success.
func TestRecordPassive5xx_SuccessWindowGate(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	newTarget := func() *Target {
		p, err := NewPool(PoolConfig{
			Name:           "api",
			Targets:        []string{"127.0.0.1:1"},
			Consecutive5xx: 3,
			Logger:         logger,
		})
		require.NoError(t, err, "NewPool")
		return p.targets[0]
	}

	t.Run("never answered ejects by count alone", func(t *testing.T) {
		t.Parallel()
		target := newTarget()
		for range 3 {
			target.recordPassive5xx(3, logger, "api", "503")
		}
		require.False(t, target.healthy.Load(), "no success ever: threshold-many 5xx eject")
	})

	t.Run("recent success excuses a 5xx burst", func(t *testing.T) {
		t.Parallel()
		target := newTarget()
		target.notePassiveSuccess()
		for range 10 {
			target.recordPassive5xx(3, logger, "api", "503")
		}
		require.True(t, target.healthy.Load(),
			"an origin answering successes is never ejected by 5xx completions alone")
		require.Zero(t, target.passiveErrors.Load(), "excused 5xx never reach the streak")
	})

	t.Run("stale success no longer excuses", func(t *testing.T) {
		t.Parallel()
		target := newTarget()
		target.notePassiveSuccess()
		target.lastSuccessUnix.Store(time.Now().Add(-passive5xxCountWindow - time.Second).UnixNano())
		for range 3 {
			target.recordPassive5xx(3, logger, "api", "503")
		}
		require.False(t, target.healthy.Load(),
			"once successes have stopped for the window, threshold-many 5xx eject")
	})

	t.Run("connection errors are never excused", func(t *testing.T) {
		t.Parallel()
		target := newTarget()
		target.notePassiveSuccess()
		for range 3 {
			target.recordPassiveError(3, logger, "api", "connection error", "refused")
		}
		require.False(t, target.healthy.Load(),
			"a transport failure is the origin not answering: no success window applies")
	})
}

// TestPool_Passive5xxBurstWithSuccessesDoesNotEject pins the gate end to
// end on the proxied path: an origin answering 200s on one path and 503s
// on another never ejects while the successes keep flowing — every
// request is answered by the origin (no "no healthy upstream" 502s) —
// and still ejects once successes have stopped past the window and the
// 5xx count reaches the threshold.
func TestPool_Passive5xxBurstWithSuccessesDoesNotEject(t *testing.T) {
	t.Parallel()
	origin := fasthttptest.NewServer(t, newMixedHandler())
	defer origin.Close()

	p := pool(t, origin.Addr)
	h := p.FastHandler(3)

	// Interleaved successes and 5xx: ten rounds of one /ok then one
	// /err. Completion order cannot matter — whichever way the /err
	// 5xx lands between the /ok successes, the most recent success is
	// within the window and every 5xx is excused.
	for range 10 {
		code, _, _ := serveHandler(t, h, "GET", "/ok", "")
		require.Equal(t, 200, code, "the success leg is served by the origin")
		code, _, _ = serveHandler(t, h, "GET", "/err", "")
		require.Equal(t, 503, code, "the 5xx leg is served by the origin, not a 502")
	}
	require.Len(t, p.Healthy(), 1,
		"a healthy origin answering successes on one path is never ejected by 5xx on another")

	// Successes stop: once the last success is older than the window,
	// threshold-many 5xx completions eject. Backdated instead of
	// sleeping — the gate reads a timestamp, not elapsed wall time.
	p.targets[0].lastSuccessUnix.Store(time.Now().Add(-passive5xxCountWindow - time.Second).UnixNano())
	for range 3 {
		code, _, _ := serveHandler(t, h, "GET", "/err", "")
		require.Equal(t, 503, code)
	}
	require.Len(t, p.Healthy(), 0, "successes stopped past the window: consecutive 5xx ejects")
}
