package origin

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/internal/testutil/poll"
)

// TestEjectReaper_RestoresAfterWindow verifies the eject_for contract:
// a passively ejected target comes back once the window elapses.
func TestEjectReaper_RestoresAfterWindow(t *testing.T) {
	t.Parallel()
	bad := fasthttptest.NewServer(t, new5xxHandler())
	defer bad.Close()

	p, err := NewPool(PoolConfig{
		Name:     "test",
		Targets:  []string{bad.Addr},
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		EjectFor: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	// Eject the target via the FastHandler passive path.
	h := p.FastHandler(1)
	serveHandler(t, h, "GET", "/", "")
	require.Len(t, p.Healthy(), 0, "target must be ejected after the threshold")

	reaper := NewEjectReaper(p)
	require.NotNil(t, reaper)

	// Fast-forward the ejection timestamp so the window has elapsed.
	for _, tgt := range p.targets {
		tgt.ejectedAt.Store(time.Now().Add(-100 * time.Millisecond).UnixNano())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go reaper.Run(ctx)

	poll.Eventually(t, 2*time.Second, 20*time.Millisecond, func() bool {
		return len(p.Healthy()) == 1
	})
}

// TestEjectReaper_NilWithoutEjectFor verifies that pools that can
// never eject get no reaper: no passive health means the zero window
// stays zero (the built-in default only applies to passive pools).
func TestEjectReaper_NilWithoutEjectFor(t *testing.T) {
	t.Parallel()
	p := pool(t, "127.0.0.1:1")
	require.Nil(t, NewEjectReaper(p), "no passive health means no ejection, so no reaper")
	require.Zero(t, p.EjectFor(), "no passive health means the window stays zero")
}

// TestNewPool_DefaultEjectForWithPassiveHealth pins the F19 fix: a
// pool with passive health but an unset eject_for resolves to the
// built-in default window instead of eject-forever, and the default
// actually restores. Measured incident: on a single-target pool, 22
// clustered transient connection errors produced one 10-consecutive
// ejection and every subsequent miss of the run returned an instant
// 502 — with no active probe and no restore endpoint, the outage
// lasted until restart.
func TestNewPool_DefaultEjectForWithPassiveHealth(t *testing.T) {
	t.Parallel()
	bad := fasthttptest.NewServer(t, new5xxHandler())
	defer bad.Close()

	p, err := NewPool(PoolConfig{
		Name:           "test",
		Targets:        []string{bad.Addr},
		Consecutive5xx: 10,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	require.Equal(t, defaultEjectFor, p.EjectFor())
	require.Equal(t, 30*time.Second, defaultEjectFor)
	require.NotNil(t, NewEjectReaper(p), "the default window must run the reaper")

	// End to end: eject through the passive path, fast-forward the
	// ejection timestamp past the default window, and the reaper must
	// bring the target back — the exact sequence that used to strand
	// the target for the process lifetime.
	h := p.FastHandler(1)
	serveHandler(t, h, "GET", "/", "")
	require.Len(t, p.Healthy(), 0, "target must be ejected after the threshold")
	for _, tgt := range p.targets {
		tgt.ejectedAt.Store(time.Now().Add(-defaultEjectFor - time.Second).UnixNano())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go NewEjectReaper(p).Run(ctx)

	poll.Eventually(t, 2*time.Second, 20*time.Millisecond, func() bool {
		return len(p.Healthy()) == 1
	})
}

// TestEjectReaper_ReEjectsAfterRestore verifies that a restored target
// that is still broken re-ejects after fresh errors.
func TestEjectReaper_ReEjectsAfterRestore(t *testing.T) {
	t.Parallel()
	bad := fasthttptest.NewServer(t, new5xxHandler())
	defer bad.Close()

	p, err := NewPool(PoolConfig{
		Name:     "test",
		Targets:  []string{bad.Addr},
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		EjectFor: 10 * time.Millisecond,
	})
	require.NoError(t, err)

	h := p.FastHandler(1)
	serveHandler(t, h, "GET", "/", "")
	require.Len(t, p.Healthy(), 0)

	// Expire the window and reap.
	for _, tgt := range p.targets {
		tgt.ejectedAt.Store(time.Now().Add(-100 * time.Millisecond).UnixNano())
	}
	r := NewEjectReaper(p)
	r.reap()
	require.Len(t, p.Healthy(), 1, "target restored after window")

	// The still-broken target is served again and re-ejects after a
	// fresh error.
	serveHandler(t, h, "GET", "/", "")
	require.Len(t, p.Healthy(), 0, "target re-ejects after fresh 5xx")
}

// TestEjectReaper_TimestampStampedOnEjection guards the ejectedAt
// stamping: the passive ejection path must set it.
func TestEjectReaper_TimestampStampedOnEjection(t *testing.T) {
	t.Parallel()
	bad := fasthttptest.NewServer(t, new5xxHandler())
	defer bad.Close()

	p := pool(t, bad.Addr)
	h := p.FastHandler(1)
	serveHandler(t, h, "GET", "/", "")
	require.Len(t, p.Healthy(), 0)
	for _, tgt := range p.targets {
		require.NotZero(t, tgt.ejectedAt.Load(), "ejection must stamp ejectedAt")
	}
}
