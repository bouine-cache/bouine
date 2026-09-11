package cluster

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// ---- cluster reconcile (stale ring entries / stale peer info) ----

// TestReconcile_PrunesStalePeerWithoutMerge reproduces the prod symptom
// where a peer that is no longer in memberlist's live set stays in the
// ring between push/pull merges: every peer-fetch routed to it pays a
// full dial timeout. The reconcile pass must prune it without waiting
// for a MergeRemoteState event.
func TestReconcile_PrunesStalePeerWithoutMerge(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig(t, "reconcile-prune", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	c.addPeer("ghost-peer", api.PeerInfo{Name: "ghost-peer", Addr: "10.0.0.99:9000"})
	require.Len(t, c.Members(), 2)

	c.reconcileOnce()

	members := c.Members()
	assert.Len(t, members, 1, "stale peer must be pruned by the reconcile pass alone")
	assert.Equal(t, "reconcile-prune", members[0].Name)
}

// TestReconcile_RefreshesStalePeerInfo covers the observed prod state:
// memberlist reports the peer alive (so prune keeps it) but the recorded
// PeerInfo still carries the peer's pre-restart address, so peer fetches
// dial a dead IP forever. The reconcile pass must re-read the address
// from the memberlist node metadata and heal the entry.
func TestReconcile_RefreshesStalePeerInfo(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig(t, "reconcile-refresh", "127.0.0.1:17963")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	// Corrupt the recorded info for the local member: wrong AdminAddr,
	// as if the peer restarted and NotifyUpdate was never delivered.
	stale := "10.0.0.55:9000"
	c.mu.Lock()
	c.peers[c.cfg.NodeName] = &Member{Info: api.PeerInfo{
		Name:      c.cfg.NodeName,
		Addr:      c.local.Addr,
		AdminAddr: stale,
		DataAddr:  c.local.DataAddr,
		Weight:    1,
	}}
	c.mu.Unlock()
	require.Equal(t, stale, c.Members()[0].AdminAddr, "precondition: stale admin addr recorded")

	c.reconcileOnce()

	members := c.Members()
	require.Len(t, members, 1)
	assert.Equal(t, cfg.PeerInfo.AdminAddr, members[0].AdminAddr,
		"stale peer info must be refreshed from memberlist node metadata")
}

// TestReconcileLoop_TicksAndStops verifies the background reconcile loop
// runs on its interval and terminates on Leave.
func TestReconcileLoop_TicksAndStops(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig(t, "reconcile-loop", "127.0.0.1:0")
	cfg.ReconcileInterval = 20 * time.Millisecond
	c, err := New(cfg)
	require.NoError(t, err)

	c.addPeer("loop-ghost", api.PeerInfo{Name: "loop-ghost", Addr: "10.0.0.98:9000"})
	require.Eventually(t, func() bool {
		return len(c.Members()) == 1
	}, 2*time.Second, 5*time.Millisecond, "reconcile loop did not prune the stale peer")

	require.NoError(t, c.Leave(t.Context()))
	assert.Never(t, func() bool { return c.reconcileRunning() }, 100*time.Millisecond, 10*time.Millisecond,
		"reconcile loop must stop after Leave")
}

// ---- peer fetcher address breaker ----

// slamServer returns a listener address where every accepted connection
// is closed immediately. Unlike a refused port (where fasthttp's
// pipeline worker hot-restarts dialing while the request stalls for the
// full 60s RPC timeout), a slam server completes the dial and fails the
// pending request promptly — the fast way to exercise transport
// failures in tests.
func slamServer(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	return l.Addr().String()
}

// TestPeerFetcher_BlacklistsDeadAddr reproduces the prod symptom: fetches
// routed to a peer address that has been dead since a rolling restart
// each pay the full dial timeout. After the configured number of
// consecutive failures the address must be blacklisted so subsequent
// fetches fail fast and the handler falls back to origin immediately.
func TestPeerFetcher_BlacklistsDeadAddr(t *testing.T) {
	t.Parallel()

	addr := slamServer(t)
	f := NewPeerFetcherWithConfig(PeerFetcherConfig{
		FailureThreshold: 3,
		FailureCooldown:  time.Hour,
	}, nil, nil)
	defer f.Close(context.Background())
	peer := api.PeerInfo{Name: "dead", Addr: addr, AdminAddr: addr}

	for range 3 {
		_, err := f.Fetch(context.Background(), peer, api.PeerFetchRequest{Key: testkey.Key(1)})
		require.Error(t, err, "dial to dead address must fail")
		require.False(t, errors.Is(err, ErrPeerBlacklisted), "not blacklisted yet at failure %d", err)
	}

	start := time.Now()
	_, err := f.Fetch(context.Background(), peer, api.PeerFetchRequest{Key: testkey.Key(1)})
	require.ErrorIs(t, err, ErrPeerBlacklisted, "address must be blacklisted after consecutive failures")
	assert.Less(t, time.Since(start), 100*time.Millisecond,
		"blacklisted fetch must fail fast, not pay a dial timeout")
}

// TestPeerFetcher_BlacklistCooldownExpires verifies the blacklist is a
// cooldown, not a permanent eviction: once the cooldown lapses the
// fetcher probes the address again (a peer may have come back).
func TestPeerFetcher_BlacklistCooldownExpires(t *testing.T) {
	t.Parallel()

	addr := slamServer(t)
	f := NewPeerFetcherWithConfig(PeerFetcherConfig{
		FailureThreshold: 2,
		FailureCooldown:  50 * time.Millisecond,
	}, nil, nil)
	defer f.Close(context.Background())
	peer := api.PeerInfo{Name: "dead", Addr: addr, AdminAddr: addr}

	for range 2 {
		_, err := f.Fetch(context.Background(), peer, api.PeerFetchRequest{Key: testkey.Key(1)})
		require.Error(t, err)
	}
	_, err := f.Fetch(context.Background(), peer, api.PeerFetchRequest{Key: testkey.Key(1)})
	require.ErrorIs(t, err, ErrPeerBlacklisted)

	time.Sleep(80 * time.Millisecond)

	// Cooldown lapsed: the fetcher dials again (and fails again — the
	// address is still dead — but with a transport error, not the
	// blacklist sentinel), then re-trips immediately on the next call.
	_, err = f.Fetch(context.Background(), peer, api.PeerFetchRequest{Key: testkey.Key(1)})
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrPeerBlacklisted), "cooldown must allow a re-probe")
	_, err = f.Fetch(context.Background(), peer, api.PeerFetchRequest{Key: testkey.Key(1)})
	require.ErrorIs(t, err, ErrPeerBlacklisted, "consecutive count survives the cooldown probe")
}

// TestPeerFetcher_SuccessResetsBreaker verifies a healthy round trip
// clears the failure count for that address, so occasional timeouts
// against a live peer never blacklist it.
func TestPeerFetcher_SuccessResetsBreaker(t *testing.T) {
	t.Parallel()

	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.ContentType, "application/octet-stream")
		ctx.Response.SetStatusCode(fasthttp.StatusNotFound)
	})
	defer srv.Close()

	f := NewPeerFetcherWithConfig(PeerFetcherConfig{
		FailureThreshold: 3,
		FailureCooldown:  time.Hour,
	}, nil, nil)
	defer f.Close(context.Background())

	// Two failures: below the threshold.
	addr := srv.Addr
	f.recordPeerFailure(addr)
	f.recordPeerFailure(addr)

	// A successful fetch (404 is a transport success) resets the count.
	_, err := f.Fetch(context.Background(),
		api.PeerInfo{Name: "live", AdminAddr: addr},
		api.PeerFetchRequest{Key: testkey.Key(1)})
	require.NoError(t, err)

	// Two more failures: still below the threshold because the count
	// was reset — the address is not blacklisted.
	f.recordPeerFailure(addr)
	f.recordPeerFailure(addr)
	assert.True(t, f.breakerAllowed(addr), "success must reset the failure count")
}

// TestPeerFetcher_PutRespectsBlacklist mirrors the fetch test for the
// write-to-owner path: blacklisted addresses skip the RPC entirely.
func TestPeerFetcher_PutRespectsBlacklist(t *testing.T) {
	t.Parallel()

	addr := slamServer(t)
	f := NewPeerFetcherWithConfig(PeerFetcherConfig{
		FailureThreshold: 2,
		FailureCooldown:  time.Hour,
	}, nil, nil)
	defer f.Close(context.Background())
	peer := api.PeerInfo{Name: "dead", Addr: addr, AdminAddr: addr}

	for range 2 {
		require.Error(t, f.Put(context.Background(), peer, &api.Object{
			Key: testkey.Key(1), StatusCode: 200, Body: []byte("x"), StoredAt: time.Now(),
		}))
	}
	err := f.Put(context.Background(), peer, &api.Object{
		Key: testkey.Key(1), StatusCode: 200, Body: []byte("x"), StoredAt: time.Now(),
	})
	require.ErrorIs(t, err, ErrPeerBlacklisted)
}
