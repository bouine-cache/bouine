package cluster

import (
	"context"
	"errors"
	"net"
	"sync"
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

// ---- stale PipelineClient retirement ----

// TestPeerFetcher_RetireAddress_ParksDial covers the residual v0.5.17
// defect observed in prod-eu: after a rolling restart, fasthttp's
// pipeline worker for a dead peer address re-dials it forever (its
// restart loop has no exit on dial failure and PipelineClient has no
// Close). RetireAddress must evict the client and park its dial: no
// dial attempt, released at fetcher close.
func TestPeerFetcher_RetireAddress_ParksDial(t *testing.T) {
	t.Parallel()

	f := NewPeerFetcherWithConfig(PeerFetcherConfig{}, nil, nil)
	addr := "10.0.0.99:9000"

	pc := f.getPipelineClient(addr)
	require.NotNil(t, pc)
	require.NotNil(t, pc.Dial, "precondition: dial closure is set")

	f.RetireAddress(addr)

	parked := make(chan error, 1)
	go func() { _, err := pc.Dial(addr); parked <- err }()
	select {
	case err := <-parked:
		t.Fatalf("dial for a retired address must park, got immediate return: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	require.NoError(t, f.Close(context.Background()))
	select {
	case err := <-parked:
		require.ErrorIs(t, err, errPeerAddrRetired)
		var netErr net.Error
		require.ErrorAs(t, err, &netErr)
		assert.True(t, netErr.Timeout(), "released dial must look like a timeout so fasthttp throttles its restart loop")
	case <-time.After(2 * time.Second):
		t.Fatal("parked dial must be released by Close")
	}
}

// TestPeerFetcher_RetireAddress_FreshClientForReturnedAddress verifies
// retirement is not a permanent eviction of the address: traffic for a
// retired address transparently gets a fresh client (the retired mark
// is dropped in getPipelineClient), so a peer that comes back at the
// same address keeps working.
func TestPeerFetcher_RetireAddress_FreshClientForReturnedAddress(t *testing.T) {
	t.Parallel()

	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.ContentType, "application/octet-stream")
		ctx.Response.SetStatusCode(fasthttp.StatusNotFound)
	})
	defer srv.Close()

	f := NewPeerFetcherWithConfig(PeerFetcherConfig{}, nil, nil)
	defer f.Close(context.Background())
	peer := api.PeerInfo{Name: "live", Addr: srv.Addr, AdminAddr: srv.Addr}

	_, err := f.Fetch(context.Background(), peer, api.PeerFetchRequest{Key: testkey.Key(1)})
	require.NoError(t, err, "precondition: fetch works before retirement")

	f.RetireAddress(srv.Addr)

	for range 2 {
		_, err := f.Fetch(context.Background(), peer, api.PeerFetchRequest{Key: testkey.Key(2)})
		require.NoError(t, err, "fetch after retirement must get a fresh client and succeed")
	}
	_, retired := f.retiredAddrs.Load(srv.Addr)
	assert.False(t, retired, "retired mark must be dropped once the address is used again")
}

// TestCluster_RemovePeer_RetiresAddress verifies the cluster notifies
// the retire callback with the removed peer's address, so the fetcher
// can evict the stale PipelineClient when a peer leaves the ring.
func TestCluster_RemovePeer_RetiresAddress(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig(t, "retire-remove", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	var mu sync.Mutex
	var retired []string
	c.SetOnPeerRetired(func(addr string) {
		mu.Lock()
		defer mu.Unlock()
		retired = append(retired, addr)
	})

	stale := "10.0.0.99:9000"
	c.addPeer("ghost", api.PeerInfo{Name: "ghost", Addr: stale, AdminAddr: stale})
	c.removePeer("ghost")

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{stale}, retired, "removed peer's address must be retired")
}

// TestCluster_PeerAddressChange_RetiresOldAddress covers the rolling
// restart case from prod: the peer rejoins under the same node name at
// a new address, so the ring entry is refreshed in place. The old
// address must be retired even though the peer never left the ring,
// and an unchanged address must not be.
func TestCluster_PeerAddressChange_RetiresOldAddress(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig(t, "retire-refresh", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	var mu sync.Mutex
	var retired []string
	c.SetOnPeerRetired(func(addr string) {
		mu.Lock()
		defer mu.Unlock()
		retired = append(retired, addr)
	})

	oldAddr, newAddr := "10.0.0.55:9000", "10.0.0.56:9000"
	c.addPeer("restarted", api.PeerInfo{Name: "restarted", Addr: oldAddr, AdminAddr: oldAddr})
	c.addPeer("restarted", api.PeerInfo{Name: "restarted", Addr: newAddr, AdminAddr: newAddr})
	c.addPeer("restarted", api.PeerInfo{Name: "restarted", Addr: newAddr, AdminAddr: newAddr})

	members := c.Members()
	for _, m := range members {
		if m.Name == "restarted" {
			assert.Equal(t, newAddr, m.AdminAddr, "ring entry must carry the new address")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{oldAddr}, retired,
		"only the address replaced by a restart must be retired, an unchanged re-add must not")
}

// TestCluster_ReconcilePrune_RetiresAddress reproduces the prod-eu
// scenario where a peer dies without a delivered NotifyLeave and the
// reconcile pass prunes it: the pruned peer's address must reach the
// retire callback so the fetcher evicts its stale PipelineClient. (The
// reconcile refresh path funnels through the addPeer address-change
// retirement covered above.)
func TestCluster_ReconcilePrune_RetiresAddress(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig(t, "retire-reconcile", "127.0.0.1:0")
	c, err := New(cfg)
	require.NoError(t, err)
	defer func() { _ = c.Leave(t.Context()) }()

	var mu sync.Mutex
	var retired []string
	c.SetOnPeerRetired(func(addr string) {
		mu.Lock()
		defer mu.Unlock()
		retired = append(retired, addr)
	})

	stale := "10.0.0.55:9000"
	c.addPeer("ghost", api.PeerInfo{Name: "ghost", Addr: stale, AdminAddr: stale})
	c.reconcileOnce()

	require.Len(t, c.Members(), 1, "precondition: reconcile pruned the ghost peer")
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{stale}, retired, "the pruned peer's address must be retired")
}
