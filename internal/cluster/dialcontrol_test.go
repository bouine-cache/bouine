package cluster

import (
	"context"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// TestPeerFetcher_DialControlInvoked pins the DialControl wiring: peer
// fetch dials must pass through PeerFetcherConfig.DialControl so the
// engine's outbound socket options (cluster.peer_tcp_fast_open →
// TCP_FASTOPEN_CONNECT) reach every peer-bound connection.
func TestPeerFetcher_DialControlInvoked(t *testing.T) {
	t.Parallel()
	key := testkey.Key(21)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Body:       []byte("dial-control"),
	}
	obj.Header = header.NewMap(1)

	srv := fasthttptest.NewServer(t, NewPeerFetchHandler(&stubStore{objects: map[api.Key]*api.Object{
		key: obj,
	}}, 0).Handle)
	defer srv.Close()

	var dials atomic.Int64
	f := NewPeerFetcherWithConfig(PeerFetcherConfig{
		MaxIdleConnDuration: 100 * time.Millisecond,
		DialControl: func(network, addr string, c syscall.RawConn) error {
			dials.Add(1)
			return nil
		},
	}, nil, nil)
	defer func() { _ = f.Close(context.Background()) }()

	got, err := f.Fetch(context.Background(),
		api.PeerInfo{AdminAddr: srv.Addr},
		api.PeerFetchRequest{Key: key})
	require.NoError(t, err, "fetch")
	require.NotNil(t, got)
	require.Positive(t, dials.Load(), "the pipeline client's dial must pass through DialControl")
}

// TestBroadcaster_DialControlInherited pins that the invalidation
// broadcast fan-out client inherits the fetcher's DialControl, so peer
// put/fetch pipelines and one-shot broadcast dials all carry the same
// outbound socket options.
func TestBroadcaster_DialControlInherited(t *testing.T) {
	t.Parallel()
	httpCalled := 0
	srv := fasthttptest.NewServer(t, func(_ *fasthttp.RequestCtx) {
		httpCalled++
	})
	defer srv.Close()

	var dials atomic.Int64
	f := NewPeerFetcherWithConfig(PeerFetcherConfig{
		DialControl: func(network, addr string, c syscall.RawConn) error {
			dials.Add(1)
			return nil
		},
	}, nil, nil)
	defer func() { _ = f.Close(context.Background()) }()

	c := minimalCluster(t, "node-0")
	c.cfg.Mode = "strong"
	c.peers["node-1"] = &Member{Info: api.PeerInfo{
		Name:      "node-1",
		AdminAddr: srv.Addr,
	}}

	b := NewBroadcaster(c, f)
	b.BroadcastPurge(context.Background(), testkey.Key(22), "")
	b.Close()

	require.Eventually(t, func() bool { return dials.Load() > 0 },
		2*time.Second, 5*time.Millisecond,
		"the broadcast fan-out dial must pass through the fetcher's DialControl")
}
