package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

func TestPeerFetcher_PipelineConfig(t *testing.T) {
	t.Parallel()

	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.ContentType, "application/octet-stream")
		obj := &api.Object{Key: testkey.Key(1), StatusCode: 200, Body: []byte("pipeline")}
		_, _ = ctx.Write(storage.EncodeObject(obj))
	})
	defer srv.Close()

	f := NewPeerFetcherWithConfig(PeerFetcherConfig{
		HopLimit:            2,
		MaxConnsPerHost:     4,
		MaxIdleConnDuration: 5 * time.Second,
	}, nil, nil)
	defer f.Close(context.Background())

	peer := api.PeerInfo{AdminAddr: srv.Addr}
	got, err := f.Fetch(context.Background(), peer, api.PeerFetchRequest{Key: testkey.Key(1)})
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "pipeline", string(got.Body))
}

func TestPeerFetcher_PipelineConcurrentFetches(t *testing.T) {
	t.Parallel()

	obj := &api.Object{Key: testkey.Key(1), StatusCode: 200, Body: []byte("concurrent")}
	encoded := storage.EncodeObject(obj)

	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.ContentType, "application/octet-stream")
		_, _ = ctx.Write(encoded)
	})
	defer srv.Close()

	f := NewPeerFetcherWithConfig(PeerFetcherConfig{
		HopLimit:            2,
		MaxConnsPerHost:     2,
		MaxIdleConnDuration: 5 * time.Second,
	}, nil, nil)
	defer f.Close(context.Background())

	peer := api.PeerInfo{AdminAddr: srv.Addr}

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := f.Fetch(context.Background(), peer, api.PeerFetchRequest{Key: testkey.Key(1)})
			require.NoError(t, err)
			require.NotNil(t, got)
			require.Equal(t, "concurrent", string(got.Body))
		}()
	}
	wg.Wait()
}

func TestPeerFetcher_PipelineReusesConnection(t *testing.T) {
	t.Parallel()

	obj := &api.Object{Key: testkey.Key(1), StatusCode: 200, Body: []byte("reuse")}
	encoded := storage.EncodeObject(obj)

	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.ContentType, "application/octet-stream")
		_, _ = ctx.Write(encoded)
	})
	defer srv.Close()

	f := NewPeerFetcherWithConfig(PeerFetcherConfig{
		HopLimit:            2,
		MaxConnsPerHost:     2,
		MaxIdleConnDuration: 30 * time.Second,
	}, nil, nil)
	defer f.Close(context.Background())

	peer := api.PeerInfo{AdminAddr: srv.Addr}

	// Sequential fetches should all succeed with pipelining.
	for i := range 10 {
		got, err := f.Fetch(context.Background(), peer, api.PeerFetchRequest{Key: testkey.Key(1)})
		require.NoError(t, err, "fetch %d", i)
		require.NotNil(t, got)
		require.Equal(t, "reuse", string(got.Body))
	}
}
