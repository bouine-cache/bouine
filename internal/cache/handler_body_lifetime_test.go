package cache

import (
	"bytes"
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

// TestServeRequest_HitBodyStableUnderSlowClientAndEviction is the
// normal-path twin of TestFastPathHit_BodyStableUnderSlowClientAndEviction:
// the preprod incident kept producing corrupt 200s after the H1 fast path
// was disabled, because serveObject serves obj.Body via SetBodyRaw on the
// same storage aliasing hole. This test pins the standard fasthttp hit
// path: a slow-reading client holds an in-flight hit response while the
// origin path reuses its buffer, Put-overwrites the key, and SIEVE
// eviction churns the shard — the client must still receive the exact
// stored bytes.
//
// The handler runs behind a real fasthttp.Server over net.Pipe (unbuffered
// and synchronous), so the response body written after the handler returns
// is delivered only as fast as the client reads.
func TestServeRequest_HitBodyStableUnderSlowClientAndEviction(t *testing.T) {
	t.Parallel()

	const bodySize = 512 * 1024

	// Budget sized so the 512 KiB object fits (1 MiB per shard) while the
	// concurrent neighbor Puts (8 x 256 KiB) push shards over budget and
	// force SIEVE evictions mid-response.
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20, NumShards: 4})
	h := NewHandler(HandlerConfig{
		Upstream:   origin200("unused"),
		FastClient: &testFastClient{handler: origin200("unused")},
		Store:      store,
	})

	url := "http://example.com/page"
	key := BuildKeyFromURL(url, nil)

	body := patternedBody(bodySize)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     headerMap("Content-Type", "application/json", "Content-Length", strconv.Itoa(bodySize), "Cache-Control", "max-age=600"),
		Body:       body,
		BodySize:   int64(bodySize),
		StoredAt:   time.Now(),
		TTL:        10 * time.Minute,
	}
	require.NoError(t, store.Put(context.Background(), key, obj), "put")

	srv := &fasthttp.Server{Handler: h.ServeRequest}

	clientConn, serverConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- srv.ServeConn(serverConn)
	}()

	_, _ = clientConn.Write([]byte("GET /page HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n"))

	// Race trigger: once the client is part-way through the body (the
	// response tail is still being written from the cached object), reuse
	// the origin-side buffer, Put-overwrite the same key, and churn
	// neighbors to force SIEVE evictions in the shard.
	raced := false
	var wg sync.WaitGroup
	head, gotBody, rerr := readResponse(t, clientConn, 8*1024, 2*time.Millisecond, func(totalRead int) {
		if raced || totalRead < 64*1024 {
			return
		}
		raced = true
		wg.Add(2)

		// Origin-side buffer reuse: the fetch path reuses its response
		// buffer once the object has been handed to the cache.
		go func() {
			defer wg.Done()
			copy(body, patternedBody(len(body)))
			for i := range body {
				body[i] ^= 0xFF
			}
		}()

		// Concurrent refresh of the same key plus neighbor churn that
		// forces SIEVE evictions in the same shard.
		go func() {
			defer wg.Done()
			refreshed := &api.Object{
				Key:        key,
				StatusCode: 200,
				Header:     headerMap("Content-Type", "application/json", "Content-Length", "16"),
				Body:       patternedBody(16),
				BodySize:   16,
				StoredAt:   time.Now(),
				TTL:        10 * time.Minute,
			}
			_ = store.Put(context.Background(), key, refreshed)
			for i := 0; i < 8; i++ {
				nk := testkey.Key(uint64(1000 + i))
				neighbor := &api.Object{
					Key:        nk,
					StatusCode: 200,
					Header:     headerMap("Content-Length", "8"),
					Body:       patternedBody(256 * 1024),
					BodySize:   256 * 1024,
					StoredAt:   time.Now(),
					TTL:        10 * time.Minute,
				}
				_ = store.Put(context.Background(), nk, neighbor)
			}
		}()
	})

	require.True(t, raced, "test must have raced the in-flight response write; increase the read pacing if flaky (head=%q err=%v bodyLen=%d)", head, rerr, len(gotBody))
	wg.Wait()
	require.NoError(t, rerr, "client read")

	assert.Contains(t, head, "HTTP/1.1 200 OK", "status line")
	assert.Contains(t, head, "X-Cache: HIT", "must be served from cache")
	require.Len(t, gotBody, bodySize, "body must be complete, not truncated")
	require.True(t, bytes.Equal(gotBody, patternedBody(bodySize)),
		"body served to a slow mid-response client must be the exact stored bytes: no origin-buffer aliasing, no cross-object mixing, no truncation")

	_ = serverConn.Close()
	select {
	case <-serveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("fasthttp ServeConn did not return after connection close")
	}
}
