package cache

import (
	"context"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// makeObj creates a cached object with the given key, body, and ETag.
func makeObj(key api.Key, body string, etag string) *api.Object {
	return &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     headerMap("Content-Type", "text/html", "Content-Length", strconv.Itoa(len(body))),
		ETag:       etag,
		Body:       []byte(body),
		BodySize:   int64(len(body)),
		StoredAt:   time.Now(),
		TTL:        600 * time.Second,
	}
}

// TestFastPathHandler_PoolReuseBetweenObjects verifies that serving
// object A, releasing the response, then serving object B does not
// leak A's body, headers, or status code into B's response. This is
// the primary cross-object contamination test for fastPathRespPool and
// fastPathHeaderPool.
func TestFastPathHandler_PoolReuseBetweenObjects(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	keyA := buildKeyFromRaw(&api.RawRequest{Method: "GET", Path: "/a", Host: "ex.com", Scheme: "http"}, nil)
	keyB := buildKeyFromRaw(&api.RawRequest{Method: "GET", Path: "/b", Host: "ex.com", Scheme: "http"}, nil)

	bodyA := "Body-AAA-unique-111"
	bodyB := "Body-BBB-unique-222"
	require.NoError(t, store.Put(context.Background(), keyA, makeObj(keyA, bodyA, `"etagA"`)))
	require.NoError(t, store.Put(context.Background(), keyB, makeObj(keyB, bodyB, `"etagB"`)))

	reqA := &api.RawRequest{Method: "GET", Path: "/a", Host: "ex.com", Scheme: "http"}
	reqB := &api.RawRequest{Method: "GET", Path: "/b", Host: "ex.com", Scheme: "http"}
	now := time.Now()

	// Serve A, consume the buffers, release.
	respA, ok := fp.TryHit(reqA, now)
	require.True(t, ok)
	require.NotNil(t, respA)
	require.Equal(t, 200, respA.StatusCode)
	require.Equal(t, bodyA, string(respA.Buffers[2]))
	writtenA := writeResponse(t, respA)
	fp.Release(respA)

	// Serve B from the same pool — must not contain any of A's data.
	respB, ok := fp.TryHit(reqB, now)
	require.True(t, ok)
	require.NotNil(t, respB)
	require.Equal(t, 200, respB.StatusCode)
	require.Equal(t, bodyB, string(respB.Buffers[2]), "second response body must be B's, not A's")
	require.NotContains(t, string(respB.Buffers[1]), "etagA", "B's header block must not contain A's ETag")
	require.NotContains(t, string(respB.Buffers[1]), bodyA, "B's header block must not contain A's body")
	writtenB := writeResponse(t, respB)
	fp.Release(respB)

	// Verify the wire bytes too.
	assert.Contains(t, string(writtenA), bodyA)
	assert.NotContains(t, string(writtenA), bodyB)
	assert.Contains(t, string(writtenB), bodyB)
	assert.NotContains(t, string(writtenB), bodyA)
}

// TestFastPathHandler_304Then200 verifies that after a 304 response
// (which sets BuffersArr[2]=nil and Buffers=BuffersArr[:2]), the next
// 200 hit from the same response pool has all 3 buffers correctly set.
func TestFastPathHandler_304Then200(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	keyA := buildKeyFromRaw(&api.RawRequest{Method: "GET", Path: "/a", Host: "ex.com", Scheme: "http"}, nil)
	keyB := buildKeyFromRaw(&api.RawRequest{Method: "GET", Path: "/b", Host: "ex.com", Scheme: "http"}, nil)

	bodyA := "Content-A-full-body"
	bodyB := "Content-B-full-body"
	require.NoError(t, store.Put(context.Background(), keyA, makeObj(keyA, bodyA, `"etagA"`)))
	require.NoError(t, store.Put(context.Background(), keyB, makeObj(keyB, bodyB, `"etagB"`)))

	now := time.Now()

	// Request 1: conditional 304 on object A.
	req304 := &api.RawRequest{Method: "GET", Path: "/a", Host: "ex.com", Scheme: "http"}
	req304.Headers[0] = api.RawHeader{Key: header.IfNoneMatch, Value: `"etagA"`}
	req304.NHeaders = 1

	resp304, ok := fp.TryHit(req304, now)
	require.True(t, ok)
	require.NotNil(t, resp304)
	require.Equal(t, 304, resp304.StatusCode)
	require.Len(t, resp304.Buffers, 2, "304 must have 2 buffers")
	writeResponse(t, resp304)
	fp.Release(resp304)

	// Request 2: full 200 on object B — must have 3 buffers and B's body.
	req200 := &api.RawRequest{Method: "GET", Path: "/b", Host: "ex.com", Scheme: "http"}
	resp200, ok := fp.TryHit(req200, now)
	require.True(t, ok)
	require.NotNil(t, resp200)
	require.Equal(t, 200, resp200.StatusCode)
	require.Len(t, resp200.Buffers, 3, "200 must have 3 buffers after 304 pool reuse")
	require.Equal(t, bodyB, string(resp200.Buffers[2]), "200 body must be B's, not nil or A's")
	writtenB := writeResponse(t, resp200)
	fp.Release(resp200)

	assert.Contains(t, string(writtenB), bodyB)
	assert.NotContains(t, string(writtenB), bodyA)
}

// TestFastPathHandler_200Then304 verifies the reverse: a 200 hit
// followed by a 304 on the same pool must not leak the 200's body
// into the 304's response.
func TestFastPathHandler_200Then304(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	keyA := buildKeyFromRaw(&api.RawRequest{Method: "GET", Path: "/a", Host: "ex.com", Scheme: "http"}, nil)
	keyB := buildKeyFromRaw(&api.RawRequest{Method: "GET", Path: "/b", Host: "ex.com", Scheme: "http"}, nil)

	bodyA := "Big-body-A-leak-check"
	bodyB := "Small-B"
	require.NoError(t, store.Put(context.Background(), keyA, makeObj(keyA, bodyA, `"etagA"`)))
	require.NoError(t, store.Put(context.Background(), keyB, makeObj(keyB, bodyB, `"etagB"`)))

	now := time.Now()

	// Request 1: full 200 on object A.
	req200 := &api.RawRequest{Method: "GET", Path: "/a", Host: "ex.com", Scheme: "http"}
	resp200, ok := fp.TryHit(req200, now)
	require.True(t, ok)
	require.Equal(t, 200, resp200.StatusCode)
	require.Equal(t, bodyA, string(resp200.Buffers[2]))
	writeResponse(t, resp200)
	fp.Release(resp200)

	// Request 2: conditional 304 on object B.
	req304 := &api.RawRequest{Method: "GET", Path: "/b", Host: "ex.com", Scheme: "http"}
	req304.Headers[0] = api.RawHeader{Key: header.IfNoneMatch, Value: `"etagB"`}
	req304.NHeaders = 1

	resp304, ok := fp.TryHit(req304, now)
	require.True(t, ok)
	require.Equal(t, 304, resp304.StatusCode)
	require.Len(t, resp304.Buffers, 2, "304 must have 2 buffers")
	require.Equal(t, 0, resp304.BytesOut, "304 must report 0 bytes out")

	// The 304 wire output must not contain A's body.
	written304 := writeResponse(t, resp304)
	fp.Release(resp304)

	assert.NotContains(t, string(written304), bodyA, "304 must not leak previous 200's body")
	assert.NotContains(t, string(written304), bodyB, "304 must not contain any body")
}

// TestFastPathHandler_ManyReusesNoCorruption verifies that repeated
// pool reuse across many different objects never mixes content.
func TestFastPathHandler_ManyReusesNoCorruption(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 10 << 20})
	fp := NewFastPathHandlerFromStore(store)

	type item struct {
		key  api.Key
		body string
		etag string
		req  *api.RawRequest
	}
	const n = 50
	items := make([]item, n)
	for i := range n {
		path := "/" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		req := &api.RawRequest{Method: "GET", Path: path, Host: "ex.com", Scheme: "http"}
		key := buildKeyFromRaw(req, nil)
		body := "body-" + string(rune('A'+i%26)) + string(rune('0'+i/26)) + "-" + string(rune('x'+i%3))
		etag := `"e` + string(rune('0'+i%10)) + `"`
		require.NoError(t, store.Put(context.Background(), key, makeObj(key, body, etag)))
		items[i] = item{key: key, body: body, etag: etag, req: req}
	}

	now := time.Now()

	// Serve all objects once, consuming and releasing each.
	for _, it := range items {
		resp, ok := fp.TryHit(it.req, now)
		require.True(t, ok, "miss for %s", it.req.Path)
		require.Equal(t, it.body, string(resp.Buffers[2]))
		writeResponse(t, resp)
		fp.Release(resp)
	}

	// Serve all objects a second time — pool should be warmed, content must still be correct.
	for _, it := range items {
		resp, ok := fp.TryHit(it.req, now)
		require.True(t, ok)
		require.Equal(t, it.body, string(resp.Buffers[2]), "wrong body on second pass for %s", it.req.Path)
		writeResponse(t, resp)
		fp.Release(resp)
	}
}

// writeResponse simulates the production write path: net.Buffers.WriteTo
// via a pipe, then returns the raw bytes written.
func writeResponse(t *testing.T, resp *api.FastPathResponse) []byte {
	t.Helper()
	r, w := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = resp.Buffers.WriteTo(w)
		_ = w.Close()
	}()
	written, err := io.ReadAll(r)
	_ = r.Close()
	<-done
	require.NoError(t, err)
	return written
}
