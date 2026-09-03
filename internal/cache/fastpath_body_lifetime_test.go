package cache

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/server/h1parser"
	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

// patternedBody returns a deterministic body whose every 8-byte block
// encodes its own offset, so any mutation, truncation, or cross-object
// byte mixing is detectable by scanning the received bytes.
func patternedBody(size int) []byte {
	body := make([]byte, size)
	var counter [8]byte
	for off := 0; off < size; off += 8 {
		binary.LittleEndian.PutUint64(counter[:], uint64(off))
		copy(body[off:], counter[:])
	}
	return body
}

// readResponse reads exactly one HTTP/1.1 response (head + Content-Length
// delimited body) from conn, one small chunk at a time to emulate a slow
// client, invoking onChunk after every chunk so the test can race the
// cache while the response is still being written.
func readResponse(t *testing.T, conn net.Conn, chunkSize int, delay time.Duration, onChunk func(totalRead int)) (head string, body []byte, err error) {
	t.Helper()

	var raw bytes.Buffer
	buf := make([]byte, chunkSize)
	for {
		n, rerr := conn.Read(buf)
		if n > 0 {
			raw.Write(buf[:n])
			if onChunk != nil {
				onChunk(raw.Len())
			}
		}
		if rerr != nil {
			return "", nil, rerr
		}

		headEnd := bytes.Index(raw.Bytes(), []byte("\r\n\r\n"))
		if headEnd < 0 {
			if delay > 0 {
				time.Sleep(delay)
			}
			continue
		}
		head = string(raw.Bytes()[:headEnd+4])

		contentLength := 0
		for _, line := range bytes.Split(raw.Bytes()[:headEnd], []byte("\r\n")) {
			const prefix = "Content-Length: "
			if len(line) > len(prefix) && bytes.EqualFold(line[:len(prefix)], []byte(prefix)) {
				for _, c := range line[len(prefix):] {
					if c < '0' || c > '9' {
						break
					}
					contentLength = contentLength*10 + int(c-'0')
				}
			}
		}

		total := headEnd + 4 + contentLength
		for raw.Len() < total {
			n, rerr = conn.Read(buf)
			if n > 0 {
				raw.Write(buf[:n])
				if onChunk != nil {
					onChunk(raw.Len())
				}
			}
			if rerr != nil {
				return head, nil, rerr
			}
			if delay > 0 {
				time.Sleep(delay)
			}
		}
		return head, raw.Bytes()[headEnd+4 : total], nil
	}
}

// TestFastPathHit_BodyStableUnderSlowClientAndEviction is the regression
// test for the preprod front-office 500s: a slow-reading client holds an
// in-flight cache-hit writev while the origin path concurrently refreshes
// the same key (Put-overwrite), evicts neighbors under memory pressure,
// and reuses the caller's body buffer. The client must still receive the
// exact bytes that were stored when the hit started — never the caller's
// reused buffer, another object's bytes, or a truncated body.
//
// net.Pipe is unbuffered and synchronous: every WriteTo byte is delivered
// only as the client reads it, so the mid-response mutation below is
// guaranteed to race the still-pending tail of the writev.
func TestFastPathHit_BodyStableUnderSlowClientAndEviction(t *testing.T) {
	t.Parallel()

	const bodySize = 512 * 1024

	// Budget sized so the 512 KiB object fits (1 MiB per shard) while
	// the concurrent neighbor Puts (8 x 256 KiB) push shards over budget
	// and force SIEVE evictions mid-response.
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 4 << 20, NumShards: 4})
	fp := NewFastPathHandlerFromStore(store)

	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/page",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	key := buildKeyFromRaw(req, nil)

	body := patternedBody(bodySize)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     headerMap("Content-Type", "application/json", "Content-Length", strconv.Itoa(bodySize)),
		Body:       body,
		BodySize:   int64(bodySize),
		StoredAt:   time.Now(),
		TTL:        10 * time.Minute,
	}
	require.NoError(t, store.Put(context.Background(), key, obj), "put")

	parser := h1parser.New(fp, func(ctx *fasthttp.RequestCtx) {}, h1parser.WithWriteTimeout(30*time.Second))

	clientConn, serverConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- parser.Serve(serverConn)
	}()

	_, _ = clientConn.Write([]byte("GET /page HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n"))

	// Race trigger: once the client is part-way through the body (the
	// writev is still flushing the tail on net.Pipe), mutate the
	// caller's body buffer, overwrite the same key with a different
	// object, and evict neighbors under shard memory pressure.
	raced := false
	var wg sync.WaitGroup
	head, gotBody, rerr := readResponse(t, clientConn, 8*1024, 2*time.Millisecond, func(totalRead int) {
		if raced || totalRead < 64*1024 {
			return
		}
		raced = true
		wg.Add(2)

		// Caller buffer reuse: the origin path reuses its read buffer
		// after handing the response to the cache.
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
				Body:       bytes.Repeat([]byte("R"), 16),
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

	require.True(t, raced, "test must have raced the in-flight writev; increase the read pacing if flaky (head=%q err=%v bodyLen=%d)", head, rerr, len(gotBody))
	wg.Wait()
	require.NoError(t, rerr, "client read")

	assert.Contains(t, head, "HTTP/1.1 200 OK", "status line")
	require.Len(t, gotBody, bodySize, "body must be complete, not truncated")
	require.True(t, bytes.Equal(gotBody, patternedBody(bodySize)),
		"body served to a slow mid-response client must be the exact stored bytes: no caller-buffer aliasing, no cross-object mixing, no truncation")

	_ = serverConn.Close()
	select {
	case <-serveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("parser.Serve did not return after connection close")
	}
}
