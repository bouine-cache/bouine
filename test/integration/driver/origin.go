//go:build integration

package driver

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/valyala/fasthttp"
)

// originControl provides runtime knobs for chaos testing.
type originControl struct {
	latencyMs atomic.Int64 // injected latency per request (0 = none)
	forceErr  atomic.Bool  // when true, all requests return 503
}

// fasthttpTestServer is a minimal fasthttp server with the same lifecycle
// semantics as httptest.Server: call Close when done.
type fasthttpTestServer struct {
	url    string
	addr   string
	server *fasthttp.Server
	ln     net.Listener
}

func (s *fasthttpTestServer) URL() string  { return s.url }
func (s *fasthttpTestServer) Addr() string { return s.addr }
func (s *fasthttpTestServer) Close() {
	_ = s.server.Shutdown()
	_ = s.ln.Close()
}

// startOriginWithControl creates a fasthttp server with controllable chaos knobs.
func startOriginWithControl() (*fasthttpTestServer, *originControl) {
	ctrl := &originControl{}

	handler := func(ctx *fasthttp.RequestCtx) {
		if ctrl.forceErr.Load() {
			ctx.SetStatusCode(503)
			ctx.WriteString("origin forced error")
			return
		}
		if ms := ctrl.latencyMs.Load(); ms > 0 {
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
		originRouteHandler(ctx)
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("startOrigin: listen: %v", err))
	}
	srv := &fasthttp.Server{Handler: handler}
	go func() { _ = srv.Serve(ln) }()

	return &fasthttpTestServer{
		url:    "http://" + ln.Addr().String(),
		addr:   ln.Addr().String(),
		server: srv,
		ln:     ln,
	}, ctrl
}

// startOrigin is the backward-compatible wrapper used by integration tests.
func startOrigin() *fasthttpTestServer {
	srv, _ := startOriginWithControl()
	return srv
}

// originRouteHandler dispatches based on path, mirroring the endpoints
// in test/integration/origin/main.go.
func originRouteHandler(ctx *fasthttp.RequestCtx) {
	path := string(ctx.Path())
	switch path {
	case "/healthz":
		ctx.SetStatusCode(fasthttp.StatusOK)
		ctx.WriteString("ok")
	case "/hit":
		ctx.Response.Header.Set("Cache-Control", "max-age=3600")
		ctx.Response.Header.Set("ETag", `"hit-v1"`)
		fmt.Fprintf(ctx, "hit at %s", time.Now().Format(time.RFC3339Nano))
	case "/miss":
		ctx.Response.Header.Set("Cache-Control", "no-store")
		fmt.Fprintf(ctx, "miss at %s", time.Now().Format(time.RFC3339Nano))
	case "/bypass":
		ctx.Response.Header.Set("Cache-Control", "private")
		fmt.Fprintf(ctx, "bypass at %s", time.Now().Format(time.RFC3339Nano))
	case "/stale":
		ctx.Response.Header.Set("Cache-Control", "max-age=1, stale-while-revalidate=3600")
		ctx.Response.Header.Set("ETag", `"stale-v1"`)
		fmt.Fprintf(ctx, "stale at %s", time.Now().Format(time.RFC3339Nano))
	case "/revalidate":
		etag := `"reval-v1"`
		if string(ctx.Request.Header.Peek("If-None-Match")) == etag {
			ctx.SetStatusCode(fasthttp.StatusNotModified)
			return
		}
		ctx.Response.Header.Set("Cache-Control", "max-age=0, must-revalidate")
		ctx.Response.Header.Set("ETag", etag)
		fmt.Fprintf(ctx, "revalidate at %s", time.Now().Format(time.RFC3339Nano))
	case "/vary":
		ctx.Response.Header.Set("Cache-Control", "max-age=3600")
		ctx.Response.Header.Set("Vary", "Accept-Encoding")
		enc := string(ctx.Request.Header.Peek("Accept-Encoding"))
		fmt.Fprintf(ctx, "vary enc=%s", enc)
	case "/error":
		ctx.SetStatusCode(503)
		ctx.WriteString("origin error")
	case "/slow":
		ms, _ := strconv.Atoi(string(ctx.QueryArgs().Peek("ms")))
		if ms <= 0 {
			ms = 500
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
		ctx.Response.Header.Set("Cache-Control", "max-age=60")
		fmt.Fprintf(ctx, "slow %dms", ms)
	case "/payload":
		// Deterministic, checksummable body for data-integrity tests:
		// every 8-byte block i is LE32(fnv32a(k)^i) || LE32(i), so clients
		// can regenerate the exact expected bytes for a given (k, kb).
		// Mirrors test/integration/origin/main.go and the k6 payload
		// helpers in bench/loadtest/scenarios/lib/payload.js.
		k := string(ctx.QueryArgs().Peek("k"))
		kb, _ := strconv.Atoi(string(ctx.QueryArgs().Peek("kb")))
		if k == "" {
			k = "default"
		}
		if kb <= 0 {
			kb = 64
		}
		if kb > 4096 {
			kb = 4096
		}
		ctx.Response.Header.Set("Cache-Control", "max-age=3600")
		ctx.Response.Header.Set("Content-Type", "application/octet-stream")
		ctx.Write(DeterministicPayload(k, kb))
	case "/unique":
		ctx.Response.Header.Set("Cache-Control", "max-age=3600")
		fmt.Fprintf(ctx, "unique %s at %s", ctx.Path(), time.Now().Format(time.RFC3339Nano))
	case "/echo":
		ctx.Response.Header.Set("Cache-Control", "max-age=3600")
		fmt.Fprintf(ctx, "uri %s", ctx.RequestURI())
	default:
		ctx.Response.Header.Set("Cache-Control", "max-age=5, stale-if-error=60, stale-while-revalidate=60")
		fmt.Fprintf(ctx, "chaos %s at %s", ctx.Path(), time.Now().Format(time.RFC3339Nano))
	}
}

// DeterministicPayload builds the deterministic body served by the
// /payload route: kb KiB of 8-byte blocks where block i is
// LE32(fnv32a(k)^i) || LE32(i). Clients regenerate the same bytes to
// verify cache integrity (test/chaos body-integrity scenario and the
// k6 payload helpers in bench/loadtest/scenarios/lib/payload.js).
func DeterministicPayload(k string, kb int) []byte {
	if kb <= 0 {
		kb = 64
	}
	body := make([]byte, kb*1024)
	seed := fnv32a(k)
	var block [8]byte
	for off := 0; off+8 <= len(body); off += 8 {
		i := off / 8
		le32(block[:4], seed^uint32(i))
		le32(block[4:], uint32(i))
		copy(body[off:], block[:])
	}
	return body
}

// fnv32a is the standard 32-bit FNV-1a hash, mirrored by the k6 payload
// helper so origin and client agree on the per-key seed.
func fnv32a(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

func le32(dst []byte, v uint32) {
	dst[0] = byte(v)
	dst[1] = byte(v >> 8)
	dst[2] = byte(v >> 16)
	dst[3] = byte(v >> 24)
}
