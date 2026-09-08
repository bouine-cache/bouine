// Command test-origin is a purpose-built origin for validating bouine's
// cache behavior. Each endpoint returns specific caching headers so every
// cache decision (HIT, MISS, BYPASS, STALE, REVALIDATE) can be tested.
//
// Endpoints:
//
//	GET /healthz       -> 200, no cache headers
//	GET /hit           -> 200, Cache-Control: max-age=3600, ETag
//	GET /miss          -> 200, Cache-Control: no-store
//	GET /bypass        -> 200, Cache-Control: private
//	GET /stale         -> 200, Cache-Control: max-age=1, stale-while-revalidate=3600
//	GET /revalidate    -> 200 or 304, Cache-Control: max-age=0 must-revalidate, ETag
//	GET /vary          -> 200, Cache-Control: max-age=3600, Vary: Accept-Encoding
//	GET /heuristic     -> 200, Last-Modified: <1 day ago>, no Cache-Control
//	GET /error         -> 503
//	GET /slow?ms=N     -> 200 after N ms, Cache-Control: max-age=60
package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/valyala/fasthttp"
)

const httpTimeFormat = "Mon, 02 Jan 2006 15:04:05 GMT"

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	srv := &fasthttp.Server{
		Handler:     originHandler,
		ReadTimeout: 5 * time.Second,
	}

	log.Printf("test-origin listening on %s", *addr)
	log.Fatal(srv.ListenAndServe(*addr))
}

func originHandler(ctx *fasthttp.RequestCtx) {
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
		fmt.Fprintf(ctx, "vary enc=%s at %s", enc, time.Now().Format(time.RFC3339Nano))
	case "/heuristic":
		ctx.Response.Header.Set("Last-Modified", time.Now().Add(-24*time.Hour).UTC().Format(httpTimeFormat))
		fmt.Fprintf(ctx, "heuristic at %s", time.Now().Format(time.RFC3339Nano))
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
		// Deterministic, checksummable body for data-integrity load
		// tests (bench/loadtest scenario 3.7): every 8-byte block i is
		// LE32(fnv32a(k)^i) || LE32(i), so k6 can regenerate the exact
		// expected bytes for a given (k, kb). Mirrored by
		// test/integration/driver (chaos integrity scenario) and
		// bench/loadtest/scenarios/lib/payload.js.
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
		ctx.Write(deterministicPayload(k, kb))
	case "/large":
		kb, _ := strconv.Atoi(string(ctx.QueryArgs().Peek("kb")))
		if kb <= 0 {
			kb = 64
		}
		ctx.Response.Header.Set("Cache-Control", "max-age=3600")
		ctx.Response.Header.Set("Content-Type", "application/octet-stream")
		ctx.Write(make([]byte, kb*1024))
	case "/unique":
		ctx.Response.Header.Set("Cache-Control", "max-age=3600")
		fmt.Fprintf(ctx, "unique %s at %s", ctx.Path(), time.Now().Format(time.RFC3339Nano))
	case "/ttl":
		s, _ := strconv.Atoi(string(ctx.QueryArgs().Peek("s")))
		if s <= 0 {
			s = 60
		}
		ctx.Response.Header.Set("Cache-Control", fmt.Sprintf("max-age=%d", s))
		fmt.Fprintf(ctx, "ttl=%ds at %s", s, time.Now().Format(time.RFC3339Nano))
	case "/outlier":
		if time.Now().UnixNano()%20 == 0 {
			time.Sleep(2000 * time.Millisecond)
		} else {
			time.Sleep(10 * time.Millisecond)
		}
		ctx.Response.Header.Set("Cache-Control", "max-age=60")
		ctx.WriteString("outlier")
	default:
		ctx.SetStatusCode(fasthttp.StatusNotFound)
	}
}

// deterministicPayload builds the deterministic body served by /payload:
// kb KiB of 8-byte blocks where block i is LE32(fnv32a(k)^i) || LE32(i).
// Clients regenerate the same bytes to verify cache integrity (chaos
// driver and the k6 payload helper mirror this scheme).
func deterministicPayload(k string, kb int) []byte {
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
