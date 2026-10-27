package main

import (
	"fmt"
	"hash/fnv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/valyala/fasthttp"
)

// Origin shapes mirror the audited SSR tier: cacheable HTML bodies 30-300 KiB
// with 50-300 ms TTFB (hit-heavy profile), and a directive-unstorable page
// (private, no-store) around 90 ms TTFB for the herd profile that measures
// the fast-path peer fetch on remote-owned unstorable keys (F12).
const (
	ssrMinBody   = 30 << 10
	ssrBodyRange = 270 << 10
	ssrMinTTFB   = 50 * time.Millisecond
	ssrTTFBRange = 250 * time.Millisecond
	nostoreTTFB  = 80 * time.Millisecond
	nostoreTTFBJ = 25 * time.Millisecond
	nostoreBody  = 8 << 10
)

type originServer struct {
	addr        string
	srv         *fasthttp.Server
	body        []byte
	ssrRendered atomic.Int64
	nostoreSeen atomic.Int64
}

func startOrigin() (*originServer, error) {
	ln, err := listenLocal()
	if err != nil {
		return nil, err
	}
	o := &originServer{addr: ln.Addr().String()}
	o.body = make([]byte, ssrMinBody+ssrBodyRange)
	for i := range o.body {
		o.body[i] = byte('a' + i%26)
	}
	o.srv = &fasthttp.Server{Handler: o.handle}
	go func() { _ = o.srv.Serve(ln) }()
	return o, nil
}

func (o *originServer) stop() {
	_ = o.srv.Shutdown()
}

type originStats struct {
	ssrRendered int64
	nostoreSeen int64
}

func (o *originServer) stats() originStats {
	return originStats{ssrRendered: o.ssrRendered.Load(), nostoreSeen: o.nostoreSeen.Load()}
}

func (o *originServer) handle(ctx *fasthttp.RequestCtx) {
	path := string(ctx.Path())
	switch {
	case strings.HasPrefix(path, "/ssr/"):
		h := hashKey(path[len("/ssr/"):])
		time.Sleep(ssrMinTTFB + time.Duration(h%uint64(ssrTTFBRange)))
		ctx.Response.Header.Set("Content-Type", "text/html; charset=utf-8")
		ctx.Response.Header.Set("Cache-Control", "public, max-age=600")
		ctx.Response.Header.Set("ETag", fmt.Sprintf(`"ssr-%x"`, h))
		_, _ = ctx.Write(o.body[:ssrMinBody+int(h%uint64(ssrBodyRange))])
		o.ssrRendered.Add(1)
	case strings.HasPrefix(path, "/nostore/"):
		h := hashKey(path[len("/nostore/"):])
		time.Sleep(nostoreTTFB + time.Duration(h%uint64(nostoreTTFBJ)))
		ctx.Response.Header.Set("Content-Type", "text/html; charset=utf-8")
		ctx.Response.Header.Set("Cache-Control", "private, no-store")
		_, _ = ctx.Write(o.body[:nostoreBody])
		o.nostoreSeen.Add(1)
	default:
		ctx.Error("not found", fasthttp.StatusNotFound)
	}
}

func hashKey(key string) uint64 {
	f := fnv.New64a()
	_, _ = f.Write([]byte(key))
	return f.Sum64()
}
