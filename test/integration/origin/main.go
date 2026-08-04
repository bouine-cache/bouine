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
	"net/http"
	"strconv"
	"time"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "ok")
	})
	mux.HandleFunc("/hit", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "max-age=3600")
		w.Header().Set("ETag", `"hit-v1"`)
		fmt.Fprintf(w, "hit at %s", time.Now().Format(time.RFC3339Nano))
	})
	mux.HandleFunc("/miss", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		fmt.Fprintf(w, "miss at %s", time.Now().Format(time.RFC3339Nano))
	})
	mux.HandleFunc("/bypass", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "private")
		fmt.Fprintf(w, "bypass at %s", time.Now().Format(time.RFC3339Nano))
	})
	mux.HandleFunc("/stale", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "max-age=1, stale-while-revalidate=3600")
		w.Header().Set("ETag", `"stale-v1"`)
		fmt.Fprintf(w, "stale at %s", time.Now().Format(time.RFC3339Nano))
	})
	mux.HandleFunc("/revalidate", func(w http.ResponseWriter, r *http.Request) {
		etag := `"reval-v1"`
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Cache-Control", "max-age=0, must-revalidate")
		w.Header().Set("ETag", etag)
		fmt.Fprintf(w, "revalidate at %s", time.Now().Format(time.RFC3339Nano))
	})
	mux.HandleFunc("/vary", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=3600")
		w.Header().Set("Vary", "Accept-Encoding")
		fmt.Fprintf(w, "vary enc=%s at %s", r.Header.Get("Accept-Encoding"), time.Now().Format(time.RFC3339Nano))
	})
	mux.HandleFunc("/heuristic", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Last-Modified", time.Now().Add(-24*time.Hour).UTC().Format(http.TimeFormat))
		fmt.Fprintf(w, "heuristic at %s", time.Now().Format(time.RFC3339Nano))
	})
	mux.HandleFunc("/error", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(503)
		fmt.Fprint(w, "origin error")
	})
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		ms, _ := strconv.Atoi(r.URL.Query().Get("ms"))
		if ms <= 0 {
			ms = 500
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
		w.Header().Set("Cache-Control", "max-age=60")
		fmt.Fprintf(w, "slow %dms", ms)
	})

	mux.HandleFunc("/large", func(w http.ResponseWriter, r *http.Request) {
		kb, _ := strconv.Atoi(r.URL.Query().Get("kb"))
		if kb <= 0 {
			kb = 64
		}
		w.Header().Set("Cache-Control", "max-age=3600")
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(make([]byte, kb*1024))
	})
	mux.HandleFunc("/unique", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=3600")
		fmt.Fprintf(w, "unique %s at %s", r.URL.Path, time.Now().Format(time.RFC3339Nano))
	})
	mux.HandleFunc("/ttl", func(w http.ResponseWriter, r *http.Request) {
		s, _ := strconv.Atoi(r.URL.Query().Get("s"))
		if s <= 0 {
			s = 60
		}
		w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%d", s))
		fmt.Fprintf(w, "ttl=%ds at %s", s, time.Now().Format(time.RFC3339Nano))
	})
	mux.HandleFunc("/outlier", func(w http.ResponseWriter, r *http.Request) {
		// 5% of requests take 2000ms; rest take 10ms (for hedging tests)
		if time.Now().UnixNano()%20 == 0 {
			time.Sleep(2000 * time.Millisecond)
		} else {
			time.Sleep(10 * time.Millisecond)
		}
		w.Header().Set("Cache-Control", "max-age=60")
		fmt.Fprint(w, "outlier")
	})

	log.Printf("test-origin listening on %s", *addr)
	log.Fatal((&http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}).ListenAndServe())
}
