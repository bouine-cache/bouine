//go:build integration

package driver

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"time"
)

// startOrigin creates an httptest server with the same endpoints as the
// test-origin binary, running in-process.
func startOrigin() *httptest.Server {
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
		fmt.Fprintf(w, "vary enc=%s", r.Header.Get("Accept-Encoding"))
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
	mux.HandleFunc("/unique", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=3600")
		fmt.Fprintf(w, "unique %s at %s", r.URL.Path, time.Now().Format(time.RFC3339Nano))
	})
	return httptest.NewServer(mux)
}
