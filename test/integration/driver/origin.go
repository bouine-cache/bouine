//go:build integration

package driver

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"time"
)

// originControl provides runtime knobs for chaos testing.
type originControl struct {
	latencyMs atomic.Int64 // injected latency per request (0 = none)
	forceErr  atomic.Bool  // when true, all requests return 503
}

// startOrigin creates an httptest server with controllable chaos knobs.
func startOriginWithControl() (*httptest.Server, *originControl) {
	ctrl := &originControl{}
	mux := http.NewServeMux()

	// Chaos middleware: latency injection + forced errors.
	wrap := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if ctrl.forceErr.Load() {
				w.WriteHeader(503)
				fmt.Fprint(w, "origin forced error")
				return
			}
			if ms := ctrl.latencyMs.Load(); ms > 0 {
				<-time.After(time.Duration(ms) * time.Millisecond)
			}
			next(w, r)
		}
	}

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "ok")
	})
	mux.HandleFunc("/hit", wrap(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "max-age=3600")
		w.Header().Set("ETag", `"hit-v1"`)
		fmt.Fprintf(w, "hit at %s", time.Now().Format(time.RFC3339Nano))
	}))
	mux.HandleFunc("/miss", wrap(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		fmt.Fprintf(w, "miss at %s", time.Now().Format(time.RFC3339Nano))
	}))
	mux.HandleFunc("/bypass", wrap(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "private")
		fmt.Fprintf(w, "bypass at %s", time.Now().Format(time.RFC3339Nano))
	}))
	mux.HandleFunc("/stale", wrap(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "max-age=1, stale-while-revalidate=3600")
		w.Header().Set("ETag", `"stale-v1"`)
		fmt.Fprintf(w, "stale at %s", time.Now().Format(time.RFC3339Nano))
	}))
	mux.HandleFunc("/revalidate", wrap(func(w http.ResponseWriter, r *http.Request) {
		etag := `"reval-v1"`
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Cache-Control", "max-age=0, must-revalidate")
		w.Header().Set("ETag", etag)
		fmt.Fprintf(w, "revalidate at %s", time.Now().Format(time.RFC3339Nano))
	}))
	mux.HandleFunc("/vary", wrap(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=3600")
		w.Header().Set("Vary", "Accept-Encoding")
		fmt.Fprintf(w, "vary enc=%s", r.Header.Get("Accept-Encoding"))
	}))
	mux.HandleFunc("/error", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(503)
		fmt.Fprint(w, "origin error")
	})
	mux.HandleFunc("/slow", wrap(func(w http.ResponseWriter, r *http.Request) {
		ms, _ := strconv.Atoi(r.URL.Query().Get("ms"))
		if ms <= 0 {
			ms = 500
		}
		<-time.After(time.Duration(ms) * time.Millisecond)
		w.Header().Set("Cache-Control", "max-age=60")
		fmt.Fprintf(w, "slow %dms", ms)
	}))
	mux.HandleFunc("/unique", wrap(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=3600")
		fmt.Fprintf(w, "unique %s at %s", r.URL.Path, time.Now().Format(time.RFC3339Nano))
	}))
	// Catch-all for chaos paths like /chaos/pk/0, /chaos/flap/1, etc.
	mux.HandleFunc("/", wrap(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=5, stale-if-error=60, stale-while-revalidate=60")
		fmt.Fprintf(w, "chaos %s at %s", r.URL.Path, time.Now().Format(time.RFC3339Nano))
	}))

	return httptest.NewServer(mux), ctrl
}

// startOrigin is the backward-compatible wrapper used by integration tests.
func startOrigin() *httptest.Server {
	srv, _ := startOriginWithControl()
	return srv
}
