// Echo origin used by the bouine integration harness. It is the
// smallest possible reflection server: every request is echoed back as
// a JSON document so tests can assert on what bouine forwarded.
//
// Endpoints:
//
//	GET  /healthz       -> 200 "ok"
//	ANY  /echo          -> 200 JSON {method, path, headers, body}
//	GET  /slow?ms=NNN   -> 200 after NNN ms
//	GET  /flaky?rate=p  -> 503 with probability p, else 200
//
// Build target: `go build .` inside this directory.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

type echoResponse struct {
	Method  string              `json:"method"`
	Path    string              `json:"path"`
	Headers map[string][]string `json:"headers"`
	Body    string              `json:"body"`
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/echo", echo)
	mux.HandleFunc("/slow", slow)
	mux.HandleFunc("/flaky", flaky)

	log.Printf("origin listening on %s", *addr)
	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("origin: %v", err)
	}
}

func echo(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	defer r.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(echoResponse{
		Method:  r.Method,
		Path:    r.URL.RequestURI(),
		Headers: r.Header,
		Body:    string(body),
	})
}

func slow(w http.ResponseWriter, r *http.Request) {
	ms, _ := strconv.Atoi(r.URL.Query().Get("ms"))
	if ms <= 0 {
		ms = 100
	}
	time.Sleep(time.Duration(ms) * time.Millisecond)
	fmt.Fprintf(w, "slept %dms\n", ms)
}

func flaky(w http.ResponseWriter, r *http.Request) {
	rate, _ := strconv.ParseFloat(r.URL.Query().Get("rate"), 64)
	if rate < 0 || rate > 1 {
		rate = 0.5
	}
	if rand.Float64() < rate { //nolint:gosec // test fixture
		http.Error(w, "flaky", http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ok\n"))
}
