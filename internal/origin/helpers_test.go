package origin

import (
	"io"
	"net/http"
)

// newEchoHandler returns an http.Handler that echoes the request body
// and sets X-Echo-Host and X-Echo-Path headers.
func newEchoHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Echo-Host", r.Host)
		w.Header().Set("X-Echo-Path", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, r.Body)
	})
}

// new5xxHandler returns an http.Handler that always returns 500.
func new5xxHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
}
