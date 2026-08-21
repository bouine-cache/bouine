package cluster

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// NewPeerPurgeHandler returns an http.Handler that decodes binary
// PurgeEvent frames and delegates to fn. Mounted at POST /v1/peer/purge.
func NewPeerPurgeHandler(fn func(api.PurgeEvent) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		evt, err := DecodePurgeHTTP(body)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := fn(evt); err != nil {
			writePeerError(w, err)
			return
		}
		writePeerOK(w, "purged")
	})
}

// NewPeerBanHandler returns an http.Handler that decodes binary
// BanEvent frames and delegates to fn. Mounted at POST /v1/peer/ban.
func NewPeerBanHandler(fn func(api.BanEvent) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		evt, err := DecodeBanHTTP(body)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := fn(evt); err != nil {
			writePeerError(w, err)
			return
		}
		writePeerOK(w, "banned")
	})
}

// NewPeerRefreshHandler returns an http.Handler that decodes binary
// RefreshEvent frames and delegates to fn. Mounted at POST /v1/peer/refresh.
func NewPeerRefreshHandler(fn func(api.RefreshEvent) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		evt, err := DecodeRefreshHTTP(body)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := fn(evt); err != nil {
			writePeerError(w, err)
			return
		}
		writePeerOK(w, "refreshed")
	})
}

func writePeerError(w http.ResponseWriter, err error) {
	w.Header().Set(header.ContentType, "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func writePeerOK(w http.ResponseWriter, status string) {
	w.Header().Set(header.ContentType, "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": status})
}
