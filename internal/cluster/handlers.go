package cluster

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// maxReplicateBodySize caps the size of a replication POST body (10MB).
// The body is a storage.EncodeObject blob; 10MB accommodates the largest
// expected cached response with headroom for headers and metadata.
const maxReplicateBodySize = 10 << 20

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

// NewPeerReplicateHandler returns an http.Handler that decodes a binary
// storage.EncodeObject blob from the request body and stores it locally.
// Event metadata (issuer, seq, issued-at, method) is read from HTTP
// headers. Mounted at POST /v1/peer/replicate (auth-exempt; same
// rationale as peer purge/ban — callers are trusted cluster peers).
func NewPeerReplicateHandler(fn func(ctx context.Context, obj *api.Object) error, metrics *Metrics, logger observability.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, maxReplicateBodySize))
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		obj, err := storage.DecodeObject(body)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		issuer := r.Header.Get(header.BouineIssuer)
		if issuer == "" {
			issuer = "unknown"
		}

		if err := fn(r.Context(), obj); err != nil {
			writePeerError(w, err)
			return
		}

		metrics.IncReplicationReceived()
		metrics.AddReplicationBytes("received", float64(len(body)))
		logger.Info("received replication from peer",
			"key", obj.Key,
			"issuer", issuer,
			"bytes", len(body),
		)
		w.WriteHeader(http.StatusNoContent)
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
