package cluster

import (
	"encoding/json"
	"fmt"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// maxPeerInvalidationBody caps single-event peer invalidation bodies.
const maxPeerInvalidationBody = 1 << 20

// newPeerInvalidationHandler is the shared body of every single-event
// peer invalidation endpoint: size-check the body, decode the binary
// frame, delegate, and write the JSON ack. Only the decoder and the
// status string differ per event kind.
func newPeerInvalidationHandler[E any](
	decode func([]byte) (E, error),
	fn func(E) error,
	status string,
) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		body := ctx.PostBody()
		if len(body) > maxPeerInvalidationBody {
			ctx.Error("bad request", fasthttp.StatusBadRequest)
			return
		}
		evt, err := decode(body)
		if err != nil {
			ctx.Error("bad request", fasthttp.StatusBadRequest)
			return
		}
		if err := fn(evt); err != nil {
			writePeerError(ctx, err)
			return
		}
		writePeerOK(ctx, status)
	}
}

// NewPeerPurgeHandler returns a fasthttp.RequestHandler that decodes
// binary PurgeEvent frames and delegates to fn. Mounted at POST /v1/peer/purge.
func NewPeerPurgeHandler(fn func(api.PurgeEvent) error) fasthttp.RequestHandler {
	return newPeerInvalidationHandler(DecodePurgeHTTP, fn, "purged")
}

// NewPeerBanHandler returns a fasthttp.RequestHandler that decodes
// binary BanEvent frames and delegates to fn. Mounted at POST /v1/peer/ban.
func NewPeerBanHandler(fn func(api.BanEvent) error) fasthttp.RequestHandler {
	return newPeerInvalidationHandler(DecodeBanHTTP, fn, "banned")
}

// NewPeerRefreshHandler returns a fasthttp.RequestHandler that decodes
// binary RefreshEvent frames and delegates to fn. Mounted at POST /v1/peer/refresh.
func NewPeerRefreshHandler(fn func(api.RefreshEvent) error) fasthttp.RequestHandler {
	return newPeerInvalidationHandler(DecodeRefreshHTTP, fn, "refreshed")
}

func writePeerError(ctx *fasthttp.RequestCtx, err error) {
	ctx.Response.Header.Set(header.ContentType, "application/json")
	ctx.SetStatusCode(fasthttp.StatusInternalServerError)
	_ = json.NewEncoder(ctx).Encode(map[string]string{"error": err.Error()})
}

func writePeerOK(ctx *fasthttp.RequestCtx, status string) {
	ctx.Response.Header.Set(header.ContentType, "application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	_ = json.NewEncoder(ctx).Encode(map[string]string{"status": status})
}

// newPeerInvalidationBatchHandler is the batch counterpart of
// newPeerInvalidationHandler: decode a batch frame and delegate each
// event to fn (ADR-0044).
func newPeerInvalidationBatchHandler[E any](
	decode func([]byte) ([]E, error),
	fn func(E) error,
	status string,
) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		body := ctx.PostBody()
		if len(body) > 4<<20 {
			ctx.Error("bad request", fasthttp.StatusBadRequest)
			return
		}
		evts, err := decode(body)
		if err != nil {
			ctx.Error("bad request", fasthttp.StatusBadRequest)
			return
		}
		applied := 0
		for _, evt := range evts {
			if err := fn(evt); err != nil {
				writePeerError(ctx, err)
				return
			}
			applied++
		}
		writePeerOK(ctx, fmt.Sprintf("%s %d", status, applied))
	}
}

// NewPeerPurgeBatchHandler returns a fasthttp.RequestHandler that
// decodes a batch of PurgeEvents and delegates each to fn. Mounted at
// POST /v1/peer/purge/batch (ADR-0044).
func NewPeerPurgeBatchHandler(fn func(api.PurgeEvent) error) fasthttp.RequestHandler {
	return newPeerInvalidationBatchHandler(DecodePurgeBatchHTTP, fn, "purged")
}

// NewPeerRefreshBatchHandler returns a fasthttp.RequestHandler that
// decodes a batch of RefreshEvents and delegates each to fn. Mounted at
// POST /v1/peer/refresh/batch (ADR-0044).
func NewPeerRefreshBatchHandler(fn func(api.RefreshEvent) error) fasthttp.RequestHandler {
	return newPeerInvalidationBatchHandler(DecodeRefreshBatchHTTP, fn, "refreshed")
}
