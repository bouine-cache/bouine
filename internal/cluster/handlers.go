package cluster

import (
	"encoding/json"
	"errors"
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

// maxPeerSurrogateKeyLen caps the surrogate key in a peer refresh
// frame. The admin plane already enforces 256 bytes
// (admin.maxSurrogateKeyLen); this is the peer-side guard against a
// corrupted or hostile frame asking every node to scan its stores for
// an unbounded string.
const maxPeerSurrogateKeyLen = 256

// validPeerSurrogateKey applies the same token rules as the admin
// plane: exact opaque token, no whitespace, commas, or control bytes.
func validPeerSurrogateKey(sk string) bool {
	if sk == "" || len(sk) > maxPeerSurrogateKeyLen {
		return false
	}
	for i := 0; i < len(sk); i++ {
		c := sk[i]
		if c <= ' ' || c == ',' || c == 0x7f {
			return false
		}
	}
	return true
}

// NewPeerRefreshSurrogateHandler returns a fasthttp.RequestHandler that
// decodes binary RefreshSurrogateEvent frames and delegates to fn.
// Mounted at POST /v1/peer/refresh/surrogate; the receiving node
// resolves the tag locally, so the fan-out stays one frame per peer.
// The tag is re-validated here (400 on garbage) because the frame may
// come from gossip replay or a misbehaving peer, not just the admin API.
func NewPeerRefreshSurrogateHandler(fn func(api.RefreshSurrogateEvent) error) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		body := ctx.PostBody()
		if len(body) > maxPeerInvalidationBody {
			ctx.Error("bad request", fasthttp.StatusBadRequest)
			return
		}
		evt, err := DecodeRefreshSurrogateHTTP(body)
		if err != nil {
			ctx.Error("bad request", fasthttp.StatusBadRequest)
			return
		}
		if !validPeerSurrogateKey(evt.SurrogateKey) {
			ctx.Error("bad request: invalid surrogate_key", fasthttp.StatusBadRequest)
			return
		}
		if err := fn(evt); err != nil {
			writePeerError(ctx, err)
			return
		}
		writePeerOK(ctx, "refreshed")
	}
}

// NewPeerHotSetHandler returns a fasthttp.RequestHandler that decodes
// HotSetEvent JSON frames and delegates to fn. Mounted at POST
// /v1/peer/config/hot: the receiving node applies the runtime knob
// locally and never re-broadcasts (loop-safe by construction).
func NewPeerHotSetHandler(fn func(api.HotSetEvent) error) fasthttp.RequestHandler {
	return newPeerInvalidationHandler(DecodeHotSetHTTP, fn, "set")
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

// ErrStaleConfig is returned by a peer reload applier when its own
// resolved config hash differs from the fan-out event's — ${VAR} env
// divergence or a different boot file (issue #951 decision 8). The
// handler maps it to 400 with a stale_config ack so the initiator can
// tell it apart from a malformed frame.
var ErrStaleConfig = fmt.Errorf("stale_config")

// NewPeerConfigReloadHandler returns a fasthttp.RequestHandler that
// decodes ConfigReloadEvent JSON frames and delegates to fn, writing
// the fn-returned ack as the response body. Mounted at POST
// /v1/peer/config/reload: the receiving node applies locally and never
// re-broadcasts (loop-free, decision 9). A fn error of ErrStaleConfig
// answers 400 with {"status":"stale_config"}; any other error answers
// 500.
func NewPeerConfigReloadHandler(fn func(api.ConfigReloadEvent) (api.ConfigReloadAck, error)) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		body := ctx.PostBody()
		if len(body) > maxPeerInvalidationBody {
			ctx.Error("bad request", fasthttp.StatusBadRequest)
			return
		}
		evt, err := DecodeConfigReloadHTTP(body)
		if err != nil {
			ctx.Error("bad request", fasthttp.StatusBadRequest)
			return
		}
		ack, err := fn(evt)
		if err != nil {
			if errors.Is(err, ErrStaleConfig) {
				ctx.Response.Header.Set(header.ContentType, "application/json")
				ctx.SetStatusCode(fasthttp.StatusBadRequest)
				ack.Status = "stale_config"
				_ = json.NewEncoder(ctx).Encode(ack)
				return
			}
			writePeerError(ctx, err)
			return
		}
		ctx.Response.Header.Set(header.ContentType, "application/json")
		ctx.SetStatusCode(fasthttp.StatusOK)
		b, _ := EncodeConfigReloadAck(ack)
		_, _ = ctx.Write(b)
	}
}
