package cluster

import (
	"encoding/json"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// NewPeerPurgeHandler returns a fasthttp.RequestHandler that decodes
// binary PurgeEvent frames and delegates to fn. Mounted at POST /v1/peer/purge.
func NewPeerPurgeHandler(fn func(api.PurgeEvent) error) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		body := ctx.PostBody()
		if len(body) > 1<<20 {
			ctx.Error("bad request", fasthttp.StatusBadRequest)
			return
		}
		evt, err := DecodePurgeHTTP(body)
		if err != nil {
			ctx.Error("bad request", fasthttp.StatusBadRequest)
			return
		}
		if err := fn(evt); err != nil {
			writePeerError(ctx, err)
			return
		}
		writePeerOK(ctx, "purged")
	}
}

// NewPeerBanHandler returns a fasthttp.RequestHandler that decodes
// binary BanEvent frames and delegates to fn. Mounted at POST /v1/peer/ban.
func NewPeerBanHandler(fn func(api.BanEvent) error) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		body := ctx.PostBody()
		if len(body) > 1<<20 {
			ctx.Error("bad request", fasthttp.StatusBadRequest)
			return
		}
		evt, err := DecodeBanHTTP(body)
		if err != nil {
			ctx.Error("bad request", fasthttp.StatusBadRequest)
			return
		}
		if err := fn(evt); err != nil {
			writePeerError(ctx, err)
			return
		}
		writePeerOK(ctx, "banned")
	}
}

// NewPeerRefreshHandler returns a fasthttp.RequestHandler that decodes
// binary RefreshEvent frames and delegates to fn. Mounted at POST /v1/peer/refresh.
func NewPeerRefreshHandler(fn func(api.RefreshEvent) error) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		body := ctx.PostBody()
		if len(body) > 1<<20 {
			ctx.Error("bad request", fasthttp.StatusBadRequest)
			return
		}
		evt, err := DecodeRefreshHTTP(body)
		if err != nil {
			ctx.Error("bad request", fasthttp.StatusBadRequest)
			return
		}
		if err := fn(evt); err != nil {
			writePeerError(ctx, err)
			return
		}
		writePeerOK(ctx, "refreshed")
	}
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
