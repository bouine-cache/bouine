package origin

import (
	"context"
	"time"

	"github.com/bouine-cache/bouine/internal/transport"

	"github.com/valyala/fasthttp"
)

// HedgeClient wraps a transport.Client and fires a duplicate request
// after a timeout. The first response wins; the loser is cancelled.
// Only used for idempotent methods (GET, HEAD, OPTIONS).
//
// Stable.
type HedgeClient struct {
	Inner   *transport.Client
	Timeout time.Duration
}

type hedgeResult struct {
	resp *fasthttp.Response
	err  error
}

// Do fires the primary request immediately. If it does not complete
// within h.Timeout, a duplicate is fired. The first to return wins.
// The caller is responsible for acquiring/releasing req. The winning
// response is a freshly acquired *fasthttp.Response that the caller
// must release. The loser response is released internally.
func (h *HedgeClient) Do(ctx context.Context, req *fasthttp.Request) (*fasthttp.Response, error) {
	if !isIdempotent(string(req.Header.Method())) || h.Timeout <= 0 {
		resp := fasthttp.AcquireResponse()
		err := h.Inner.Do(ctx, req, resp)
		if err != nil {
			fasthttp.ReleaseResponse(resp)
			return nil, err
		}
		return resp, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch := make(chan hedgeResult, 2)

	fire := func() {
		cloneReq := fasthttp.AcquireRequest()
		req.CopyTo(cloneReq)
		defer fasthttp.ReleaseRequest(cloneReq)

		resp := fasthttp.AcquireResponse()
		err := h.Inner.Do(ctx, cloneReq, resp)
		ch <- hedgeResult{resp, err}
	}

	// Primary request.
	go fire()

	timer := time.NewTimer(h.Timeout)
	defer timer.Stop()

	select {
	case res := <-ch:
		if res.err != nil {
			fasthttp.ReleaseResponse(res.resp)
			return nil, res.err
		}
		go func() {
			loser := <-ch
			if loser.resp != nil {
				fasthttp.ReleaseResponse(loser.resp)
			}
		}()
		return res.resp, nil
	case <-timer.C:
		go fire()
	}

	res := <-ch
	if res.err != nil {
		fasthttp.ReleaseResponse(res.resp)
		go func() {
			loser := <-ch
			if loser.resp != nil {
				fasthttp.ReleaseResponse(loser.resp)
			}
		}()
		return nil, res.err
	}

	go func() {
		loser := <-ch
		if loser.resp != nil {
			fasthttp.ReleaseResponse(loser.resp)
		}
	}()
	return res.resp, nil
}

// isIdempotent returns true for HTTP methods that are safe to hedge.
func isIdempotent(method string) bool {
	return method == fasthttp.MethodGet ||
		method == fasthttp.MethodHead ||
		method == fasthttp.MethodOptions
}
