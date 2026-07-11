package origin

import (
	"context"
	"net/http"
	"time"
)

// HedgedTransport wraps an http.RoundTripper and fires a duplicate
// request after a timeout. The first response wins; the loser is
// cancelled. Only used for idempotent methods (GET, HEAD, OPTIONS).
//
// Stable.
type HedgedTransport struct {
	Inner   http.RoundTripper
	Timeout time.Duration
}

type hedgeResult struct {
	resp *http.Response
	err  error
}

// RoundTrip fires the primary request immediately. If it does not
// complete within h.Timeout, a duplicate is fired. The first to
// return wins.
func (h *HedgedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !isIdempotent(req.Method) || h.Timeout <= 0 {
		return h.Inner.RoundTrip(req)
	}

	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()

	ch := make(chan hedgeResult, 2)

	fire := func(clone bool) {
		var r *http.Request
		if clone {
			r = req.Clone(ctx)
		} else {
			r = req.WithContext(ctx)
		}
		resp, err := h.Inner.RoundTrip(r) //nolint:bodyclose // closed by loser cleanup goroutine
		ch <- hedgeResult{resp, err}
	}

	// Primary request — no clone needed, the original request is not
	// used again unless the hedge fires (which clones it).
	go fire(false)

	timer := time.NewTimer(h.Timeout)
	defer timer.Stop()

	select {
	case res := <-ch:
		return res.resp, res.err
	case <-timer.C:
		go fire(true)
	}

	res := <-ch
	go func() {
		loser := <-ch
		if loser.resp != nil {
			_ = loser.resp.Body.Close()
		}
	}()
	return res.resp, res.err
}

func isIdempotent(method string) bool {
	return method == http.MethodGet ||
		method == http.MethodHead ||
		method == http.MethodOptions
}
