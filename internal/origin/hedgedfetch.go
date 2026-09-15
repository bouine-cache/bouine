package origin

import (
	"context"
	"time"

	"github.com/valyala/fasthttp"
)

// Hedged fetch mechanics for PoolFastClient: the primary attempt fires
// immediately and, if it has not completed after the hedge delay, a
// duplicate fires against the same pool. The first response wins; the
// losing attempt is aborted via its context and reaped.
//
// Both attempts run through the pool's normal fetch path, so target
// selection, URI rewriting, metrics, and passive health accounting apply
// to each attempt exactly as they do to a single fetch. A hedge doubles
// origin traffic on slow requests — the operator opts in via
// connect.hedge_timeout.
//
// Idempotent methods only (GET/HEAD/OPTIONS): the second attempt is a
// re-execution, which must be safe for the method. SSE-intent requests
// fetch singly regardless of the knob: the stream client's idle
// deadlines make the body open-ended, and a hedge would double an
// unbounded stream for no tail-latency benefit.

// isIdempotent returns true for HTTP methods that are safe to hedge:
// the second attempt is a re-execution of the request, which must not
// have visible side effects.
func isIdempotent(method string) bool {
	return method == fasthttp.MethodGet ||
		method == fasthttp.MethodHead ||
		method == fasthttp.MethodOptions
}

// hedgingAllowed reports whether a request may be hedged: idempotent
// method and not an SSE-intent fetch (which would double an open-ended
// stream).
func hedgingAllowed(req *fasthttp.Request, streamClient *fasthttp.Client) bool {
	if !isIdempotent(string(req.Header.Method())) {
		return false
	}
	if streamClient != nil && isSSERequest(req) {
		return false
	}
	return true
}

// doHedged runs fire twice, hedge seconds apart, and returns the first
// successful-or-final outcome. Semantics:
//
//   - Primary completes within delay → its result wins immediately
//     (even an error: the hedge has not fired yet, so there is nothing
//     to race against).
//   - Primary still in flight after delay → the duplicate fires; the
//     first completion wins. If the winner is an error, the reaper
//     waits for the other attempt: an error from a request that raced
//     against a still-successful duplicate must not fail the caller.
//     The context cancellation (cancel) only aborts the LOSING
//     attempt's transport after both outcomes are in.
//   - ctx is cancelled (client gone, handler shutdown) → the in-flight
//     attempts observe it through fn's transport and the select falls
//     through to whichever result arrives; the loser is reaped by the
//     buffered channel either way.
//
// fire must be safe to call twice for the same logical request: it
// clones the request itself (the caller's req is owned by the primary).
// The channel is buffered with capacity 2 so the losing goroutine
// always delivers its result and exits — no goroutine leak, no
// use-after-release of pooled fasthttp objects.
func doHedged(
	ctx context.Context,
	delay time.Duration,
	fire func(attemptCtx context.Context) error,
) error {
	_, err := doHedgedResponse(ctx, delay, func(aCtx context.Context) (any, error) {
		return nil, fire(aCtx)
	})
	return err
}

// doHedgedResponse is doHedged carrying a per-attempt payload (the
// pooled winning response) alongside the error. The winning result is
// returned to the caller; the losing attempt's result stays buffered on
// its channel and is released by the loser's own error path or by the
// next iteration of the pooled channel (the attempt releases its own
// response before delivering an error, and a successful loser is only
// possible in the wait-for-second branch, which is error-path-only).
// The cancel aborts the losing attempt's transport where the fetch is
// ctx-aware; the absolute-deadline path aborts at the kernel deadline,
// so the loser's goroutine may linger until then — bounded, not leaked.
//
//nolint:contextcheck // the DoDeadline path passes nil ctx by contract; hedging there is bounded by the per-attempt kernel deadline
func doHedgedResponse[T any](
	ctx context.Context,
	delay time.Duration,
	fire func(attemptCtx context.Context) (T, error),
) (T, error) {
	// The DoDeadline path has no context; hedging there is bounded by
	// the per-attempt kernel deadline instead of a cancelable parent.
	if ctx == nil {
		ctx = context.TODO()
	}
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resCh := make(chan attemptResult[T], 2)

	go func() { resCh <- runAttempt(attemptCtx, fire) }()

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case res := <-resCh:
		return res.val, res.err
	case <-timer.C:
		go func() { resCh <- runAttempt(attemptCtx, fire) }()
	}

	first := <-resCh
	cancel()
	if first.err == nil {
		// The first attempt won. The loser's goroutine is aborted by
		// the cancel where the transport observes ctx (the ctx-aware Do
		// path). The DoDeadline path aborts at the kernel deadline only,
		// so the loser may linger until its own deadline — its result
		// is buffered and the goroutine exits without further work;
		// nothing is leaked, it is merely released late. Never wait for
		// the loser here: a winner must return immediately (that is
		// the entire point of hedging).
		return first.val, nil
	}
	second := <-resCh
	if second.err == nil {
		// The hedge won with a response; the first attempt's failure
		// carried no response (released on its error path).
		return second.val, nil
	}
	return first.val, first.err
}

// attemptResult is one attempt's outcome. val may be non-nil with a
// nil err (success) or nil with a non-nil err (failure). The value owns
// pooled resources until the caller reaps or consumes it.
type attemptResult[T any] struct {
	val T
	err error
}

// runAttempt wraps fire so both attempts deliver exactly one result.
func runAttempt[T any](aCtx context.Context, fire func(context.Context) (T, error)) attemptResult[T] {
	val, err := fire(aCtx)
	return attemptResult[T]{val: val, err: err}
}
