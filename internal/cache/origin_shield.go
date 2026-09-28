// Package cache: origin shielding — on a cold key the ring owner
// drives one collapsed origin fetch on behalf of peer waiters, so the
// whole cluster costs one origin request per key instead of one per node.
package cache

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

// errCoalesceMethod: the owner never fetches origin for a
// non-cacheable method.
var errCoalesceMethod = errors.New("coalesced fetch: method is not GET/HEAD")

// coalesceWaitTimeout is the waiter-side wait budget; the cluster
// package bounds the RPC with the same api.CoalesceFetchTimeout.
const coalesceWaitTimeout = api.CoalesceFetchTimeout

// coalesceOutcome reports how the coalesced owner call ended for a
// hard miss. The distinction matters because coalesceWait returning
// without serving means two different things: the owner call was
// attempted and failed (plain peer fetch must not retry a third RPC),
// or the request never qualified (plain peer fetch still applies).
type coalesceOutcome int

const (
	// coalesceNotApplied: shielding does not apply (not a hard miss,
	// non-GET/HEAD, or the feature is off) — the plain peer fetch runs.
	coalesceNotApplied coalesceOutcome = iota
	// coalesceAttempted: the owner call ran but produced no servable
	// answer — the plain peer fetch is skipped (its answer would come
	// from the same owner that just failed or shed).
	coalesceAttempted
	// coalesceServed: the authoritative answer was written to the
	// client; the caller returns immediately.
	coalesceServed
)

// coalesceApplies: origin shielding is GET/HEAD-only and hard-miss
// only; a stale-usable object keeps plain peer-fetch semantics.
func (h *Handler) coalesceApplies(obj *api.Object, method []byte) bool {
	if h.peerFetchCoalesce == nil || obj != nil {
		return false
	}
	return string(method) == fasthttp.MethodGet || string(method) == fasthttp.MethodHead
}

// coalesceWait asks the owner to drive the origin fetch and serves its
// authoritative answer. coalesceServed means the response was written;
// coalesceAttempted means the owner call ran and failed (origin fetch
// follows, plain peer fetch must not); coalesceNotApplied means the
// request never qualified and the caller keeps today's plain peer fetch.
func (h *Handler) coalesceWait(ctx *fasthttp.RequestCtx, owner api.PeerInfo, lookupKey api.Key, obj *api.Object, now time.Time, ri RequestInfo) coalesceOutcome {
	if !h.coalesceApplies(obj, ctx.Method()) {
		return coalesceNotApplied
	}
	oreq := h.originEnvelope(ctx)
	// A fasthttp RequestCtx has no Done channel: WithoutCancel avoids
	// the WithTimeout panic. Client cancellation is not propagated; the
	// wait is bounded by coalesceWaitTimeout and the owner's flight is
	// detached anyway.
	waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), coalesceWaitTimeout)
	defer cancel()
	// Hard miss: no local object to derive a Vary assertion from.
	peerObj, err := h.peerFetchCoalesce(waitCtx, owner, lookupKey, "", oreq)
	if err != nil || peerObj == nil {
		h.logger.Debug("coalesced peer fetch failed, falling back to origin",
			"peer", owner.Addr, "key", lookupKey, "error", err)
		if h.onCoalescedFallback != nil {
			h.onCoalescedFallback()
		}
		return coalesceAttempted
	}
	if !h.servePeerHit(ctx, lookupKey, peerObj, now, ri) {
		// Variant gate rejection or staleness: fall back to origin.
		if h.onCoalescedFallback != nil {
			h.onCoalescedFallback()
		}
		return coalesceAttempted
	}
	if h.onCoalescedSaved != nil {
		h.onCoalescedSaved()
	}
	h.maybeBackfill(ctx, lookupKey, peerObj)
	return coalesceServed
}

// originEnvelope builds the owner's upstream replay: method, URI,
// host, and only the headers that change the origin's answer —
// Accept-Encoding, Authorization, and the route's include_headers.
// Everything else is dropped. A Vary on a dropped header is caught by
// the owner/waiter VaryKey comparison: degraded hit rate, never a
// wrong-variant serve.
func (h *Handler) originEnvelope(ctx *fasthttp.RequestCtx) *api.OriginRequest {
	// HEAD→GET: a HEAD origin fetch yields an empty body; the waiter
	// suppresses the body for HEAD clients when serving.
	method := string(ctx.Method())
	if method == fasthttp.MethodHead {
		method = fasthttp.MethodGet
	}
	oreq := &api.OriginRequest{
		Method: method,
		URI:    string(ctx.RequestURI()),
		Host:   string(ctx.Host()),
	}
	for k, v := range ctx.Request.Header.All() {
		name := strings.ToLower(string(k))
		if name == "accept-encoding" || name == "authorization" || h.policyIncludesHeader(name) {
			oreq.Headers = append(oreq.Headers, api.PeerHeader{Name: string(k), Value: string(v)})
		}
	}
	return oreq
}

// policyIncludesHeader reports whether the policy consults the header.
func (h *Handler) policyIncludesHeader(name string) bool {
	if h.policy == nil {
		return false
	}
	return slices.Contains(h.policy.includeHeaders, name)
}

// maybeBackfill stores a coalesced object locally with probability
// peerBackfillProbability; p=0 keeps the owner-only partition. The
// owner always keeps its own fill.
func (h *Handler) maybeBackfill(ctx *fasthttp.RequestCtx, lookupKey api.Key, obj *api.Object) {
	if h.peerBackfillProbability <= 0 {
		return
	}
	// Same admission as the local fill path: oversized bodies are
	// served but never stored.
	if h.maxObjectSize > 0 && int64(len(obj.Body)) > h.maxObjectSize {
		return
	}
	// Probabilistic placement, not a security decision (G404).
	if h.peerBackfillProbability < 1 && rand.Float64() >= h.peerBackfillProbability { //nolint:gosec // G404
		return
	}
	// The owner already applied the route's freshness policy: store
	// as-is, bypassing the ownership gate (backfill is the knob that
	// relaxes it).
	ri := RequestInfo{Method: fasthttp.MethodGet, Header: obj.Header.Clone()}
	h.storeObjectLocal(ctx, lookupKey, obj, ri, false, 0)
}

// FetchOrigin drives this route's origin fetch on behalf of a peer
// waiter (owner side of the shield). The fetch joins the route's
// foreground inflight latch — the same one a local miss uses — so peer
// waiters plus local requests collapse into one origin fetch. The
// object is stored locally and returned encoded. An origin error STATUS
// is an authoritative answer (negative caching survives); only a
// flight failure (transport error, shed, unshareable stream) errors.
func (h *Handler) FetchOrigin(ctx context.Context, key api.Key, originReq *fasthttp.Request) (*api.Object, error) {
	method := string(originReq.Header.Method())
	if method != fasthttp.MethodGet && method != fasthttp.MethodHead {
		return nil, errCoalesceMethod
	}
	// HEAD→GET, same canonicalization as the cache key: an empty HEAD
	// body must never be stored under the GET key.
	if method == fasthttp.MethodHead {
		method = fasthttp.MethodGet
		originReq.Header.SetMethod(fasthttp.MethodGet)
	}
	// Route through the route's origin-bound URI rewrite; request-header
	// rewrites are already applied in the envelope.
	originURI := h.originURI(originReq.RequestURI())
	originReq.SetRequestURIBytes(originURI)

	ri := RequestInfo{
		Method: method,
		URI:    string(originReq.RequestURI()),
		Host:   string(originReq.Host()),
		Path:   string(originReq.URI().Path()),
		Header: headerFromFastHTTPReqHeader(&originReq.Header),
	}

	// Join-or-lead the foreground latch, exactly as a local miss does.
	// Followers receive the leader's buffered result (already stored);
	// errors surface as the follower error.
	inflight := &inflightStream{done: make(chan struct{})}
	if actual, loaded := h.inflightStreams.loadOrStore(key, inflight); loaded {
		<-actual.done
		res := actual.res
		if res.Err != nil {
			return nil, res.Err
		}
		// buildObject mutates the header map and the result is shared
		// with followers: clone first.
		res.Header = res.Header.ownedClone()
		return buildObject(key, ri, res, res.Header.ToMap(), h.neg, h.defaultTTL,
			h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.policy, time.Now()), nil
	}
	// Leader: publish the result before returning so a late arrival
	// finds the object in the store.
	defer h.inflightStreams.delete(key)
	res := h.doFetchBg(ctx, originReq)
	inflight.res = res
	close(inflight.done)
	if res.Err != nil {
		return nil, res.Err
	}
	// Clone before buildObject: the published result is shared with
	// followers, and buildObject mutates the header map.
	res.Header = res.Header.ownedClone()
	obj := buildObject(key, ri, res, res.Header.ToMap(), h.neg, h.defaultTTL,
		h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.policy, time.Now())
	// Oversized bodies are served to the peer but never stored.
	if h.maxObjectSize > 0 && int64(len(obj.Body)) > h.maxObjectSize {
		return obj, nil
	}
	h.storeObjectLocal(ctx, key, obj, ri, false, 0)
	return obj, nil
}
