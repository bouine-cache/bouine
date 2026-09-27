// Package cache (origin_shield.go) implements the non-owner (waiter)
// and owner sides of cluster-coordinated origin shielding: on a cold
// key, the owner drives its own collapsed origin fetch on behalf of a
// peer waiter, so a mass purge / deploy / TTL expiry produces one
// origin request for the whole cluster instead of one per node.
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

// errCoalesceMethod is returned when a coalesced fetch envelope asks
// for a non-cacheable method: the owner never fetches origin for one.
var errCoalesceMethod = errors.New("coalesced fetch: method is not GET/HEAD")

// coalesceWaitTimeout bounds how long a waiter waits on the owner's
// coalesced origin fetch before giving up and falling back to its own
// origin fetch. It is the same constant the cluster package bounds the
// RPC with (api.CoalesceFetchTimeout) — one definition, two consumers.
const coalesceWaitTimeout = api.CoalesceFetchTimeout

// coalesceApplies reports whether the coalesced owner call applies to
// this miss: origin shielding is GET/HEAD-only (the only cacheable
// methods) and only on a hard miss — a stale-usable object keeps
// today's plain peer-fetch semantics (no coordinated revalidation).
func (h *Handler) coalesceApplies(obj *api.Object, method []byte) bool {
	if h.peerFetchCoalesce == nil || obj != nil {
		return false
	}
	return string(method) == fasthttp.MethodGet || string(method) == fasthttp.MethodHead
}

// coalesceWait runs the non-owner side of the origin shield: send the
// owner a coalesced peer-fetch with an OriginRequest envelope, wait for
// the authoritative answer, and serve it. Returns true when the
// response was written (caller must return). Any failure — owner down,
// lane shed, deadline, variant-gate rejection — returns false and the
// caller falls through to its own origin fetch.
func (h *Handler) coalesceWait(ctx *fasthttp.RequestCtx, owner api.PeerInfo, lookupKey api.Key, obj *api.Object, now time.Time, ri RequestInfo) bool {
	if !h.coalesceApplies(obj, ctx.Method()) {
		return false
	}
	oreq := h.originEnvelope(ctx)
	// WithoutCancel: a bare fasthttp RequestCtx has no Done channel, so
	// wrapping it directly in WithTimeout panics. Waiter-side client
	// cancellation is therefore not propagated into the RPC; the wait
	// is bounded by coalesceWaitTimeout instead, and the owner's flight
	// is detached from all waiters anyway (WithoutCancel in cluster).
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
		return false
	}
	if !h.servePeerHit(ctx, lookupKey, peerObj, now, ri) {
		// Variant gate rejection or not fresh enough: fall back to
		// origin rather than serve doubtful content.
		if h.onCoalescedFallback != nil {
			h.onCoalescedFallback()
		}
		return false
	}
	if h.onCoalescedSaved != nil {
		h.onCoalescedSaved()
	}
	h.maybeBackfill(ctx, lookupKey, peerObj)
	return true
}

// originEnvelope builds the OriginRequest the owner needs to replay the
// upstream fetch: method, request URI, host, and the closed set of
// headers that change the origin's ANSWER — Accept-Encoding (body
// identity), Authorization (the response is only cacheable with the
// shared-cache opt-in the local path applies to the same header), and
// every header the route's cache-key policy consults
// (cache.key.include_headers). Everything else is dropped. An
// origin-declared Vary header that is NOT in the envelope is caught
// downstream: the owner computes the object's VaryKey from the envelope
// headers, servePeerHit recomputes it from the real client headers, and
// a mismatch is rejected into the origin fallback — degraded hit rate,
// never wrong-variant content.
func (h *Handler) originEnvelope(ctx *fasthttp.RequestCtx) *api.OriginRequest {
	// HEAD→GET: keys are canonicalized HEAD→GET everywhere else (the
	// local miss path never stores a HEAD fill, stream.go's isHEAD
	// gate), and a HEAD origin fetch would return an empty body the
	// owner would cache under the GET key. The waiter suppresses the
	// body at write time (serveObject's HEAD check), so a GET replay
	// answers HEAD clients correctly.
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

// policyIncludesHeader reports whether the route's cache-key policy
// consults the (lower-cased) header name.
func (h *Handler) policyIncludesHeader(name string) bool {
	if h.policy == nil {
		return false
	}
	return slices.Contains(h.policy.includeHeaders, name)
}

// maybeBackfill stores a coalesced object locally with probability
// peerBackfillProbability. Fewer local copies means future requests on
// this pod re-peer-fetch the owner (acceptable, peer RTT); p=0 keeps
// the strict owner-only partition, p=1 stores every coalesced object.
// The owner always stores its own fill regardless of this knob — it
// fills through its own fetch path, never through backfill.
func (h *Handler) maybeBackfill(ctx *fasthttp.RequestCtx, lookupKey api.Key, obj *api.Object) {
	if h.peerBackfillProbability <= 0 {
		return
	}
	// Same storage admission as the local fill path (stream.go): a body
	// larger than the route's max_object_size must not enter the local
	// store, coalesced or not. The object is still served to THIS
	// client — only the local copy is skipped.
	if h.maxObjectSize > 0 && int64(len(obj.Body)) > h.maxObjectSize {
		return
	}
	// Non-crypto randomness: this is probabilistic cache placement, not
	// a security decision — predicting the roll gains an attacker
	// nothing (gosec G404 threat model).
	if h.peerBackfillProbability < 1 && rand.Float64() >= h.peerBackfillProbability { //nolint:gosec // G404
		return
	}
	// Same contract as StoreFromPeer: the object is authoritative —
	// the owner already applied the route's freshness policy — so it
	// is stored without re-evaluating cache headers. The ownership
	// gate is bypassed deliberately: backfill is the knob that relaxes
	// the strong-mode owner-only partition.
	ri := RequestInfo{Method: fasthttp.MethodGet, Header: obj.Header.Clone()}
	h.storeObjectLocal(ctx, lookupKey, obj, ri, false, 0)
}

// FetchOrigin drives this route's origin fetch on behalf of a cluster
// peer (the owner side of the origin shield). The originReq is the
// rebuilt upstream request from the waiter's OriginRequest envelope.
// The fetch joins the route's foreground inflight latch — the SAME
// structure a local client miss uses in fetchAndStore — so peer
// waiters plus this node's own client requests collapse into exactly
// one origin fetch. Latching on a separate singleflight here would let
// a local miss and a coalesced RPC race into two origin fetches for
// the same cold key. The resulting object is stored locally (the owner
// always keeps its own fill) and returned to the peer encoded.
//
// Errors mean the flight failed (origin transport error, shed,
// unshareable stream): the peer falls back to origin. An origin error
// STATUS is not an error — it is returned as an authoritative answer
// so negative caching survives.
func (h *Handler) FetchOrigin(ctx context.Context, key api.Key, originReq *fasthttp.Request) (*api.Object, error) {
	method := string(originReq.Header.Method())
	if method != fasthttp.MethodGet && method != fasthttp.MethodHead {
		return nil, errCoalesceMethod
	}
	// HEAD→GET, same canonicalization as the local miss path: a HEAD
	// origin fetch yields an empty body that must never be stored under
	// the (HEAD→GET-canonicalized) cache key. The waiter's serveObject
	// suppresses the body for HEAD clients, so the waiter-side answer
	// is unchanged.
	if method == fasthttp.MethodHead {
		method = fasthttp.MethodGet
		originReq.Header.SetMethod(fasthttp.MethodGet)
	}
	// Route the fetch through the owning route's own origin-bound URI
	// rewrite (strip_prefix / path_rewrite). Request-header rewrites
	// need no re-application: the requester built the envelope from
	// its already-rewritten request.
	originURI := h.originURI(originReq.RequestURI())
	originReq.SetRequestURIBytes(originURI)

	ri := RequestInfo{
		Method: method,
		URI:    string(originReq.RequestURI()),
		Host:   string(originReq.Host()),
		Path:   string(originReq.URI().Path()),
		Header: headerFromFastHTTPReqHeader(&originReq.Header),
	}

	// Join-or-lead the foreground inflight latch, exactly as a local
	// client miss does in fetchAndStore. Followers of a client-led
	// leader receive the leader's buffered result (already stored);
	// ErrStreamUnshareable, shed, and transport failure surface as the
	// follower error, sending the waiter back to origin.
	inflight := &inflightStream{done: make(chan struct{})}
	if actual, loaded := h.inflightStreams.loadOrStore(key, inflight); loaded {
		<-actual.done
		res := actual.res
		if res.Err != nil {
			return nil, res.Err
		}
		// buildObject mutates the header map (attribution headers) and
		// the published result is shared with every follower: clone
		// before building (same discipline as collapsedFetch).
		res.Header = res.Header.ownedClone()
		return buildObject(key, ri, res, res.Header.ToMap(), h.neg, h.defaultTTL,
			h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.policy, time.Now()), nil
	}
	// Leader: publish the buffered result for followers (client or
	// peer) BEFORE returning, so a late arrival finds the object in
	// the store instead of starting a second flight.
	defer h.inflightStreams.delete(key)
	res := h.doFetchBg(ctx, originReq)
	inflight.res = res
	close(inflight.done)
	if res.Err != nil {
		return nil, res.Err
	}
	// Clone before buildObject: the published inflight.res is shared
	// with followers, and buildObject mutates the header map.
	res.Header = res.Header.ownedClone()
	obj := buildObject(key, ri, res, res.Header.ToMap(), h.neg, h.defaultTTL,
		h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.policy, time.Now())
	// Same storage admission as the local fill path: an oversized body
	// is served to the waiting peer but never enters the owner's store.
	if h.maxObjectSize > 0 && int64(len(obj.Body)) > h.maxObjectSize {
		return obj, nil
	}
	h.storeObjectLocal(ctx, key, obj, ri, false, 0)
	return obj, nil
}
