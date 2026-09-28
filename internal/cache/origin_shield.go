// Package cache: origin shielding (requester side). On a hard GET/HEAD
// miss where the plain peer-fetch answered 404, the non-owner forwards
// the original client request to the ring owner, which runs its own
// standard miss path and proxies the response back — one origin fetch
// for the whole cluster (ADR-0052). The owner side lives in
// internal/cluster/shield.go.
package cache

import (
	"bytes"
	"context"
	"math/rand/v2"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// shieldApplies reports whether the shield branch runs for this miss:
// hard GET/HEAD miss only (D8), shield wired on (D9), and no
// stale-usable object in scope (a stale object keeps plain
// peer-fetch + stayin-alive semantics; the shield deliberately does
// not do coordinated revalidation, plan §4 "not in scope").
func (h *Handler) shieldApplies(obj *api.Object, method []byte) bool {
	if h.shieldForward == nil || obj != nil {
		return false
	}
	return string(method) == fasthttp.MethodGet || string(method) == fasthttp.MethodHead
}

// shieldOutcome reports how the shield forward ended for a hard miss.
// The distinction mirrors #731's tri-state: returning without serving
// means either the forward ran and failed (the plain peer-fetch's
// answer would come from the same owner that just failed — do not
// re-ask) or the request never qualified.
type shieldOutcome int

const (
	// shieldNotApplied: the request did not qualify — the caller keeps
	// today's behavior untouched (plain peer-fetch already ran).
	shieldNotApplied shieldOutcome = iota
	// shieldAttempted: the forward ran and produced no servable answer
	// — the origin fetch below follows; re-asking the owner is useless.
	shieldAttempted
	// shieldServed: the owner's answer was written to the client; the
	// caller returns immediately.
	shieldServed
)

// shieldForwardRequest builds the wire form of the ORIGINAL client
// request (D1): method, full request URI, host, and headers copied
// verbatim from the live ctx. Route rewrites are NOT applied — the
// owner's route applies them itself, exactly once (the double-rewrite
// risk in ADR-0052 is why the forward must carry the un-rewritten
// bytes). The deadline is min(remaining client-relevant budget, the
// shield bound) — the owner clamps it again against its own fetch
// budget (D4).
func (h *Handler) shieldForwardRequest(ctx *fasthttp.RequestCtx) (*fasthttp.Request, time.Time, func()) {
	req := fasthttp.AcquireRequest()
	req.Header.SetMethodBytes(ctx.Method())
	req.SetRequestURIBytes(ctx.RequestURI())
	req.Header.SetHostBytes(ctx.Host())
	for k, v := range ctx.Request.Header.All() {
		req.Header.AddBytesKV(k, v)
	}
	// A fasthttp RequestCtx has no Done channel: WithoutCancel avoids
	// the WithTimeout panic on a ctx that implements context.Context.
	// Client cancellation is not propagated to the wait; the deadline
	// bounds it, and the owner's flight is the owner's to finish (its
	// fill lands in its store either way).
	waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), api.ShieldForwardTimeout)
	_ = waitCtx // the RPC budget is the deadline below; ctx cancels nothing extra
	return req, time.Now().Add(api.ShieldForwardTimeout), cancel
}

// handleShieldMiss runs the shield branch for a hard miss the plain
// peer-fetch could not answer. shieldServed means the response was
// written and the caller must return; shieldAttempted means the
// forward ran and failed — the origin fetch below follows, and the
// plain peer-fetch must NOT be retried (the same owner just answered
// 404 or errored); shieldNotApplied means the request never qualified.
//
//nolint:gocyclo // 13: outcome/gate/backfill branches are the ADR-0052 checklist
func (h *Handler) handleShieldMiss(ctx *fasthttp.RequestCtx, owner api.PeerInfo, lookupKey api.Key, obj *api.Object, ri RequestInfo) shieldOutcome {
	if !h.shieldApplies(obj, ctx.Method()) {
		return shieldNotApplied
	}
	fwdReq, deadline, cancel := h.shieldForwardRequest(ctx)
	defer cancel()
	defer fasthttp.ReleaseRequest(fwdReq)

	resp, err := h.shieldForward(context.WithoutCancel(ctx), owner, fwdReq, lookupKey, ctx.IsTLS(), deadline)
	if err != nil || resp == nil {
		h.logger.Debug("shield forward failed, falling back to origin",
			"peer", owner.Addr, "key", lookupKey, "error", err)
		if h.onShieldFallback != nil {
			h.onShieldFallback()
		}
		return shieldAttempted
	}
	defer fasthttp.ReleaseResponse(resp)

	if resp.StatusCode() != fasthttp.StatusOK {
		// The owner's own miss path answered an error status (503 shed,
		// 502 origin failure mapped by its router). It is an
		// authoritative answer about the OWNER's fetch attempt, but the
		// requester still has its own origin budget — fall back rather
		// than relay a degradation the local fetch may not share.
		h.logger.Debug("shield forward answered non-OK, falling back to origin",
			"peer", owner.Addr, "key", lookupKey, "status", resp.StatusCode())
		if h.onShieldFallback != nil {
			h.onShieldFallback()
		}
		return shieldAttempted
	}

	// Serve the proxied bytes. The owner already applied its route's
	// response rewrites; applying them again here would be the
	// response-side double-rewrite, so none run.
	resp.Header.CopyTo(&ctx.Response.Header)
	if len(resp.Header.Peek(header.Age)) == 0 {
		ctx.Response.Header.SetCanonical(header.S2b(header.Age), header.S2b("0"))
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
	// resp is pooled and released by the deferred ReleaseResponse when
	// this miss returns — before fasthttp writes ctx.Response to the
	// socket, and before the backfilled object is ever served. Copy the
	// body once and serve/store from the owned copy (buildObject's
	// body-ownership contract, handler.go).
	body := append([]byte(nil), resp.Body()...)
	if !bytes.Equal(ctx.Method(), []byte(fasthttp.MethodHead)) {
		ctx.Response.SetBodyRaw(body)
	}
	if h.onShieldSaved != nil {
		h.onShieldSaved()
	}
	h.shieldBackfill(ctx, lookupKey, resp, body, ri)
	return shieldServed
}

// shieldBackfill stores the shield-sourced response locally per the
// probability knob (D10: shield fills only). The object is built by
// the same buildObject the local miss path uses, so Vary storage,
// negative caching and size admission behave identically.
func (h *Handler) shieldBackfill(ctx *fasthttp.RequestCtx, lookupKey api.Key, resp *fasthttp.Response, body []byte, ri RequestInfo) {
	if h.shieldBackfillProbability <= 0 {
		return
	}
	if bytes.Equal(ctx.Method(), []byte(fasthttp.MethodHead)) {
		// A HEAD answer carries no body by protocol (the owner's
		// serveObject suppresses it) — storing it under the GET
		// canonical key would poison every later GET with an empty
		// object (PR #731 review blocker #1). The next GET re-runs the
		// shield with a GET replay; the owner's own GET-canonical fill
		// serves it.
		return
	}
	// Same admission as the local fill path: oversized bodies are
	// served but never stored.
	if h.maxObjectSize > 0 && int64(len(body)) > h.maxObjectSize {
		return
	}
	if h.shieldBackfillProbability < 1 && rand.Float64() >= h.shieldBackfillProbability { //nolint:gosec // G404: probabilistic placement, not a security decision
		return
	}
	hdrMap := header.FromFastHTTP(&resp.Header)
	res := fetchResult{
		StatusCode: resp.StatusCode(),
		Header:     fromHeaderMap(hdrMap),
		Body:       body,
	}
	resMap := hdrMap
	primaryKey := lookupKey
	storeKey := lookupKey
	if vary := effectiveVary(resMap, h.policy); vary != "" {
		storeKey = VariantKey(primaryKey, vary, ri.Header, h.policy)
	}
	if storeKey != primaryKey {
		if !h.reserveVariantSlot(ctx, primaryKey, storeKey) {
			return
		}
	}
	obj := buildObject(storeKey, ri, res, resMap, h.neg, h.defaultTTL, h.overrideTTL, h.defaultSWR, h.defaultSIE, h.jitterPercent, h.policy, h.stayinAlive, h.poolName, time.Now())
	// The owner already stored its fill under the ownership rules; the
	// backfill is the knob that deliberately relaxes the partition on
	// the non-owner (D10), so it bypasses storeObject's owner gate.
	h.storeObjectLocal(ctx, storeKey, obj, ri, false, 0)
}
