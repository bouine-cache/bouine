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
	"strings"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// shieldApplies: hard GET/HEAD miss only, shield wired on, and no
// stale-usable object (a stale object keeps plain peer-fetch +
// stayin-alive semantics — coordinated revalidation is out of scope).
func (h *Handler) shieldApplies(obj *api.Object, method []byte) bool {
	if h.shieldForward == nil || obj != nil {
		return false
	}
	return string(method) == fasthttp.MethodGet || string(method) == fasthttp.MethodHead
}

// shieldForwardRequest copies the ORIGINAL client request verbatim
// (D1) — route rewrites are NOT applied; the owner's route applies
// them once. Hop-by-hop headers are stripped (RFC 9110 §7.6.1,
// including the Connection token list and Proxy-*): they describe the
// client's connection to THIS node, and a copied "Connection: close"
// would degrade the peer connection the forward rides. The deadline is
// the shield bound; the owner clamps it against its own fetch budget
// (D4).
func (h *Handler) shieldForwardRequest(ctx *fasthttp.RequestCtx) (*fasthttp.Request, time.Time) {
	req := fasthttp.AcquireRequest()
	req.Header.SetMethodBytes(ctx.Method())
	req.SetRequestURIBytes(ctx.RequestURI())
	req.Header.SetHostBytes(ctx.Host())
	connList := connectionTokenList(ctx.Request.Header.Peek(header.Connection))
	for k, v := range ctx.Request.Header.All() {
		if shieldHeaderForwardable(k, connList) {
			req.Header.AddBytesKV(k, v)
		}
	}
	return req, time.Now().Add(api.ShieldForwardTimeout)
}

// connectionTokenList parses the Connection header's tokens (the
// headers it names are hop-by-hop too, RFC 9110 §7.6.1).
func connectionTokenList(b []byte) map[string]struct{} {
	if len(b) == 0 {
		return nil
	}
	tokens := map[string]struct{}{}
	for _, f := range bytes.Split(b, []byte(",")) {
		tok := bytes.TrimSpace(f)
		if len(tok) > 0 {
			tokens[string(bytes.ToLower(tok))] = struct{}{}
		}
	}
	return tokens
}

// shieldHeaderForwardable reports whether a client request header may
// ride the shield forward: hop-by-hop headers (RFC 9110 §7.6.1) and
// Proxy-* fields must not (AGENTS §6); everything else forwards.
// Comparison is case-insensitive: fasthttp's canonicalization ("Te")
// differs from the RFC's registered forms.
func shieldHeaderForwardable(key []byte, connList map[string]struct{}) bool {
	lower := string(bytes.ToLower(key))
	switch lower {
	case "connection", "keep-alive", "transfer-encoding",
		"te", "trailer", "upgrade":
		return false
	}
	if _, listed := connList[lower]; listed {
		return false
	}
	return !strings.HasPrefix(lower, "proxy-")
}

// handleShieldMiss runs the shield branch for a hard miss the plain
// peer-fetch could not answer (definitive owner miss, or the peer-fetch
// itself failed — a retry against the same owner's stronger path is
// still worth one RPC): forward the original request to the ring owner
// and relay its answer. Returns true when the response was written and
// the caller must return; false (request never qualified, the forward
// failed, or the owner answered 5xx) falls through to the caller's own
// origin fetch.
//
//nolint:gocyclo // 13: outcome/gate/backfill branches are the ADR-0052 checklist
func (h *Handler) handleShieldMiss(ctx *fasthttp.RequestCtx, owner api.PeerInfo, lookupKey api.Key, obj *api.Object, ri RequestInfo) bool {
	if !h.shieldApplies(obj, ctx.Method()) {
		return false
	}
	fwdReq, deadline := h.shieldForwardRequest(ctx)
	defer fasthttp.ReleaseRequest(fwdReq)

	resp, err := h.shieldForward(context.WithoutCancel(ctx), owner, fwdReq, lookupKey, ctx.IsTLS(), deadline)
	if err != nil || resp == nil {
		h.logger.Debug("shield forward failed, falling back to origin",
			"peer", owner.Addr, "key", lookupKey, "error", err)
		if h.onShieldFallback != nil {
			h.onShieldFallback()
		}
		return false
	}
	defer fasthttp.ReleaseResponse(resp)

	status := resp.StatusCode()
	if status >= fasthttp.StatusInternalServerError {
		// The owner's miss path degraded (shed, origin failure). The
		// requester's own fetch gets a fresh budget — fall back rather
		// than relay a degradation the local fetch may not share.
		h.logger.Debug("shield forward answered 5xx, falling back to origin",
			"peer", owner.Addr, "key", lookupKey, "status", status)
		if h.onShieldFallback != nil {
			h.onShieldFallback()
		}
		return false
	}

	// Relay the owner's answer (200, but also a negative-cached 404, a
	// 304 revalidation, a redirect — anything the owner's standard
	// miss path produced for this exact request); the owner already
	// applied response rewrites — applying them again would be the
	// response-side double-rewrite.
	resp.Header.CopyTo(&ctx.Response.Header)
	if len(resp.Header.Peek(header.Age)) == 0 {
		ctx.Response.Header.SetCanonical(header.S2b(header.Age), header.S2b("0"))
	}
	ctx.SetStatusCode(status)
	// resp is released on return, before the body is served or stored —
	// copy once and serve/store from the owned copy.
	body := append([]byte(nil), resp.Body()...)
	if !bytes.Equal(ctx.Method(), []byte(fasthttp.MethodHead)) {
		ctx.Response.SetBodyRaw(body)
	}
	if h.onShieldSaved != nil {
		h.onShieldSaved()
	}
	h.shieldBackfill(ctx, lookupKey, resp, body, ri)
	return true
}

// shieldBackfill stores the shield-sourced response locally per the
// probability knob (D10: shield fills only). Uses the same buildObject
// as the local miss path, so Vary storage, negative caching and size
// admission behave identically.
func (h *Handler) shieldBackfill(ctx *fasthttp.RequestCtx, lookupKey api.Key, resp *fasthttp.Response, body []byte, ri RequestInfo) {
	if h.shieldBackfillProbability <= 0 {
		return
	}
	if bytes.Equal(ctx.Method(), []byte(fasthttp.MethodHead)) {
		// A HEAD answer carries no body — storing it under the GET
		// canonical key would poison every later GET (the #731 review's
		// HEAD blocker). The next GET re-runs the shield as a GET.
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
	// The backfill is the knob that deliberately relaxes the
	// owner-only partition on the non-owner (D10).
	h.storeObjectLocal(ctx, storeKey, obj, ri, false, 0)
}
