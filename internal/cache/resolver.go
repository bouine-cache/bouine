package cache

import (
	"context"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

// buildResolver derives the primary-key Vary resolver from the freshly
// stored variant (ADR-0065): the same headers and validators, no body.
// The resolver exists so lookup() finds the stored Vary list to select a
// variant and so revalidateResolverMiss can serve after a validating 304
// — and the bytes it serves are the mirror variant's, fetched at serve
// time. Storing a second private copy of the body made every Vary page
// cost 2x its body in hot-tier RAM (#824); the resolver now accounts
// only headers and metadata.
//
// VaryKey is blanked: by protocol the primary-key entry is never a
// variant. The peer-fetch and fast-path gates classify resolvers by the
// blank VaryKey — a peer whose only entry for a key is the resolver
// answers a variant fetch with a miss instead of a foreign variant's
// body (peerfetch.go), and the fast path treats the entry as a miss
// (fastpath.go). The streaming fill path previously skipped the
// blanking, so streamed resolvers carried the variant's vary-key hash
// and leaked through those gates; funneling every fill through this
// constructor fixes the drift on all three sites at once.
//
// PairedKey names the variant whose body the resolver mirrors. The pair
// is coherent by construction: the resolver is created only alongside
// the variant's store, and every later full-200 re-fill overwrites both
// — a 304 freshening never changes a body.
//
// Headers are shared with the variant here and deep-cloned per Put by
// the hot store (TestHotStore_PutOwnsCallerBody); serveResolver304 sets
// Content-Length from the mirror bytes it actually serves, so the
// resolver never emits representation metadata it cannot back.
func buildResolver(variant *api.Object, primaryKey api.Key) *api.Object {
	resolver := variant.CloneForReturn(nil)
	resolver.Key = primaryKey
	resolver.VaryKey = ""
	resolver.Body = nil
	resolver.BodySize = 0
	resolver.PairedKey = variant.Key
	return resolver
}

// shouldRegisterRefresh reports whether obj joins the proactive refresh
// registry. Vary resolvers never do (ADR-0065): a background refresh of
// the primary key would re-store a full body there, resurrecting the
// duplicate #824 removed, and the mirror variant refreshes on its own
// cycle anyway. The resolver's validators therefore age with their
// store; a stale ETag still conditions the revalidateResolverMiss
// fetch, and a rotated representation answers full-200 → ordinary
// fill. Resolver identity is the protocol invariant (blank VaryKey
// with a Vary list), not PairedKey: variants carry PairedKey too (the
// primary they belong to), so the field cannot discriminate.
func (h *Handler) shouldRegisterRefresh(obj *api.Object) bool {
	return h.refreshBeforeExpiry && obj.TTL >= minRefreshTTL && !obj.IsVaryResolver()
}

// isPeerResolverMetadata reports whether a peer-fetched object is the
// metadata-only Vary resolver (ADR-0065): the answer an owner gives to
// a bare primary-key ask so the requester can derive the Vary list it
// could not compute locally. Legacy resolvers (pre-ADR blobs carrying
// their own body) are never answered over the wire — the peer serve
// side withholds them outright.
func isPeerResolverMetadata(obj *api.Object) bool {
	return obj.IsVaryResolver() && len(obj.Body) == 0
}

// serveResolverPeerVariant completes the two-step variant resolution for
// a node with no local Vary context (ADR-0065). Step one fetched the
// metadata-only resolver from the primary key's owner; this step derives
// the variant key for THIS request's selecting headers and asks the
// variant's owner with a real vary-key assertion. Both gates still
// apply — the owner-side variant gate on the answer, servePeerHit's
// response-side gate and freshness evaluation — and any miss or
// rejection falls back to the caller's ordinary origin fill: the
// resolver metadata is an optimization, never a requirement.
//
// On main this convergence path did not exist: streamed resolvers
// carried a non-blank VaryKey (the fill-site drift this change fixes),
// slipped past the owner's resolver-withhold gate, and were served as
// primary-key peer HITS — the first variant's body to every requester.
// Closing the leak made the legitimate two-step path mandatory.
func (h *Handler) serveResolverPeerVariant(ctx *fasthttp.RequestCtx, primaryKey api.Key, resolver *api.Object, now time.Time, ri RequestInfo) bool {
	vk := VariantKey(primaryKey, resolver.VaryValue, ri.Header, h.policy)
	if vk == primaryKey {
		// The Vary list collapsed to the primary key: the resolver
		// itself would be this request's entry — metadata-only, so
		// nothing is servable. The ordinary fill re-populates it.
		return false
	}
	assertion := BuildVaryKey(resolver.VaryValue, ri.Header, h.policy)
	if assertion == "" {
		return false
	}
	variant := h.getVariantForResolution(ctx, vk, assertion)
	if variant == nil {
		return false
	}
	// servePeerHit never stores: a remote variant is served as-is, and
	// a local one (this node owns the variant key even though the
	// primary hashes elsewhere) is served without re-storing. Its
	// Source label says "peer" — a cosmetic mislabel for the local
	// case on a cold path.
	return h.servePeerHit(ctx, vk, variant, now, ri)
}

func (h *Handler) getVariantForResolution(ctx context.Context, vk api.Key, assertion string) *api.Object {
	owner, isLocal := h.ownerFn(vk)
	if isLocal {
		variant, _, _ := h.store.Get(ctx, vk)
		return variant
	}
	variant, err := h.peerFetch(ctx, owner, vk, assertion)
	if err != nil {
		return nil
	}
	return variant
}
