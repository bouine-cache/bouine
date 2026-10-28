package cache

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

// buildResolver derives the primary-key Vary resolver from the freshly
// stored variant (ADR-0068): the same headers and validators, no body.
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
// registry. Vary resolvers never do (ADR-0068): a background refresh of
// the primary key would re-store a full body there, resurrecting the
// duplicate #824 removed, and the mirror variant refreshes on its own
// cycle anyway. The resolver's validators therefore age with their
// store; a stale ETag still conditions the revalidateResolverMiss
// fetch, and a rotated representation answers full-200 → ordinary
// fill. Resolver identity is the protocol invariant (blank VaryKey
// with a Vary list), not PairedKey: variants carry PairedKey too (the
// primary they belong to), so the field cannot discriminate.
//
// A request without a URI never registers either: the refresh executor
// rebuilds its fetch from the registry entry's url (refresh_registry
// stores ri.GetURI()), and an empty url schedules a fetch that fails
// locally on every attempt and retries until the TTL lapses — a
// scheduled failure loop, never a refresh. The one URI-less site is
// the peer-put callback (StoreFromPeer): the write-to-owner RPC
// carries an api.Object and the object codec has no URL field, so the
// owner cannot reconstruct the fetch. Peer-put objects keep
// request-driven revalidation (SWR/SIE/stale-on-hit), the strong-mode
// refresh path (issue #509).
func (h *Handler) shouldRegisterRefresh(obj *api.Object, ri RequestInfo) bool {
	return h.refreshBeforeExpiry && obj.TTL >= minRefreshTTL && !obj.IsVaryResolver() &&
		ri.GetURI() != ""
}

// isPeerResolverMetadata reports whether a peer-fetched object is the
// metadata-only Vary resolver (ADR-0068): the answer an owner gives to
// a bare primary-key ask so the requester can derive the Vary list it
// could not compute locally. Legacy resolvers (pre-ADR blobs carrying
// their own body) are never answered over the wire — the peer serve
// side withholds them outright.
func isPeerResolverMetadata(obj *api.Object) bool {
	return obj.IsVaryResolver() && len(obj.Body) == 0
}

// transferredResolver returns the metadata-only Vary resolver the H1
// fast path already fetched for THIS request (ADR-0068), or nil when
// the slow path must ask the owner itself. The fast path sets
// RawRequest.OwnerResolver when the owner answers its bare primary-key
// ask with resolver metadata; the h1parser transfers it to the
// fallback RequestCtx under api.OwnerResolverContextKey. Reuse is
// bounded by the same identity proof the boolean owner hints cannot
// carry: the object's own Key must equal the lookup key this handler
// would have asked for (on a variant miss the slow path asks the
// VARIANT key — a different question — and the mismatch rejects the
// transfer), and obj must be nil: with a stored object the slow path
// asks with peerVaryAssertion(obj), again a different question. No
// nil-policy restriction: the Key comparison proves the question
// identical regardless of the route's policy.
func transferredResolver(ctx *fasthttp.RequestCtx, obj *api.Object, lookupKey api.Key) *api.Object {
	if obj != nil {
		return nil
	}
	xfer, ok := ctx.UserValue(api.OwnerResolverContextKey).(*api.Object)
	if !ok || xfer == nil || !isPeerResolverMetadata(xfer) || xfer.Key != lookupKey {
		return nil
	}
	return xfer
}

// configDerivedVary returns the Vary list a route's KeyPolicy alone
// determines: the union of the key's include_headers allow-list and the
// synthetic cookie-presence field, normalized exactly as effectiveVary
// normalizes the stored union (stream.go) — sorted, deduplicated,
// ", "-joined. Non-empty therefore only when the policy keys variants
// (ADR-0071): on such a route effectiveVary always unions the include
// list in, so every stored entry lives under a derived variant key and
// the primary key only ever holds the metadata-only resolver.
//
// The value equals the stored VaryValue iff the origin declared no Vary
// field beyond the policy's own (a declared field inside the include
// list dedups into the same union). An origin that declares extra
// fields makes the derivation wrong — the derived variant key hashes
// elsewhere — which answers 404 and falls back to the two-step
// resolution (ADR-0068), so wrong derivations cost one extra ask, never
// a wrong body: the variant-assertion gates (peerfetch.go,
// servePeerHit) still bound what is served.
func configDerivedVary(policy *KeyPolicy) string {
	if policy == nil || (len(policy.includeHeaders) == 0 && !policy.hasCookiePresence()) {
		return ""
	}
	// includeHeaders is already trimmed, lowercased, sorted, and
	// deduped by NewKeyPolicy; the sort+dedup below mirrors
	// effectiveVary's union step so the shapes stay byte-identical.
	fields := make([]string, 0, len(policy.includeHeaders)+1)
	fields = append(fields, policy.includeHeaders...)
	if policy.hasCookiePresence() {
		fields = append(fields, cookiePresenceField)
	}
	sort.Strings(fields)
	uniq := fields[:0]
	for i, f := range fields {
		if i == 0 || f != fields[i-1] {
			uniq = append(uniq, f)
		}
	}
	return strings.Join(uniq, ", ")
}

// serveResolverPeerVariant completes the two-step variant resolution for
// a node with no local Vary context (ADR-0068). Step one fetched the
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
	if !h.serveDerivedPeerVariant(ctx, primaryKey, resolver.VaryValue, now, ri) {
		return false
	}
	// Count completions only: a rejection or stale answer is not a
	// converged two-step, and the consumer-side counter exists to
	// prove the fleet's Vary cold misses converge through the owner
	// (zero here means they go to origin instead, ADR-0068).
	if h.onResolverTwoStep != nil {
		h.onResolverTwoStep()
	}
	return true
}

// serveDerivedPeerVariant is the step-two body shared by the wire-resolved
// two-step (serveResolverPeerVariant) and the config-derived single ask
// (ADR-0071, handleCacheMiss): derive the variant key for THIS request's
// selecting headers from varyValue — learned from the resolver or
// computed from the route's KeyPolicy alone — and ask the variant's
// owner with a real vary-key assertion. Never servable metadata: a miss
// or rejection returns false and the caller falls back to its ordinary
// path (origin fill, or the bare ask when the derivation is unproven).
func (h *Handler) serveDerivedPeerVariant(ctx *fasthttp.RequestCtx, primaryKey api.Key, varyValue string, now time.Time, ri RequestInfo) bool {
	vk := VariantKey(primaryKey, varyValue, ri.Header, h.policy)
	if vk == primaryKey {
		// The Vary list collapsed to the primary key: the resolver
		// itself would be this request's entry — metadata-only, so
		// nothing is servable. The ordinary fill re-populates it.
		return false
	}
	assertion := BuildVaryKey(varyValue, ri.Header, h.policy)
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
