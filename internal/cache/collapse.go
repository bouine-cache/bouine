package cache

import (
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// collapseDenied reports whether a request may not share an in-flight
// origin fetch with any other request. It is true exactly when the
// request carries an Authorization or a Cookie header, or when its
// method is unsafe (anything but GET, HEAD, or OPTIONS).
//
// Storage of a response to an authorized request is gated by
// RFC 9111 §3.5 — a shared cache stores it only when the response is
// explicitly shareable (public, must-revalidate, or s-maxage). Request
// collapsing is not storage: the leader's in-flight response is handed
// to every follower that parks on the same flight key, with no such
// gate. Two authorized callers received each other's responses.
//
// The same argument holds, a fortiori, for unsafe methods (POST, PUT,
// DELETE, and anything beyond the safe set). They never reach the
// collapsing paths today — ServeRequest dispatches them to
// invalidateAndProxy, which fetches directly — but that guarantee is
// purely structural: the dispatcher lives one refactor away from
// leaking a POST into the miss pipeline, and the flight key is the
// cache key (method included), so two identical POSTs would merge
// onto one origin mutation with the loser's body silently dropped.
// The gate makes the invariant local: a mutation is never parked on
// someone else's flight, whatever the dispatcher does.
//
// The same argument covers cookies (ADR-0054): an SSR origin renders
// per-user content from the request's session cookie, so a follower
// parked on another user's fetch receives that user's body in-flight —
// the Set-Cookie storage block never fires for origins that only read
// the cookie. Unlike bypass_on_cookie, this refuses the *sharing*
// without refusing the *cache*: cookied requests keep participating in
// the cache per RFC 9111 (a stored anonymous response may still be
// served to them), so the cache-tests other-cookie optimal case keeps
// passing — it is a served-from-store assertion, not a collapse
// assertion, and the two requests in that test are sequential, never
// concurrent. The cost mirrors ADR-0052: concurrent cookied misses on
// one URL pay one origin fetch each, bounded by the fetch semaphore and
// shed machinery.
//
// The strict rule is chosen over an identity-equality partition
// (hashing the credential into the flight key) deliberately:
// "same Authorization string ⇒ interchangeable callers" is an
// assumption a shared cache cannot verify. A service-scoped credential
// impersonating per-tenant callers (the production incident shape:
// identical JWT, tenant selected by an undeclared custom header) is
// indistinguishable from a genuinely shared credential at the flight
// key. Refusing to collapse every authorized flight costs one origin
// fetch per concurrent authorized caller — bounded by the fetch
// semaphore and shed machinery — and cannot leak. The flip side is
// documented in ADR-0052: same-credential fan-outs no longer dedup.
//
// include_headers are NOT part of this decision: they already govern
// storage variants (ADR-0046), and declaring an identity dimension
// does not make an authorized response shareable in-flight. Anonymous
// traffic on include_headers routes keeps collapsing unchanged.
func collapseDenied(ri RequestInfo) bool {
	// Method first: a mutation never parks on another caller's flight,
	// whatever the dispatcher did with it. The safe set is isInvalidating's
	// — GET and HEAD share flight space by design (BuildKey folds HEAD
	// into GET); OPTIONS is safe and stateless. The method is checked in
	// whichever form RequestInfo carries it: GetMethod() would materialize
	// a string from methodBytes on the miss path and break the alloc
	// budget. A RequestInfo with no method at all (zero value, tests)
	// has no mutation semantics to protect and stays eligible.
	if ri.Method != "" {
		if isInvalidating(ri.Method) {
			return true
		}
	} else if len(ri.methodBytes) > 0 && isInvalidatingBytes(ri.methodBytes) {
		return true
	}
	return ri.Header.Get(header.Authorization) != "" ||
		ri.Header.Get(header.Cookie) != ""
}

// collapseFlightKey returns the key a request parks its origin fetch
// under; api.Key{} (zero value) means the request must fetch on its own.
// A flight may be shared only when its key encodes every dimension the
// response varies on (ADR-0057):
//
//   - STORED OBJECT (warm or variant-miss flight): the lookup key is the
//     flight key. A stored object — with or without Vary — is the
//     origin's own declaration for this URL (RFC 9110 §12.5.5); a
//     variant key with no object was derived from a stored VaryValue
//     and encodes the same surface.
//
//   - DECLARED DIMENSIONS (cold flight, no stored object): the primary
//     key carries only scheme|host|path|query|method — not the
//     include_headers dimensions, which govern stored variants only
//     (ADR-0046). The flight key extends it with the declared headers,
//     hashed exactly as the storage variant key hashes them
//     (variantKeyCore/varyHeaderValue), so a flight collapses two
//     requests only when their stored variants would be identical.
//
//   - NO DECLARED DIMENSIONS (cold flight, include-free route):
//     refused — the origin's Vary is unknowable before the first
//     response arrives, so no shared key can be proven safe. The cost
//     is one origin fetch per concurrent caller for the first burst
//     per key; the next burst is warm and collapses.
//
// The gate shares only what a declaration proves shareable, and refuses
// when nothing does. Declared headers are hashed only into the FLIGHT
// key; the request's own headers travel to the origin untouched, and
// the leader's stored variants are keyed by the storing caller's own
// headers (ADR-0046), so a follower's later HITs come from its own
// variant. ri.Header is an owned snapshot on every collapsing path, so
// reading it here is safe.
func collapseFlightKey(h *Handler, primaryKey, lookupKey api.Key, obj *api.Object, ri RequestInfo) api.Key {
	if obj != nil || primaryKey != lookupKey {
		// Warm or variant-miss flight: the lookup key already encodes
		// the origin's declared surface.
		return lookupKey
	}
	if !h.policy.hasFlightDimensions() {
		// Cold flight, no declaration, no stored evidence: refuse.
		return api.Key{}
	}
	// Cold flight on a declared-dimensions route.
	vk := variantKeyCore(primaryKey, h.policy.flightVary(), mapVarySrc{flightHeaders(h, ri)}, h.policy)
	if vk == primaryKey {
		// Declared list empty or fully excluded: the primary key is
		// already a faithful flight identity.
		return primaryKey
	}
	return vk
}

// flightHeaders snapshots the declared include_headers off the request
// into a header.Map for the flight-key hash — plus the Cookie header
// when the route declares cookie presence, so variantKeyCore's synthetic
// cookie-presence field can compute the presence bits through the same
// path the storage variant key uses (policy.cookiePresenceValue over
// the Cookie value). A fresh map (not an alias of ri.Header) because
// mapVarySrc.getValue runs while the flight's other callers may still be
// mutating their own maps. The map carries exactly the declared
// dimensions' inputs, nothing else.
func flightHeaders(h *Handler, ri RequestInfo) header.Map {
	policy := h.policy
	m := header.NewMap(len(policy.includeHeaders) + 1)
	for _, f := range policy.includeHeaders {
		m.Set(f, ri.Header.Get(f))
	}
	if policy.hasCookiePresence() {
		m.Set(header.Cookie, ri.Header.CookieAll())
	}
	return m
}
