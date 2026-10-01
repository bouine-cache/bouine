package cache

import (
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
