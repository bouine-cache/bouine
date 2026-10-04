package cache

import (
	"github.com/bouine-cache/bouine/pkg/header"
)

// collapseDenied reports whether a request may not share an in-flight
// origin fetch with any other request. It is true when the request
// carries an Authorization or a Cookie header.
//
// Storage of a response to an authorized request is gated by
// RFC 9111 §3.5 — a shared cache stores it only when the response is
// explicitly shareable (public, must-revalidate, or s-maxage). Request
// collapsing is not storage: the leader's in-flight response is handed
// to every follower that parks on the same flight key, with no such
// gate. Two authorized callers received each other's responses.
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
	return ri.Header.Get(header.Authorization) != "" ||
		ri.Header.Get(header.Cookie) != ""
}
