package cache

import (
	"github.com/bouine-cache/bouine/pkg/header"
)

// collapseDenied reports whether a request may not share an in-flight
// origin fetch with any other request. It is true exactly when the
// request carries an Authorization header.
//
// Storage of a response to an authorized request is gated by
// RFC 9111 §3.5 — a shared cache stores it only when the response is
// explicitly shareable (public, must-revalidate, or s-maxage). Request
// collapsing is not storage: the leader's in-flight response is handed
// to every follower that parks on the same flight key, with no such
// gate. Two authorized callers received each other's responses.
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
	return ri.Header.Get(header.Authorization) != ""
}
