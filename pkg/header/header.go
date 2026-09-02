// Package header defines the canonical string constants for every HTTP
// header name used by bouine. Centralising these in one place eliminates
// typo-prone string literals scattered across packages and makes header
// usage greppable.
//
// All constants use the canonical HTTP header name casing as registered
// in the IANA Message Headers registry. Go's [net/http] canonicalises
// header names on access (Get/Set/Add/Del), so comparisons are
// case-insensitive at runtime — but using these constants ensures
// consistent spelling in logs, metrics, and documentation.
//
// The package is a leaf: it imports nothing from internal/ and may be
// imported by any layer.
package header

// Standard HTTP request and response headers defined in RFC 9110
// (HTTP Semantics) and related specifications.
const (
	// Accept — RFC 9110 §12.5.1. Media types acceptable in the response.
	Accept = "Accept"

	// AcceptEncoding — RFC 9110 §12.5.3. Acceptable content encodings.
	AcceptEncoding = "Accept-Encoding"

	// AcceptLanguage — RFC 9110 §12.5.4. Preferred natural languages.
	AcceptLanguage = "Accept-Language"

	// Authorization — RFC 9110 §11.6.2. Credentials for HTTP authentication.
	Authorization = "Authorization"

	// ContentEncoding — RFC 9110 §8.4. Content codings applied to the
	// representation body.
	ContentEncoding = "Content-Encoding"

	// ContentLength — RFC 9110 §8.2. Length of the representation body
	// in octets.
	ContentLength = "Content-Length"

	// ContentLocation — RFC 9110 §8.5. URI reference for the resource
	// enclosed in the message.
	ContentLocation = "Content-Location"

	// ContentRange — RFC 9110 §14.4. Byte range of the representation
	// returned in a 206 Partial Content response.
	ContentRange = "Content-Range"

	// ContentType — RFC 9110 §8.3. Media type of the representation.
	ContentType = "Content-Type"

	// Date — RFC 9110 §6.6.1. Date and time at which the message
	// originated.
	Date = "Date"

	// ETag — RFC 9110 §8.8.3. Identifier for a specific version of a
	// resource. Stored in canonical form ("Etag") to match Go's
	// textproto.CanonicalMIMEHeaderKey so it can be used directly as an
	// http.Header map key without .Set()/.Get() canonicalization.
	ETag = "Etag"

	// Expires — RFC 9110 §8.7.2 / RFC 9111 §4.2.1. Date/time after which
	// the response is considered stale.
	Expires = "Expires"

	// Host — RFC 9110 §7.2. Target URI host and port (also sent as a
	// header in HTTP/1.1).
	Host = "Host"

	// IfModifiedSince — RFC 9110 §13.1.3. Conditional request: 304 if
	// not modified since the given date.
	IfModifiedSince = "If-Modified-Since"

	// IfNoneMatch — RFC 9110 §13.1.2. Conditional request: 304 if the
	// current representation matches the given ETag.
	IfNoneMatch = "If-None-Match"

	// LastModified — RFC 9110 §8.8.2. Date and time the representation
	// was last modified.
	LastModified = "Last-Modified"

	// Location — RFC 9110 §10.2.2. Target resource URI for a redirect.
	Location = "Location"

	// Pragma — RFC 9110 §15.2. Obsolete implementation-defined directives
	// (still used for "no-cache" in legacy clients).
	Pragma = "Pragma"

	// Range — RFC 9110 §14.2. Request a range of the representation.
	Range = "Range"

	// RetryAfter — RFC 9110 §10.2.3. Time to wait before retrying a
	// request.
	RetryAfter = "Retry-After"

	// SetCookie — RFC 6265 §4.1. Response cookie. May appear multiple
	// times.
	SetCookie = "Set-Cookie"

	// Warning — RFC 9110 §15.8 (obsolete). Additional information about
	// the status of a message.
	Warning = "Warning"

	// WWWAuthenticate — RFC 9110 §11.6.1. Challenge for HTTP authentication
	// in a 401 response.
	WWWAuthenticate = "WWW-Authenticate"
)

// Cache-specific headers defined in RFC 9111 (HTTP Caching) and related
// specifications.
const (
	// Age — RFC 9111 §4.2.3. The age of the response in seconds
	// (time since origin server generated it).
	Age = "Age"

	// CacheControl — RFC 9110 §5.2 / RFC 9111 §4.2.1. Directives for
	// caches along the request/response path.
	CacheControl = "Cache-Control"

	// CDNCacheControl — RFC 9211 §4. CDN-specific cache directives that
	// override Cache-Control for shared caches. The canonical MIME form
	// is "Cdn-Cache-Control" (first letter uppercase, rest lowercase per
	// word); using this constant avoids canonicalize allocations on every
	// header lookup.
	CDNCacheControl = "Cdn-Cache-Control"

	// Vary — RFC 9111 §4.1. Request header fields that a cache must
	// include in the cache key to select the correct variant.
	Vary = "Vary"
)

// Surrogate-key headers used by different CDN dialects for grouped
// invalidation. The first non-empty header wins when parsing.
const (
	// SurrogateKey — Fastly dialect for grouped invalidation labels.
	SurrogateKey = "Surrogate-Key"

	// CacheTag — Cloudflare dialect for grouped invalidation labels.
	CacheTag = "Cache-Tag"

	// XCacheTags — Varnish dialect for grouped invalidation labels.
	XCacheTags = "X-Cache-Tags"
)

// SurrogateKeyHeaders lists the response header names that different CDN
// dialects use to carry surrogate keys (Fastly: Surrogate-Key, Cloudflare:
// Cache-Tag, Varnish: X-Cache-Tags). The first non-empty header wins.
var SurrogateKeyHeaders = []string{
	SurrogateKey,
	CacheTag,
	XCacheTags,
}

// Hop-by-hop headers defined in RFC 9110 §7.6.1. These headers must be
// stripped when forwarding messages through a proxy because they apply
// only to a single transport-level connection.
const (
	// Connection — RFC 9110 §7.6.1. Controls hop-by-hop options and
	// lists headers to remove when forwarding.
	Connection = "Connection"

	// KeepAlive — Obsolete hop-by-hop header for persistent connections.
	KeepAlive = "Keep-Alive"

	// TE — RFC 9110 §7.1.4. Transfer codings acceptable in the request.
	TE = "TE"

	// Trailer — RFC 9110 §6.6.2. Header fields present in the trailer
	// of a chunked message.
	Trailer = "Trailer"

	// TransferEncoding — RFC 9110 §6.2.2. How the message body is
	// encoded for transfer (e.g. "chunked").
	TransferEncoding = "Transfer-Encoding"

	// Upgrade — RFC 9110 §7.8. Request to upgrade to a different
	// protocol on the same connection.
	Upgrade = "Upgrade"
)

// Bouine-specific custom headers. These carry internal metadata between
// bouine nodes or are set on responses to expose cache state to clients.
const (
	// XCache — bouine's cache result header set on every served
	// response: "HIT", "MISS", "STALE", "BYPASS", or "REVALIDATED".
	XCache = "X-Cache"

	// XCacheSource — bouine's cache source header set on every served
	// response: "hot", "warm", "peer", "origin", or empty for non-origin
	// paths (BYPASS, only-if-cached 504). Split from X-Cache so operators
	// can distinguish where a HIT was served from without scraping the
	// storage layer.
	XCacheSource = "X-Cache-Source"

	// XBouineHost — stored on cached objects to record the request Host
	// at storage time. Used by ban/purge predicates and the dashboard.
	XBouineHost = "X-Bouine-Host"

	// XBouinePath — stored on cached objects to record the request URL
	// path at storage time. Used by ban/purge predicates and the
	// dashboard.
	XBouinePath = "X-Bouine-Path"

	// XBouineRoute — dashboard attribution key. The data-plane router
	// sets the matched route's label under this name as a fasthttp
	// UserValue (not a wire header) so the dashboard rings can attribute
	// per-route traffic; an inbound header of this name is stripped as
	// attacker input. Prometheus metrics carry upstream_pool instead:
	// route cardinality scales with the number of proxy rules, while
	// pools are a small config-bounded set.
	XBouineRoute = "X-Bouine-Route"

	// XBouinePool — Prometheus attribution key. The data-plane router
	// sets the serving route's upstream pool under this name as a
	// fasthttp UserValue (not a wire header); the observability
	// middleware uses it as the upstream_pool metric label. Also
	// stripped as attacker input like every X-Bouine-* header.
	XBouinePool = "X-Bouine-Pool"

	// BouineHop — carries the current peer-fetch hop count for cluster
	// loop detection.
	BouineHop = "Bouine-Hop"

	// XBouineClusterVersion — carries the cluster protocol version for
	// negotiation during rolling upgrades.
	XBouineClusterVersion = "X-Bouine-Cluster-Version"

	// BouineIssuer — carries the node name that issued a purge or ban
	// event, sent as an HTTP header on peer POST endpoints.
	BouineIssuer = "Bouine-Issuer"

	// BouineSeq — carries the monotonic sequence number of a purge or
	// ban event, sent as an HTTP header on peer POST endpoints.
	BouineSeq = "Bouine-Seq"

	// BouineIssuedAt — carries the wall-clock timestamp (RFC3339) of a
	// purge or ban event, sent as an HTTP header on peer POST endpoints.
	BouineIssuedAt = "Bouine-Issued-At"

	// BouineMethod — carries the HTTP method of the original cached
	// request, sent as an HTTP header on peer POST endpoints.
	BouineMethod = "Bouine-Method"

	// HXTrigger — htmx trigger header. Set on dashboard responses to
	// tell the client to fire a client-side event (e.g. "refreshOpsLog").
	HXTrigger = "HX-Trigger"
)
