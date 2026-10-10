// forwarded.go implements per-route client-identity header injection
// (request.forwarded, issue #769): X-Forwarded-For (append-only),
// X-Forwarded-Proto, X-Forwarded-Host, and Via (RFC 9110 §7.6.3) on
// origin-bound requests.
//
// Placement contract: injection happens at origin-bound request
// construction only — doFetchFast, revalidate, invalidateAndProxy,
// doFetchStream (miss/bypass/SSE), and the stored-RequestInfo background
// fetchers (doBackgroundRevalidate, doBackgroundRefresh, doShedRefill).
// It never touches the live *fasthttp.RequestCtx headers, so:
//
//   - the cache-hit path is untouched (zero headers added, zero cost —
//     the policy check is a single bool on routes without the block);
//   - the cache key and the Vary variant key cannot see the injected
//     values (threat-model T06: no implicit header keying);
//   - the stored RequestInfo headers carry no per-client data, so a
//     background refresh can never replay one user's identity for
//     another user's request.
//
// Spoofing model (threat-model T04): a client-supplied X-Forwarded-For
// chain is untrusted input. bouine appends the address of its immediate
// peer — the connection it actually received the request on — and never
// parses or acts on the existing entries. X-Forwarded-Proto and
// X-Forwarded-Host are SET (replacing any client-supplied value) because
// bouine is authoritative about what it itself received.

package cache

import (
	"net"
	"strings"

	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// forwardedMaxHeaderBytes is the per-header budget for the injected
// X-Forwarded-For / Via chains (threat-model T37: per-header ≤ 8 KiB).
// The max_append entry cap normally keeps chains far below it; the byte
// cap is the hard backstop against a client sending a single ~8 KiB
// "entry" that would otherwise ride through at cap × 8 KiB.
const forwardedMaxHeaderBytes = 8 << 10

// defaultForwardedMaxAppend mirrors config.DefaultForwardedMaxAppend.
// Duplicated because this package compiles route policies without
// importing the config package; if you change one, change the other.
const defaultForwardedMaxAppend = 5

// viaEntry is what bouine appends to Via (RFC 9110 §7.6.3): the received
// protocol version and this proxy. The fixed pseudonym keeps origins
// from fingerprinting per-node versions; cluster-internal loop detection
// uses Bouine-Hop, not Via.
const viaEntry = "1.1 bouine"

// ForwardedPolicy is the compiled request.forwarded directive
// (config.ForwardedConfig). The zero value disables injection; the
// enabled check is a single bool read on routes without the block.
// Unstable.
type ForwardedPolicy struct {
	// ClientIP appends the immediate peer address to X-Forwarded-For.
	ClientIP bool
	// Proto sets X-Forwarded-Proto from the receiving listener.
	Proto bool
	// Host sets X-Forwarded-Host to the received Host header.
	Host bool
	// Via appends "1.1 bouine" to Via (RFC 9110 §7.6.3).
	Via bool
	// MaxAppend caps the XFF/Via chain length; DefaultForwardedMaxAppend
	// semantics are applied by the caller (config validation or the
	// builder) so this is never 0 when any flag is set.
	MaxAppend int
}

// Enabled reports whether any forwarded header is configured. Kept as a
// method (not a field) so the disable check cannot drift from the flag
// set.
func (p ForwardedPolicy) Enabled() bool {
	return p.ClientIP || p.Proto || p.Host || p.Via
}

// forwardedProto is the pre-materialised X-Forwarded-Proto values; byte
// slices so the hot injection avoids string→[]byte per call.
var (
	protoHTTP  = []byte("http")
	protoHTTPS = []byte("https")
)

// applyForwardedCtx injects the configured client-identity headers onto
// an origin-bound request built from a live *fasthttp.RequestCtx. Called
// only on miss/bypass/revalidate/streaming paths, never the hit path.
// No-op on routes without request.forwarded.
func (h *Handler) applyForwardedCtx(dst *fasthttp.RequestHeader, ctx *fasthttp.RequestCtx) {
	if !h.forwarded.Enabled() {
		return
	}
	if h.forwarded.ClientIP {
		appendForwardedValue(dst, header.XForwardedFor, peerIP(ctx.RemoteAddr()), h.forwarded.MaxAppend)
	}
	if h.forwarded.Proto {
		v := protoHTTP
		if ctx.IsTLS() {
			v = protoHTTPS
		}
		dst.SetCanonical(header.S2b(header.XForwardedProto), v)
	}
	if h.forwarded.Host {
		dst.SetCanonical(header.S2b(header.XForwardedHost), ctx.Host())
	}
	if h.forwarded.Via {
		appendForwardedValue(dst, header.Via, viaEntry, h.forwarded.MaxAppend)
	}
}

// applyForwardedInfo injects the client-identity headers that stay
// truthful without a live client connection onto an origin-bound request
// rebuilt from a stored RequestInfo (background revalidate / refresh /
// shed refill). X-Forwarded-For is deliberately NOT appended: there is no
// live peer for this fetch, and replaying the original requester's
// address would attribute one user's identity to an anonymous
// background refresh. Proto and Host come from the captured request
// (ri.TLS / ri host), which keeps origin-side rendering decisions
// (locale, absolute URLs) consistent with the original fetch. Via is
// appended: this fetch is still a bouine→origin hop.
func (h *Handler) applyForwardedInfo(dst *fasthttp.RequestHeader, ri RequestInfo) {
	if !h.forwarded.Enabled() {
		return
	}
	if h.forwarded.Proto {
		v := "http"
		if ri.TLS {
			v = "https"
		}
		dst.Set(header.XForwardedProto, v)
	}
	if h.forwarded.Host {
		dst.Set(header.XForwardedHost, ri.GetHost())
	}
	if h.forwarded.Via {
		appendForwardedValue(dst, header.Via, viaEntry, h.forwarded.MaxAppend)
	}
}

// appendForwardedValue appends addend to the comma-separated chain in the
// named header, replacing the header with the joined result. The chain is
// capped two ways: at most maxAppend entries (rightmost — nearest, most
// recent — kept, matching the trust direction of XFF chains), and under
// forwardedMaxHeaderBytes total (oldest entries dropped first; our own
// appended entry is always kept, so the header never grows unbounded
// across bouine hops). Existing values are carried verbatim — never
// parsed, never rewritten (threat-model T04).
func appendForwardedValue(dst *fasthttp.RequestHeader, name, addend string, maxAppend int) {
	entries := make([]string, 0, maxAppend)
	for _, line := range dst.PeekAll(name) {
		for _, entry := range strings.Split(string(line), ",") {
			if entry = strings.TrimSpace(entry); entry != "" {
				entries = append(entries, entry)
			}
		}
	}
	keep := maxAppend - 1
	if keep < 0 {
		keep = 0
	}
	if len(entries) > keep {
		entries = entries[len(entries)-keep:]
	}
	entries = append(entries, addend)

	total := len(addend)
	for _, entry := range entries {
		total += len(entry) + 2
	}
	for total > forwardedMaxHeaderBytes && len(entries) > 1 {
		total -= len(entries[0]) + 2
		entries = entries[1:]
	}
	dst.Set(name, strings.Join(entries, ", "))
}

// peerIP extracts the host part of addr — the address of the peer that
// opened the connection to bouine (the edge in the issue #769 chain).
// The port is dropped: XFF carries addresses, not ports, and the peer
// port is ephemeral noise.
func peerIP(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	if tcp, ok := addr.(*net.TCPAddr); ok && tcp.IP != nil {
		return tcp.IP.String()
	}
	if host, _, err := net.SplitHostPort(addr.String()); err == nil {
		return host
	}
	return addr.String()
}
