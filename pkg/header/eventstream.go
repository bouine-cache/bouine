// eventstream.go — Server-Sent Events (SSE, WHATWG HTML §9.2.2) media-type
// matching shared by every layer that needs SSE awareness. It lives in this
// leaf package because the detection is needed simultaneously in
// internal/cache (origin-response routing), internal/origin (fetch-client
// selection), and internal/server (per-request write-deadline selection),
// which cannot import each other per AGENTS.md §3.1.
package header

// eventStreamMediaType is the SSE media type (WHATWG HTML, "Server-sent
// events": the response must have a Content-Type whose media type is
// text/event-stream). Media types are case-insensitive per RFC 9110 §8.3.1,
// and parameters (e.g. "; charset=utf-8") may follow the media type.
const eventStreamMediaType = "text/event-stream"

// IsEventStreamContentType reports whether a raw Content-Type value carries
// the text/event-stream media type. Media-type matching is case-insensitive
// (RFC 9110 §8.3.1); parameters after ";" and surrounding OWS are ignored.
// Zero allocations: the scan walks the input bytes directly.
func IsEventStreamContentType(ct []byte) bool {
	// Trim leading OWS.
	for len(ct) > 0 && (ct[0] == ' ' || ct[0] == '\t') {
		ct = ct[1:]
	}
	mt := eventStreamMediaType
	if len(ct) < len(mt) {
		return false
	}
	for i := 0; i < len(mt); i++ {
		lo, want := ct[i]|0x20, mt[i]
		if lo != want {
			return false
		}
	}
	rest := ct[len(mt):]
	// The media type must end at the value end or at a parameter separator.
	for len(rest) > 0 && (rest[0] == ' ' || rest[0] == '\t') {
		rest = rest[1:]
	}
	return len(rest) == 0 || rest[0] == ';'
}

// AcceptsEventStream reports whether a raw Accept value explicitly asks for
// text/event-stream among its media ranges. Per the WHATWG SSE contract,
// conforming clients (EventSource and SDKs) announce stream intent with
// "Accept: text/event-stream"; a bare "*/*" does not count because every
// ordinary browser request carries it. Parameters and q-values after the
// media type are ignored. Zero allocations.
func AcceptsEventStream(accept []byte) bool {
	for len(accept) > 0 {
		// Find the end of this comma-separated media range.
		end := 0
		for end < len(accept) && accept[end] != ',' {
			end++
		}
		if IsEventStreamContentType(accept[:end]) {
			return true
		}
		if end < len(accept) {
			end++ // skip the comma
		}
		accept = accept[end:]
	}
	return false
}
