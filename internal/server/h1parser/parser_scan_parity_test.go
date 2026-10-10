package h1parser

import (
	"errors"
	"math/rand"
	"testing"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// parser_scan_parity_test.go pins the W1 scan replacements
// (parseRequestLine / parseHeaders / skipRequestLine using the stdlib's
// vectorized bytes.Index/bytes.IndexByte searches —
// docs/plans/h1-reactor-perf-round-5.md) against the scalar loops they
// replaced. The parser feeds every cache key, so a semantic drift here is
// a cache-poisoning surface, not just a perf change: the reference
// implementations below are the old loops verbatim, and the differential
// asserts byte-identical outputs (extracted fields, header array, scan
// flags, error identity) across deterministic pseudo-random inputs that
// hammer the boundary conditions (lone '\r', CRLF pairs at the exact end
// of the buffer, pairs straddling the request line, obs-fold).
//
// The differential lives in the test, not in production: once parity is
// pinned here, the fuzz corpus (testdata/fuzz) and conformance suite keep
// it pinned forward.

// refParseRequestLine is the pre-W1 scalar implementation, verbatim.
func refParseRequestLine(buf []byte, req *api.RawRequest) error {
	lineEnd := 0
	for lineEnd < len(buf) && buf[lineEnd] != '\r' {
		lineEnd++
	}
	if lineEnd >= len(buf)-1 || buf[lineEnd+1] != '\n' {
		return errors.New("h1parser: malformed request line")
	}
	line := buf[:lineEnd]

	sp1 := 0
	for sp1 < len(line) && line[sp1] != ' ' {
		sp1++
	}
	if sp1 == len(line) {
		return errors.New("h1parser: missing path")
	}
	req.Method = header.BytesToString(line[:sp1])

	sp2 := sp1 + 1
	for sp2 < len(line) && line[sp2] != ' ' {
		sp2++
	}
	if sp2 == len(line) {
		return errors.New("h1parser: missing version")
	}
	fullPath := header.BytesToString(line[sp1+1 : sp2])

	if q := refIndexByte(fullPath, '?'); q >= 0 {
		req.Path = fullPath[:q]
		req.Query = fullPath[q+1:]
	} else {
		req.Path = fullPath
	}

	req.HTTPVersion = header.BytesToString(line[sp2+1:])
	return nil
}

// refIndexByte is the pre-W1 scalar byte search, verbatim.
func refIndexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// refSkipRequestLine is the pre-W1 scalar implementation, verbatim.
func refSkipRequestLine(buf []byte) int {
	pos := 0
	for pos < len(buf)-1 && (buf[pos] != '\r' || buf[pos+1] != '\n') {
		pos++
	}
	return pos + 2
}

// refParseHeaders is the pre-W1 scalar implementation, verbatim except
// that it shares appendHeader with production (that function is not part
// of the W1 diff; sharing it makes the differential cover the scan change
// exactly).
func refParseHeaders(buf []byte, req *api.RawRequest) error {
	pos := refSkipRequestLine(buf)

	for pos < len(buf) {
		if buf[pos] == '\r' && pos+1 < len(buf) && buf[pos+1] == '\n' {
			break
		}

		lineEnd := pos
		for lineEnd < len(buf)-1 && (buf[lineEnd] != '\r' || buf[lineEnd+1] != '\n') {
			lineEnd++
		}
		if lineEnd >= len(buf)-1 {
			break
		}

		if buf[pos] == ' ' || buf[pos] == '\t' {
			return errors.New("h1parser: obs-fold not supported")
		}

		if req.NHeaders >= api.MaxRawHeaders {
			return errors.New("h1parser: too many headers")
		}

		appendHeader(req, buf[pos:lineEnd])

		pos = lineEnd + 2
	}
	return nil
}

// parityReq captures every field the scanners can influence.
type parityReq struct {
	err             error
	method          string
	path            string
	query           string
	version         string
	host            string
	ccRaw           string
	connectionClose bool
	scanFlags       api.RequestScanFlags
	nHeaders        int
	headers         [api.MaxRawHeaders]api.RawHeader
}

func captureParity(err error, req *api.RawRequest) parityReq {
	p := parityReq{
		err:             err,
		method:          req.Method,
		path:            req.Path,
		query:           req.Query,
		version:         req.HTTPVersion,
		host:            req.Host,
		ccRaw:           req.CacheControlRaw,
		connectionClose: req.ConnectionClose,
		scanFlags:       req.ScanFlags,
		nHeaders:        req.NHeaders,
	}
	copy(p.headers[:], req.Headers[:])
	return p
}

func (p parityReq) diff(other parityReq) string {
	gotMsg, wantMsg := "", ""
	if p.err != nil {
		gotMsg = p.err.Error()
	}
	if other.err != nil {
		wantMsg = other.err.Error()
	}
	if gotMsg != wantMsg {
		return "error"
	}
	if p.err != nil {
		return "" // parse stopped at the same error: later fields are unspecified in both
	}
	if p.method != other.method {
		return "method"
	}
	if p.path != other.path {
		return "path"
	}
	if p.query != other.query {
		return "query"
	}
	if p.version != other.version {
		return "version"
	}
	if p.host != other.host {
		return "host"
	}
	if p.ccRaw != other.ccRaw {
		return "cacheControlRaw"
	}
	if p.connectionClose != other.connectionClose {
		return "connectionClose"
	}
	if p.scanFlags != other.scanFlags {
		return "scanFlags"
	}
	if p.nHeaders != other.nHeaders {
		return "nHeaders"
	}
	for i := 0; i < p.nHeaders; i++ {
		if p.headers[i] != other.headers[i] {
			return "header"
		}
	}
	return ""
}

// scanAlphabet biases generated input toward the parser's decision
// bytes: CR/LF (line boundaries), SP (request-line fields), ':' and OWS
// (header lines), plus ordinary request bytes.
var scanAlphabet = []byte(" \r\n:/?=&,.;aA1-\"'*\tGTEOHXPx")

// genInput builds a deterministic pseudo-random byte string over the
// scan alphabet. Fixed seed: no test randomness policy violation.
func genInput(rng *rand.Rand, maxLen int) []byte {
	n := rng.Intn(maxLen + 1)
	b := make([]byte, n)
	for i := range b {
		b[i] = scanAlphabet[rng.Intn(len(scanAlphabet))]
	}
	return b
}

// TestScanParity_RandomDifferential asserts the W1 vectorized scanners
// produce byte-identical parses to the scalar reference across
// deterministic pseudo-random inputs.
func TestScanParity_RandomDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(605)) //nolint:gosec // G404: deterministic test seed, not crypto
	for i := 0; i < 20000; i++ {
		buf := genInput(rng, 96)
		var got, want api.RawRequest
		gotErr := parseRequestLine(buf, &got)
		wantErr := refParseRequestLine(buf, &want)
		if field := captureParity(gotErr, &got).diff(captureParity(wantErr, &want)); field != "" {
			t.Fatalf("input %d %q: request line diverged at %s\ngot:  %+v\nwant: %+v",
				i, buf, field, got, want)
		}
		if gotErr != nil {
			continue
		}
		gotErr = parseHeaders(buf, &got)
		wantErr = refParseHeaders(buf, &want)
		if field := captureParity(gotErr, &got).diff(captureParity(wantErr, &want)); field != "" {
			t.Fatalf("input %d %q: headers diverged at %s\ngot:  %+v\nwant: %+v",
				i, buf, field, got, want)
		}
	}
}

// TestScanParity_SkipRequestLine pins skipRequestLine's exact return
// values (including the no-pair sentinels that callers' loop guards
// depend on) against the scalar reference.
func TestScanParity_SkipRequestLine(t *testing.T) {
	rng := rand.New(rand.NewSource(606)) //nolint:gosec // G404: deterministic test seed, not crypto
	for i := 0; i < 20000; i++ {
		buf := genInput(rng, 64)
		if got, want := skipRequestLine(buf), refSkipRequestLine(buf); got != want {
			t.Fatalf("input %d %q: skipRequestLine = %d, want %d", i, buf, got, want)
		}
	}
}

// TestScanParity_Boundaries pins the hand-derived edge cases of the
// vectorized searches — the exact inputs where a naive Index replacement
// diverges from the scalar scans it replaced.
func TestScanParity_Boundaries(t *testing.T) {
	cases := []string{
		"",                                    // empty buffer: malformed request line
		"\r",                                  // lone CR as the final byte: malformed
		"\r\n",                                // empty request line: missing path
		"GET\r\n",                             // no SP: missing path
		"GET / HTTP/1.1\r",                    // CR at len-2 without LF: malformed
		"GET / HTTP/1.1\r\n",                  // request line only, no headers
		"GET /?a=1 HTTP/1.1\r\nHost: h\r\n",   // query split on the last header line
		"GET / HTTP/1.1\r\nX: 1\r",            // header line whose CRLF is truncated to a lone CR
		"GET / HTTP/1.1\r\nX: 1\r\n\r",        // terminator truncated to a lone CR
		"GET / HTTP/1.1\r\nX: 1\r\n\r\n",      // terminator ends at the final byte
		"GET / HTTP/1.1\r\nX: 1\r\n\r\nGET",   // terminator + pipelined excess
		"GET / HTTP/1.1\r\n X: 1\r\n\r\n",     // obs-fold at the first header line
		"GET / HTTP/1.1\r\nX: a\rb\r\n\r\n",   // lone CR mid header line
		"GET / HTTP/1.1\r\n\rX: 1\r\n\r\n",    // lone CR at a header line start
		"GET /a?b HTTP/1.1\r\nX: 1\r\n\r\n\r", // trailing lone CR after full block
	}
	for _, raw := range cases {
		buf := []byte(raw)
		var got, want api.RawRequest
		gotErr := parseRequestLine(buf, &got)
		wantErr := refParseRequestLine(buf, &want)
		if field := captureParity(gotErr, &got).diff(captureParity(wantErr, &want)); field != "" {
			t.Errorf("%q: request line diverged at %s\ngot:  %+v\nwant: %+v", raw, field, got, want)
			continue
		}
		if gotErr != nil {
			continue
		}
		gotErr = parseHeaders(buf, &got)
		wantErr = refParseHeaders(buf, &want)
		if field := captureParity(gotErr, &got).diff(captureParity(wantErr, &want)); field != "" {
			t.Errorf("%q: headers diverged at %s\ngot:  %+v\nwant: %+v", raw, field, got, want)
		}
	}
}

// TestScanParity_RealisticTraffic runs the differential over
// production-shaped request heads (the shape the W2 gates benchmark)
// with seeded mutations, so the parity holds on realistic input too,
// not just adversarial byte soup.
func TestScanParity_RealisticTraffic(t *testing.T) {
	rng := rand.New(rand.NewSource(607)) //nolint:gosec // G404: deterministic test seed, not crypto
	base := benchRealisticHead(8)
	for i := 0; i < 5000; i++ {
		buf := append([]byte(nil), base...)
		for m := 0; m < rng.Intn(4); m++ {
			pos := rng.Intn(len(buf))
			buf[pos] = scanAlphabet[rng.Intn(len(scanAlphabet))]
		}
		var got, want api.RawRequest
		gotErr := parseRequestLine(buf, &got)
		wantErr := refParseRequestLine(buf, &want)
		if field := captureParity(gotErr, &got).diff(captureParity(wantErr, &want)); field != "" {
			t.Fatalf("mutation %d: request line diverged at %s\ngot:  %+v\nwant: %+v", i, field, got, want)
		}
		if gotErr != nil {
			continue
		}
		gotErr = parseHeaders(buf, &got)
		wantErr = refParseHeaders(buf, &want)
		if field := captureParity(gotErr, &got).diff(captureParity(wantErr, &want)); field != "" {
			t.Fatalf("mutation %d: headers diverged at %s\ngot:  %+v\nwant: %+v", i, field, got, want)
		}
	}
}

// benchRealisticHead builds a production-shaped request head with
// extraHeaders headers beyond Host: a long request line with a query,
// plus the canonical browser/proxy headers real clients send (~60 B per
// line). The W2 gate benchmarks parse this shape — the toy 2-header
// request the older gates use hides the per-header scan cost that
// dominates real parse time.
func benchRealisticHead(extraHeaders int) []byte {
	buf := []byte("GET /v1/products/42?include=specs&fields=all HTTP/1.1\r\nHost: www.example.com\r\n")
	for i := 0; i < extraHeaders; i++ {
		switch i % 8 {
		case 0:
			buf = append(buf, "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36\r\n"...)
		case 1:
			buf = append(buf, "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8\r\n"...)
		case 2:
			buf = append(buf, "Accept-Language: en-US,en;q=0.9,fr;q=0.8\r\n"...)
		case 3:
			buf = append(buf, "Accept-Encoding: gzip, deflate, br\r\n"...)
		case 4:
			buf = append(buf, "Referer: https://www.example.com/home?page=2&sort=desc\r\n"...)
		case 5:
			buf = append(buf, "X-Forwarded-For: 203.0.113.195, 70.41.3.18\r\n"...)
		case 6:
			buf = append(buf, "X-Request-Id: 8f66467e-9a2c-4c82-a1d3-5f9a3c1d2e4f\r\n"...)
		default:
			buf = append(buf, "Sec-Fetch-Mode: navigate\r\n"...)
		}
	}
	return append(buf, "\r\n"...)
}
