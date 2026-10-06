package cache

import (
	"bufio"
	"strings"
	"testing"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

// multiLineCookieRaw builds a RawRequest whose cookies are spread over
// two Cookie header lines — the shape the h1parser produces for a
// client that sent repeated Cookie field lines.
func multiLineCookieRaw(path string) *api.RawRequest {
	req := &api.RawRequest{
		Method:      "GET",
		Path:        path,
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	req.Headers[0] = api.RawHeader{Key: header.Cookie, Value: "analytics=1"}
	req.Headers[1] = api.RawHeader{Key: header.Cookie, Value: "consent=yes"}
	req.NHeaders = 2
	return req
}

// Regression: the presence bits must hash the same value over EVERY
// representation of the same wire request — the slow-path map
// (separate entries), the RawRequest (separate lines), fasthttp's
// header (separate args pre-collection), and the peer-gate's map
// built from the RawRequest (reqHeaderMapFromRaw — the input
// BuildVaryKey, the wire VaryKey, is defined over). The old code fed
// GetAll's ", " join into the "; "-splitting presence computation, so
// the second line's consent pair folded into the first line's last
// value and the bits hashed 0 for a request whose listed cookie was
// present — the peer gate then rejected every presence-keyed exchange
// of a multi-line cookied request.
func TestCookiePresence_MultiLineCookieKeyParity(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookiePresence([]string{"consent"})
	primary := testkey.Key(31)
	vary := policy.CookiePresenceVary()

	// The map with separate Cookie entries — headerFromCtx shape for a
	// request whose Cookie lines were never collected... and the exact
	// shape reqHeaderMapFromRaw produces on the peer branch.
	sep := header.NewMap(2)
	sep.AppendEntryCanonical(header.Cookie, "analytics=1")
	sep.AppendEntryCanonical(header.Cookie, "consent=yes")

	kSep := VariantKey(primary, vary, sep, policy)

	// The RawRequest shape (fast path).
	kRaw := VariantKeyFromRaw(primary, vary, multiLineCookieRaw("/"), policy)

	// The fasthttp shape: a real parsed 2-line wire request.
	var fh fasthttp.RequestHeader
	wire := "GET / HTTP/1.1\r\nHost: example.com\r\nCookie: analytics=1\r\nCookie: consent=yes\r\n\r\n"
	require.NoError(t, fh.Read(bufio.NewReader(strings.NewReader(wire))))
	kFast := VariantKeyFast(primary, vary, &fh, policy)

	// One-line joined control: the canonical "; " value.
	joined := header.NewMap(1)
	joined.AppendEntryCanonical(header.Cookie, "analytics=1; consent=yes")
	kJoined := VariantKey(primary, vary, joined, policy)

	require.Equal(t, kJoined, kSep, "separate-entry map must key like the joined value")
	require.Equal(t, kJoined, kRaw, "RawRequest must key like the joined value")
	require.Equal(t, kJoined, kFast, "fasthttp header must key like the joined value")

	// The assertion (peer-gate hash) and the variant key must agree —
	// and the bits must be 1 (consent present), never 0.
	require.Equal(t, policy.cookiePresenceValue("analytics=1; consent=yes"), "1")
	assertion := BuildVaryKey(vary, sep, policy)
	require.NotEmpty(t, assertion)
	require.Equal(t, BuildVaryKey(vary, joined, policy), assertion,
		"the VaryKey assertion must not depend on the Cookie line layout")
}

// Regression: requestInfoFromRaw — the h1parser keeps Cookie lines
// separately and header.Map.Set overwrites, so a per-line Set dropped
// every line but the last, and the SWR replay (TriggerBgRevalidateFromFastPath)
// re-keyed the presence bits wrong. The Cookie lines must be joined.
func TestCookiePresence_RequestInfoFromRawJoinsCookieLines(t *testing.T) {
	t.Parallel()
	ri := requestInfoFromRaw(multiLineCookieRaw("/join"))
	require.Equal(t, "analytics=1; consent=yes", ri.Header.Get(header.Cookie),
		"every Cookie line must survive the materialization, joined §4.2")
}

// Regression, end-to-end: a 2-line wire Cookie request on a
// presence-keyed route must HIT the variant its presence selects —
// not the absent-cookie variant, and not a miss.
func TestCookiePresence_MultiLineWireHitsRightVariant(t *testing.T) {
	t.Parallel()
	h := testHandlerCookiePresence(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60, public")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("render-consent=" + string(ctx.Request.Header.Peek(header.Cookie))))
	}, []string{"consent"})

	// Fill the absent variant.
	anon := testCtx("GET", "http://example.com/m")
	h.ServeRequest(anon)
	require.Equal(t, "MISS", respHeader(anon, header.XCache))

	// Fill the present variant (single line).
	present := testCtx("GET", "http://example.com/m")
	present.Request.Header.Set(header.Cookie, "consent=yes")
	h.ServeRequest(present)
	require.Equal(t, "MISS", respHeader(present, header.XCache))

	// 2-line wire request whose consent sits on the second line: the
	// presence bit is 1, so it must HIT the present variant.
	multi := testCtx("GET", "http://example.com/m")
	wire := "GET /m HTTP/1.1\r\nHost: example.com\r\nCookie: analytics=1\r\nCookie: consent=yes\r\n\r\n"
	require.NoError(t, multi.Request.Read(bufio.NewReader(strings.NewReader(wire))))
	h.ServeRequest(multi)
	require.Equal(t, "HIT", respHeader(multi, header.XCache),
		"the 2-line request selects the present variant and must HIT it")
	require.Contains(t, respBody(multi), "consent", "must serve the present-variant render, never the absent one")
}

// Regression: the refresh registry replay over a multi-line Cookie
// request must reproduce the stored VaryKey — the registry joins with
// "; " (CookieAll), and BuildVaryKey over the joined single entry must
// equal the store's assertion over the original separate entries.
func TestCookiePresence_RefreshRegistryMultiLineReplay(t *testing.T) {
	t.Parallel()
	// Build the map exactly as headerFromCtx does for a 2-line Cookie
	// request that went through All() collection: fasthttp collects into
	// ONE joined value, so requestInfoFromCtx sees one entry. The
	// separate-entry shape is the RawRequest-derived one; both must
	// register and replay the same assertion.
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookiePresence([]string{"consent"})

	sep := header.NewMap(2)
	sep.AppendEntryCanonical(header.Cookie, "analytics=1")
	sep.AppendEntryCanonical(header.Cookie, "consent=yes")
	stored := BuildVaryKey(policy.CookiePresenceVary(), sep, policy)

	joined := header.NewMap(1)
	joined.AppendEntryCanonical(header.Cookie, "analytics=1; consent=yes")
	replayable := BuildVaryKey(policy.CookiePresenceVary(), joined, policy)
	require.Equal(t, stored, replayable)
}

// One control: all four representations of the same wire request key
// identically, and the peer-gate assertion equals the store's.
func TestCookiePresence_MultiLineGateAssertionParity(t *testing.T) {
	t.Parallel()
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookiePresence([]string{"consent"})
	vary := policy.CookiePresenceVary()
	primary := testkey.Key(32)

	sep := header.NewMap(2)
	sep.AppendEntryCanonical(header.Cookie, "analytics=1")
	sep.AppendEntryCanonical(header.Cookie, "consent=yes")
	joined := header.NewMap(1)
	joined.AppendEntryCanonical(header.Cookie, "analytics=1; consent=yes")

	// VariantKey across representations.
	require.Equal(t, VariantKey(primary, vary, joined, policy), VariantKey(primary, vary, sep, policy), "map: joined == separate")
	require.Equal(t, VariantKey(primary, vary, joined, policy), VariantKeyFromRaw(primary, vary, multiLineCookieRaw("/"), policy), "raw == joined")
	var fh fasthttp.RequestHeader
	wire := "GET / HTTP/1.1\r\nHost: example.com\r\nCookie: analytics=1\r\nCookie: consent=yes\r\n\r\n"
	require.NoError(t, fh.Read(bufio.NewReader(strings.NewReader(wire))))
	require.Equal(t, VariantKey(primary, vary, joined, policy), VariantKeyFast(primary, vary, &fh, policy), "fasthttp == joined")

	// Peer-gate assertion across representations.
	require.Equal(t, BuildVaryKey(vary, joined, policy), BuildVaryKey(vary, sep, policy), "assertion: joined == separate")
	require.Equal(t, BuildVaryKey(vary, joined, policy), BuildVaryKey(vary, reqHeaderMapFromRaw(multiLineCookieRaw("/")), policy), "assertion: raw map == joined")
	require.Equal(t, BuildVaryKey(vary, joined, policy), BuildVaryKey(vary, headerFromFastHTTPReqHeader(&fh), policy), "assertion: fasthttp map == joined")
}
