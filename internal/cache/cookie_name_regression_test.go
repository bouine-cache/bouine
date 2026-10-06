package cache

import (
	"bufio"
	"strings"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

// Regression: the listed cookie in every position must trigger the
// bypass — the scanner's length gate ran before the OWS trim, so
// " session_id" (11 bytes) was rejected against a 10-byte name before
// the trim could see it. Real browsers send "; "-joined cookie lines,
// so the listed cookie typically sits AFTER a separator.
func TestCookieNameBypass_ListedCookieAfterSeparator(t *testing.T) {
	t.Parallel()
	h := testHandlerBypassOnCookieNames(t, origin200("shared"), []string{"session_id"})

	anon := testCtx("GET", "http://example.com/sep")
	h.ServeRequest(anon)
	require.Equal(t, "MISS", respHeader(anon, header.XCache))

	for _, cookie := range []string{
		"theme=dark; session_id=1",
		"a=1; b=2; session_id=x",
		"session_id=1; theme=dark",
		"theme=dark;  session_id=1", // double space
		"\tsession_id=1",            // leading tab
		"session_id",                // value-less pair
	} {
		req := testCtx("GET", "http://example.com/sep")
		req.Request.Header.Set(header.Cookie, cookie)
		h.ServeRequest(req)
		assertBypass(t, req, cookie)
	}

	// Still no false positives on unlisted names.
	for _, cookie := range []string{
		"theme=dark; other=1",
		"mysession_id=1; session_id_v2=2",
		"ref=session_id",
	} {
		req := testCtx("GET", "http://example.com/sep")
		req.Request.Header.Set(header.Cookie, cookie)
		h.ServeRequest(req)
		assertNotBypass(t, req, cookie)
	}
}

// Regression: multi-line Cookie on the slow path. fasthttp's Peek
// returns only the FIRST line until collectCookies runs, so a
// single-Peek gate missed a listed cookie on the second line and the
// fall-through request was served from the cache.
func TestCookieNameBypass_SlowPathMultiLineWire(t *testing.T) {
	t.Parallel()
	h := testHandlerBypassOnCookieNames(t, origin200("shared"), []string{"session_id"})

	anon := testCtx("GET", "http://example.com/wire")
	h.ServeRequest(anon)
	require.Equal(t, "MISS", respHeader(anon, header.XCache))

	// Exactly the bytes handleFallThrough produces for a 2-line Cookie
	// request: rebuildRequestHead replays every line, fasthttp parses
	// them, ServeRequest gates. The listed cookie is on line 2.
	ctx := &fasthttp.RequestCtx{}
	wire := "GET /wire HTTP/1.1\r\nHost: example.com\r\nCookie: analytics=1\r\nCookie: session_id=u\r\n\r\n"
	require.NoError(t, ctx.Request.Read(bufio.NewReader(strings.NewReader(wire))))
	h.ServeRequest(ctx)
	assertBypass(t, ctx, "2-line Cookie wire request")

	// Line order swapped: listed cookie on line 1, unlisted on line 2.
	ctx2 := &fasthttp.RequestCtx{}
	wire2 := "GET /wire HTTP/1.1\r\nHost: example.com\r\nCookie: session_id=u\r\nCookie: analytics=1\r\n\r\n"
	require.NoError(t, ctx2.Request.Read(bufio.NewReader(strings.NewReader(wire2))))
	h.ServeRequest(ctx2)
	assertBypass(t, ctx2, "2-line Cookie wire request, listed first")

	// Multi-line with only unlisted cookies still participates.
	ctx3 := &fasthttp.RequestCtx{}
	wire3 := "GET /wire HTTP/1.1\r\nHost: example.com\r\nCookie: analytics=1\r\nCookie: theme=dark\r\n\r\n"
	require.NoError(t, ctx3.Request.Read(bufio.NewReader(strings.NewReader(wire3))))
	h.ServeRequest(ctx3)
	assertNotBypass(t, ctx3, "2-line Cookie wire request, all unlisted")
}

// Regression: the same multi-line shape through the bypass_on_cookie
// presence trigger (the old gate was a single Peek there too).
func TestCookieBypass_PresenceTriggerMultiLineWire(t *testing.T) {
	t.Parallel()
	h := testCookieBypassHandler(t)

	anon := testCtx("GET", "http://example.com/pres")
	h.ServeRequest(anon)
	require.Equal(t, "MISS", respHeader(anon, header.XCache))

	ctx := &fasthttp.RequestCtx{}
	wire := "GET /pres HTTP/1.1\r\nHost: example.com\r\nCookie: analytics=1\r\nCookie: theme=dark\r\n\r\n"
	require.NoError(t, ctx.Request.Read(bufio.NewReader(strings.NewReader(wire))))
	h.ServeRequest(ctx)
	assertBypass(t, ctx, "presence trigger over 2-line Cookie wire request")

	// The case a single Peek genuinely missed: an EMPTY first Cookie
	// line. fasthttp's Peek returns "" (line 1) until collection, so
	// the old len(Peek) == 0 gate saw "no cookie" and served from the
	// cache; the real cookies were on line 2. PeekAll must see them.
	ctx2 := &fasthttp.RequestCtx{}
	wire2 := "GET /pres HTTP/1.1\r\nHost: example.com\r\nCookie: \r\nCookie: theme=dark\r\n\r\n"
	require.NoError(t, ctx2.Request.Read(bufio.NewReader(strings.NewReader(wire2))))
	h.ServeRequest(ctx2)
	assertBypass(t, ctx2, "presence trigger with empty first Cookie line")
}

// Regression: fast-path decline parity for the multi-line shape —
// cookieBypassTriggeredRaw walks each line; the slow path must agree
// on the same wire bytes (the pair is what a fall-through exercises).
func TestCookieNameBypass_FastPathDeclinesMultiLineAndSlowPathAgrees(t *testing.T) {
	t.Parallel()
	h := testHandlerBypassOnCookieNames(t, origin200("shared"), []string{"session_id"})

	anon := testCtx("GET", "http://example.com/decline")
	h.ServeRequest(anon)
	require.Equal(t, "MISS", respHeader(anon, header.XCache))

	fp := NewFastPathHandler(h)
	raw := &api.RawRequest{Method: "GET", Path: "/decline", Host: "example.com", Scheme: "http", HTTPVersion: "HTTP/1.1"}
	raw.Headers[0] = api.RawHeader{Key: header.Cookie, Value: "analytics=1"}
	raw.Headers[1] = api.RawHeader{Key: header.Cookie, Value: "session_id=u"}
	raw.NHeaders = 2
	_, ok := fp.TryHit(raw, time.Now())
	require.False(t, ok, "fast path must decline the listed cookie on the second line")
}

func testCookieBypassHandler(t *testing.T) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  1 << 20,
		NumShards: 2,
	})
	return NewHandler(HandlerConfig{
		Upstream:       origin200("shared"),
		FastClient:     &testFastClient{handler: origin200("shared")},
		Store:          store,
		BypassOnCookie: true,
	})
}

func assertBypass(t *testing.T, ctx *fasthttp.RequestCtx, caseName string) {
	t.Helper()
	require.Equal(t, "BYPASS", respHeader(ctx, header.XCache),
		"%s: expected BYPASS, got %s", caseName, respHeader(ctx, header.XCache))
}

func assertNotBypass(t *testing.T, ctx *fasthttp.RequestCtx, caseName string) {
	t.Helper()
	require.NotEqual(t, "BYPASS", respHeader(ctx, header.XCache),
		"%s: expected participation (not BYPASS)", caseName)
}
