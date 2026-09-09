package dashboard

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/header"
)

// newTestHandler builds a Handler with the minimum config needed to exercise
// the apiOK/apiError/invalidation handlers: a NoopLogger so render errors are
// logged rather than panicked, and no invalidation closures wired.
func newTestHandler(t *testing.T) *Handler {
	t.Helper()
	return &Handler{
		cfg: Config{
			Logger: observability.NoopLogger{},
		},
	}
}

func TestAPIError_EscapesHTML(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/ban")

	h.apiError(ctx, `<img src=x onerror=alert("xss")>`)

	body := string(ctx.Response.Body())
	assert.Contains(t, body, `&lt;img src=x onerror=alert(&#34;xss&#34;)&gt;`)
	assert.NotContains(t, body, `<img src=x onerror=`)
	assert.Equal(t, "text/html; charset=utf-8", string(ctx.Response.Header.Peek(header.ContentType)))
}

func TestAPIOk_EscapesHTML(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/purge")

	h.apiOK(ctx, `purged <script>alert(1)</script>`)

	body := string(ctx.Response.Body())
	assert.Contains(t, body, `&lt;script&gt;alert(1)&lt;/script&gt;`)
	assert.NotContains(t, body, `<script>`)
	// The htmx trigger that tells the client to refresh the ops log is preserved.
	assert.Equal(t, "refreshOpsLog", string(ctx.Response.Header.Peek(header.HXTrigger)))
}

// TestAPIBan_InvalidRegexEscaped reproduces the reflected-XSS vector from
// issue #294: an invalid host_regex whose compile error echoes the attacker
// input must be HTML-escaped in the response, never rendered as markup.
func TestAPIBan_InvalidRegexEscaped(t *testing.T) {
	t.Parallel()
	h := &Handler{
		cfg: Config{
			Logger: observability.NoopLogger{},
			BanFn: func(_ context.Context, _, _ string) (int, error) {
				return 0, nil
			},
		},
	}

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/ban")
	ctx.Request.Header.SetContentType("application/x-www-form-urlencoded")
	ctx.PostArgs().Set("host_regex", `<img src=x onerror=alert(1>`)

	h.apiBan(ctx)

	resp := string(ctx.Response.Body())
	// The raw attacker payload must never appear as live markup.
	assert.NotContains(t, resp, `<img src=x onerror=`)
	// It must appear HTML-escaped inside the flash-err pill.
	assert.Contains(t, resp, `&lt;img src=x onerror=alert(1&gt;`)
	assert.Contains(t, resp, `class="flash-err"`)
}

func TestLoginHandler_RendersForm(t *testing.T) {
	t.Parallel()
	sa := newSessionAuth("tok")
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/login")
	sa.LoginHandler(ctx)

	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
	body := string(ctx.Response.Body())
	assert.Contains(t, body, `<title>bouine · login</title>`)
	assert.Contains(t, body, `name="token"`)
	assert.Contains(t, body, `type="password"`)
}

// newPurgeHandler builds a Handler recording every purged URL.
func newPurgeHandler(t *testing.T) (*Handler, *[]string) {
	t.Helper()
	var purged []string
	h := &Handler{
		cfg: Config{
			Logger: observability.NoopLogger{},
			Rings:  observability.NewRings("self"),
			PurgeFn: func(_ context.Context, url string) error {
				purged = append(purged, url)
				return nil
			},
		},
	}
	return h, &purged
}

// TestAPIPurgeBatch_Form asserts the batch endpoint accepts
// newline-separated URLs from the textarea form field and purges each
// in order.
func TestAPIPurgeBatch_Form(t *testing.T) {
	t.Parallel()
	h, purged := newPurgeHandler(t)
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/purge/batch")
	ctx.Request.Header.SetContentType("application/x-www-form-urlencoded")
	ctx.PostArgs().Set("urls", "https://example.com/a\nhttps://example.com/b\n\n  https://example.com/c  ")

	h.apiPurgeBatch(ctx)

	resp := string(ctx.Response.Body())
	assert.Contains(t, resp, "purged 3 URLs")
	assert.Contains(t, resp, "class=\"flash-ok\"")
	assert.Equal(t, []string{
		"https://example.com/a",
		"https://example.com/b",
		"https://example.com/c",
	}, *purged)
}

// TestAPIPurgeBatch_JSON asserts the batch endpoint also accepts a JSON
// urls array (API parity with the admin batch endpoint).
func TestAPIPurgeBatch_JSON(t *testing.T) {
	t.Parallel()
	h, purged := newPurgeHandler(t)
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/purge/batch")
	ctx.Request.Header.SetContentType("application/json")
	ctx.Request.SetBody([]byte(`{"urls":["https://example.com/x","https://example.com/y"]}`))

	h.apiPurgeBatch(ctx)

	assert.Contains(t, string(ctx.Response.Body()), "purged 2 URLs")
	assert.Len(t, *purged, 2)
}

// TestAPIPurgeBatch_InvalidURL asserts one bad URL rejects the whole
// batch before any purge runs.
func TestAPIPurgeBatch_InvalidURL(t *testing.T) {
	t.Parallel()
	h, purged := newPurgeHandler(t)
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/purge/batch")
	ctx.Request.Header.SetContentType("application/json")
	ctx.Request.SetBody([]byte(`{"urls":["https://example.com/ok","ftp://example.com/bad"]}`))

	h.apiPurgeBatch(ctx)

	assert.Contains(t, string(ctx.Response.Body()), "must begin with http")
	assert.Empty(t, *purged, "no URL should be purged when the batch is rejected")
}

// TestAPIPurgeBatch_Limit asserts the batch cap rejects oversized
// submissions before purging.
func TestAPIPurgeBatch_Limit(t *testing.T) {
	t.Parallel()
	h, purged := newPurgeHandler(t)
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/purge/batch")
	ctx.Request.Header.SetContentType("application/json")
	urls := make([]string, maxPurgeBatchURLs+1)
	for i := range urls {
		urls[i] = "https://example.com/" + strconv.Itoa(i)
	}
	body, err := json.Marshal(map[string][]string{"urls": urls})
	require.NoError(t, err)
	ctx.Request.SetBody(body)

	h.apiPurgeBatch(ctx)

	assert.Contains(t, string(ctx.Response.Body()), "URL limit")
	assert.Empty(t, *purged)
}

// newBanHandler builds a Handler whose BanFn reports n evictions.
func newBanHandler(t *testing.T, n int) *Handler {
	t.Helper()
	return &Handler{
		cfg: Config{
			Logger: observability.NoopLogger{},
			Rings:  observability.NewRings("self"),
			BanFn: func(_ context.Context, _, _ string) (int, error) {
				return n, nil
			},
		},
	}
}

// TestAPIBan_FeedbackEagerScan asserts the success flash reports the
// eager-scan eviction count when the scan ran and matched entries.
func TestAPIBan_FeedbackEagerScan(t *testing.T) {
	t.Parallel()
	h := newBanHandler(t, 12)
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/ban")
	ctx.Request.Header.SetContentType("application/x-www-form-urlencoded")
	ctx.PostArgs().Set("path_regex", `^/api/`)

	h.apiBan(ctx)

	resp := string(ctx.Response.Body())
	assert.Contains(t, resp, "banned, 12 entries evicted")
	assert.Contains(t, resp, "class=\"flash-ok\"")
}

// TestAPIBan_FeedbackCoalescedScan asserts that a zero eviction count —
// which happens when the eager scan coalesced into a recent 50ms window
// or genuinely matched nothing — still reads as a successful ban, with
// wording that explains the lazy predicate semantics instead of
// implying the ban was a no-op.
func TestAPIBan_FeedbackCoalescedScan(t *testing.T) {
	t.Parallel()
	h := newBanHandler(t, 0)
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/api/ban")
	ctx.Request.Header.SetContentType("application/x-www-form-urlencoded")
	ctx.PostArgs().Set("path_regex", `^/api/`)

	h.apiBan(ctx)

	resp := string(ctx.Response.Body())
	assert.Contains(t, resp, "banned; predicate is registered")
	assert.Contains(t, resp, "class=\"flash-ok\"")
	assert.NotContains(t, resp, "0 entries evicted")
}
