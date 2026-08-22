package dashboard

import (
	"context"
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
