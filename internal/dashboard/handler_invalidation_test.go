package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/header"
)

// newTestHandler builds a Handler with the minimum config needed to exercise
// the apiOK/apiError/invalidation handlers: a NoopLogger (so render errors
// don't panic) and the supplied invalidation closures.
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
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/dashboard/api/ban", nil)

	h.apiError(w, r, `<img src=x onerror=alert("xss")>`)

	body := w.Body.String()
	assert.Contains(t, body, `&lt;img src=x onerror=alert(&#34;xss&#34;)&gt;`)
	assert.NotContains(t, body, `<img src=x onerror=`)
	assert.Equal(t, "text/html; charset=utf-8", w.Header().Get(header.ContentType))
}

func TestAPIOk_EscapesHTML(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/dashboard/api/purge", nil)

	h.apiOK(w, r, `purged <script>alert(1)</script>`)

	body := w.Body.String()
	assert.Contains(t, body, `&lt;script&gt;alert(1)&lt;/script&gt;`)
	assert.NotContains(t, body, `<script>`)
	// The htmx trigger that tells the client to refresh the ops log is preserved.
	assert.Equal(t, "refreshOpsLog", w.Header().Get(header.HXTrigger))
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

	body := url.Values{"host_regex": {`<img src=x onerror=alert(1>`}}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/dashboard/api/ban",
		strings.NewReader(body.Encode()))
	r.Header.Set(header.ContentType, "application/x-www-form-urlencoded")

	h.apiBan(w, r)

	resp := w.Body.String()
	// The raw attacker payload must never appear as live markup.
	assert.NotContains(t, resp, `<img src=x onerror=`)
	// It must appear HTML-escaped inside the flash-err pill.
	assert.Contains(t, resp, `&lt;img src=x onerror=alert(1&gt;`)
	assert.Contains(t, resp, `class="flash-err"`)
}

func TestLoginHandler_GETEscapesStaticMarkup(t *testing.T) {
	t.Parallel()
	sa := newSessionAuth("tok")
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/dashboard/login", nil)
	sa.LoginHandler(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	assert.Contains(t, body, `<title>bouine · login</title>`)
	assert.Contains(t, body, `name="token"`)
	assert.Contains(t, body, `type="password"`)
	// The static page has no interpolation, but assert no raw script tags leak.
	assert.NotContains(t, body, `<script`)
}
