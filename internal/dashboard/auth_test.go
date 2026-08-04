package dashboard

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/header"
)

func TestSessionAuth_SignAndValidate(t *testing.T) {
	t.Parallel()
	sa := newSessionAuth("secret-token")

	tok, err := sa.makeToken()
	require.NoErrorf(t, err, "makeToken: %v", err)
	assert.True(t, sa.valid(tok))
}

func TestSessionAuth_ValidRejectsShort(t *testing.T) {
	t.Parallel()
	sa := newSessionAuth("secret-token")
	assert.False(t, sa.valid("short"))
	assert.False(t, sa.valid(""))
}

func TestSessionAuth_ValidRejectsTamperedSig(t *testing.T) {
	t.Parallel()
	sa := newSessionAuth("secret-token")
	tok, _ := sa.makeToken()
	// Flip last character.
	tampered := tok[:len(tok)-1] + "X"
	assert.False(t, sa.valid(tampered))
}

func TestSessionAuth_DifferentKeyRejects(t *testing.T) {
	t.Parallel()
	sa1 := newSessionAuth("token-a")
	sa2 := newSessionAuth("token-b")
	tok, _ := sa1.makeToken()
	assert.False(t, sa2.valid(tok))
}

func TestSessionAuth_LoginGet(t *testing.T) {
	t.Parallel()
	sa := newSessionAuth("my-token")
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/dashboard/login", nil)
	sa.LoginHandler(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "bouine")
}

func TestSessionAuth_LoginPostWrongToken(t *testing.T) {
	t.Parallel()
	sa := newSessionAuth("correct-token")
	body := url.Values{"token": {"wrong"}}.Encode()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/dashboard/login",
		strings.NewReader(body))
	r.Header.Set(header.ContentType, "application/x-www-form-urlencoded")
	sa.LoginHandler(w, r)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestSessionAuth_LoginPostCorrectToken(t *testing.T) {
	t.Parallel()
	sa := newSessionAuth("correct-token")
	body := url.Values{"token": {"correct-token"}}.Encode()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/dashboard/login",
		strings.NewReader(body))
	r.Header.Set(header.ContentType, "application/x-www-form-urlencoded")
	sa.LoginHandler(w, r)
	assert.Equal(t, http.StatusSeeOther, w.Code)
	cookie := w.Header().Get(header.SetCookie)
	assert.Contains(t, cookie, sessionCookieName)
}

func TestSessionAuth_MiddlewareRedirectsWithoutCookie(t *testing.T) {
	t.Parallel()
	sa := newSessionAuth("tok")
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/dashboard/", nil)
	sa.Middleware(next).ServeHTTP(w, r)
	assert.Equal(t, http.StatusFound, w.Code)
}

func TestSessionAuth_MiddlewarePassesValidCookie(t *testing.T) {
	t.Parallel()
	sa := newSessionAuth("tok")
	tok, err := sa.makeToken()
	require.NoErrorf(t, err, "makeToken: %v", err)

	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/dashboard/", nil)
	r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: tok})
	sa.Middleware(next).ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
}
