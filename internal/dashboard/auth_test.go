package dashboard

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/bouine-cache/bouine/pkg/header"
)

func TestSessionAuth_SignAndValidate(t *testing.T) {
	t.Parallel()
	sa := newSessionAuth("secret-token")

	tok, err := sa.makeToken()
	if err != nil {
		t.Fatalf("makeToken: %v", err)
	}
	if !sa.valid(tok) {
		t.Error("freshly minted token must be valid")
	}
}

func TestSessionAuth_ValidRejectsShort(t *testing.T) {
	t.Parallel()
	sa := newSessionAuth("secret-token")
	if sa.valid("short") {
		t.Error("short token should be invalid")
	}
	if sa.valid("") {
		t.Error("empty token should be invalid")
	}
}

func TestSessionAuth_ValidRejectsTamperedSig(t *testing.T) {
	t.Parallel()
	sa := newSessionAuth("secret-token")
	tok, _ := sa.makeToken()
	// Flip last character.
	tampered := tok[:len(tok)-1] + "X"
	if sa.valid(tampered) {
		t.Error("tampered token should be invalid")
	}
}

func TestSessionAuth_DifferentKeyRejects(t *testing.T) {
	t.Parallel()
	sa1 := newSessionAuth("token-a")
	sa2 := newSessionAuth("token-b")
	tok, _ := sa1.makeToken()
	if sa2.valid(tok) {
		t.Error("token from sa1 must not validate against sa2")
	}
}

func TestSessionAuth_LoginGet(t *testing.T) {
	t.Parallel()
	sa := newSessionAuth("my-token")
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/dashboard/login", nil)
	sa.LoginHandler(w, r)
	if w.Code != http.StatusOK {
		t.Errorf("GET login: want 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "bouine") {
		t.Error("login page should contain brand name")
	}
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
	if w.Code != http.StatusUnauthorized {
		t.Errorf("wrong token: want 401, got %d", w.Code)
	}
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
	if w.Code != http.StatusSeeOther {
		t.Errorf("correct token: want 303, got %d", w.Code)
	}
	cookie := w.Header().Get(header.SetCookie)
	if !strings.Contains(cookie, sessionCookieName) {
		t.Errorf("expected session cookie in Set-Cookie, got: %q", cookie)
	}
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
	if w.Code != http.StatusFound {
		t.Errorf("no cookie: want 302, got %d", w.Code)
	}
}

func TestSessionAuth_MiddlewarePassesValidCookie(t *testing.T) {
	t.Parallel()
	sa := newSessionAuth("tok")
	tok, err := sa.makeToken()
	if err != nil {
		t.Fatalf("makeToken: %v", err)
	}

	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/dashboard/", nil)
	r.AddCookie(&http.Cookie{Name: sessionCookieName, Value: tok})
	sa.Middleware(next).ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Errorf("valid cookie: want 200, got %d", w.Code)
	}
}
