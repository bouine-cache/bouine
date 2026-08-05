package dashboard

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"time"

	"github.com/bouine-cache/bouine/internal/dashboard/templates"
	"github.com/bouine-cache/bouine/pkg/header"
)

const (
	sessionCookieName = "bouine_session"
	sessionMaxAge     = 24 * 60 * 60 // 24h in seconds
)

// sessionAuth handles login and session cookie validation.
type sessionAuth struct {
	token   string
	hmacKey []byte
}

func newSessionAuth(adminToken string) *sessionAuth {
	// Derive HMAC key from the admin token.
	h := sha256.Sum256([]byte("bouine-session-v1:" + adminToken))
	return &sessionAuth{
		token:   adminToken,
		hmacKey: h[:],
	}
}

func (sa *sessionAuth) sign(value string) string {
	mac := hmac.New(sha256.New, sa.hmacKey)
	mac.Write([]byte(value))
	return hex.EncodeToString(mac.Sum(nil))
}

func (sa *sessionAuth) makeToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	nonce := hex.EncodeToString(b)
	return nonce + "." + sa.sign(nonce), nil
}

func (sa *sessionAuth) valid(cookie string) bool {
	if len(cookie) < 34 {
		return false
	}
	dot := len(cookie) - 65
	if dot < 1 || cookie[dot] != '.' {
		return false
	}
	nonce := cookie[:dot]
	sig := cookie[dot+1:]
	return hmac.Equal([]byte(sig), []byte(sa.sign(nonce)))
}

// LoginHandler renders the login form (GET) and processes it (POST).
func (sa *sessionAuth) LoginHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		r.Body = http.MaxBytesReader(w, r.Body, maxAdminFormBytes)
		submitted := r.FormValue("token")
		if !hmac.Equal([]byte(submitted), []byte(sa.token)) {
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}
		sessionToken, err := sa.makeToken()
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		http.SetCookie(w, &http.Cookie{ //nolint:gosec // Secure omitted: admin port may be HTTP in dev; operators add TLS terminator
			Name:     sessionCookieName,
			Value:    sessionToken,
			MaxAge:   sessionMaxAge,
			HttpOnly: true,
			SameSite: http.SameSiteStrictMode,
			Path:     "/dashboard",
		})
		http.Redirect(w, r, "/dashboard/", http.StatusSeeOther)
		return
	}
	// GET — render login form.
	w.Header().Set(header.ContentType, "text/html; charset=utf-8")
	if err := templates.Login().Render(r.Context(), w); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

// Middleware protects dashboard routes with the session cookie.
func (sa *sessionAuth) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie(sessionCookieName)
		if err != nil || !sa.valid(c.Value) {
			http.Redirect(w, r, "/dashboard/login", http.StatusFound)
			return
		}
		// Slide the cookie expiry on each request.
		http.SetCookie(w, &http.Cookie{ //nolint:gosec // Secure omitted: admin port may be HTTP in dev; operators add TLS terminator
			Name:     sessionCookieName,
			Value:    c.Value,
			MaxAge:   sessionMaxAge,
			HttpOnly: true,
			SameSite: http.SameSiteStrictMode,
			Path:     "/dashboard",
			Expires:  time.Now().Add(24 * time.Hour),
		})
		next.ServeHTTP(w, r)
	})
}
