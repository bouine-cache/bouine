package dashboard

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"time"

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
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(loginHTML)
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

var loginHTML = []byte(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>bouine · login</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Inter',system-ui,sans-serif;background:#08060f;color:#ede6ff;display:flex;align-items:center;justify-content:center;min-height:100vh}
.card{background:#110e1c;border:1px solid #1e1830;border-radius:12px;padding:2.5rem;width:340px;box-shadow:0 8px 32px rgba(0,0,0,.5)}
h1{font-size:1.1rem;font-weight:700;color:#c4b5fd;margin-bottom:.35rem}
p{font-size:.78rem;color:#6050a0;margin-bottom:1.5rem}
label{display:block;font-size:.68rem;font-weight:600;text-transform:uppercase;letter-spacing:.1em;color:#6050a0;margin-bottom:.35rem}
input{width:100%;background:#0e0b18;border:1px solid #1e1830;color:#ede6ff;padding:.55rem .75rem;border-radius:7px;font-size:.82rem;font-family:inherit;margin-bottom:1rem}
input:focus{outline:none;border-color:rgba(139,92,246,.5);box-shadow:0 0 0 2px rgba(139,92,246,.1)}
button{width:100%;padding:.55rem;border-radius:7px;border:none;background:#8b5cf6;color:#fff;font-size:.82rem;font-weight:600;cursor:pointer;font-family:inherit}
button:hover{background:#a78bfa}
.brand{font-family:'JetBrains Mono',monospace;font-size:1.3rem;font-weight:700;color:#8b5cf6;margin-bottom:1.5rem;display:flex;align-items:center;gap:.5rem}
</style>
</head>
<body>
<div class="card">
  <div class="brand">🐟 bouine</div>
  <h1>Admin dashboard</h1>
  <p>Enter your admin token to continue.</p>
  <form method="POST" action="/dashboard/login">
    <label>Token</label>
    <input type="password" name="token" placeholder="your-admin-token" autofocus required>
    <button type="submit">Sign in →</button>
  </form>
</div>
</body>
</html>`)
