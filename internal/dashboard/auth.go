package dashboard

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/valyala/fasthttp"

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
func (sa *sessionAuth) LoginHandler(ctx *fasthttp.RequestCtx) {
	if string(ctx.Method()) == "POST" {
		if len(ctx.PostBody()) > maxAdminFormBytes {
			ctx.Error("request body too large", fasthttp.StatusRequestEntityTooLarge)
			return
		}
		args := ctx.PostArgs()
		submitted := string(args.Peek("token"))
		if !hmac.Equal([]byte(submitted), []byte(sa.token)) {
			ctx.Error("invalid token", fasthttp.StatusUnauthorized)
			return
		}
		sessionToken, err := sa.makeToken()
		if err != nil {
			ctx.Error("internal error", fasthttp.StatusInternalServerError)
			return
		}
		var c fasthttp.Cookie
		c.SetKey(sessionCookieName)
		c.SetValue(sessionToken)
		c.SetMaxAge(sessionMaxAge)
		c.SetHTTPOnly(true)
		c.SetSameSite(fasthttp.CookieSameSiteStrictMode)
		c.SetPath("/dashboard")
		ctx.Response.Header.SetCookie(&c)
		ctx.Redirect("/dashboard/", fasthttp.StatusSeeOther)
		return
	}
	ctx.Response.Header.Set(header.ContentType, "text/html; charset=utf-8")
	if err := templates.Login().Render(context.Background(), ctx); err != nil {
		ctx.Error("internal error", fasthttp.StatusInternalServerError)
	}
}

// Middleware protects dashboard routes with the session cookie.
func (sa *sessionAuth) Middleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		c := ctx.Request.Header.Cookie(sessionCookieName)
		if len(c) == 0 || !sa.valid(string(c)) {
			ctx.Redirect("/dashboard/login", fasthttp.StatusFound)
			return
		}
		var cookie fasthttp.Cookie
		cookie.SetKey(sessionCookieName)
		cookie.SetValue(string(c))
		cookie.SetMaxAge(sessionMaxAge)
		cookie.SetHTTPOnly(true)
		cookie.SetSameSite(fasthttp.CookieSameSiteStrictMode)
		cookie.SetPath("/dashboard")
		cookie.SetExpire(time.Now().Add(24 * time.Hour))
		ctx.Response.Header.SetCookie(&cookie)
		next(ctx)
	}
}
