package dashboard

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

func TestSessionAuth_SignAndValidate(t *testing.T) {
	t.Parallel()
	sa := newSessionAuth("secret-token")

	tok, err := sa.makeToken()
	require.NoError(t, err, "makeToken")
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
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/login")
	sa.LoginHandler(ctx)
	assert.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
	assert.Contains(t, string(ctx.Response.Body()), "bouine")
}

func TestSessionAuth_LoginPostWrongToken(t *testing.T) {
	t.Parallel()
	sa := newSessionAuth("correct-token")
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/login")
	ctx.Request.Header.SetContentType("application/x-www-form-urlencoded")
	ctx.PostArgs().Set("token", "wrong")
	sa.LoginHandler(ctx)
	assert.Equal(t, fasthttp.StatusUnauthorized, ctx.Response.StatusCode())
}

func TestSessionAuth_LoginPostCorrectToken(t *testing.T) {
	t.Parallel()
	sa := newSessionAuth("correct-token")
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("http://test/dashboard/login")
	ctx.Request.Header.SetContentType("application/x-www-form-urlencoded")
	ctx.PostArgs().Set("token", "correct-token")
	sa.LoginHandler(ctx)
	assert.Equal(t, fasthttp.StatusSeeOther, ctx.Response.StatusCode())
	cookie := string(ctx.Response.Header.Peek("Set-Cookie"))
	assert.Contains(t, cookie, sessionCookieName)
}

func TestSessionAuth_MiddlewareRedirectsWithoutCookie(t *testing.T) {
	t.Parallel()
	sa := newSessionAuth("tok")
	next := func(ctx *fasthttp.RequestCtx) { ctx.SetStatusCode(fasthttp.StatusOK) }
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/")
	sa.Middleware(next)(ctx)
	assert.Equal(t, fasthttp.StatusFound, ctx.Response.StatusCode())
}

func TestSessionAuth_MiddlewarePassesValidCookie(t *testing.T) {
	t.Parallel()
	sa := newSessionAuth("tok")
	tok, err := sa.makeToken()
	require.NoError(t, err, "makeToken")

	next := func(ctx *fasthttp.RequestCtx) { ctx.SetStatusCode(fasthttp.StatusOK) }
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://test/dashboard/")
	ctx.Request.Header.SetCookie(sessionCookieName, tok)
	sa.Middleware(next)(ctx)
	assert.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
}
