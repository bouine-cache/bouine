package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/valyala/fasthttp"
)

func TestStripPrefixFastHTTP_Strips(t *testing.T) {
	t.Parallel()
	var gotPath string
	origin := func(ctx *fasthttp.RequestCtx) {
		gotPath = string(ctx.Path())
	}
	h := stripPrefixFastHTTP("/api/v1", origin)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/api/v1/users/123")
	h(ctx)
	assert.Equal(t, "/users/123", gotPath)
}

func TestStripPrefixFastHTTP_PreservesLeadingSlash(t *testing.T) {
	t.Parallel()
	var gotPath string
	origin := func(ctx *fasthttp.RequestCtx) {
		gotPath = string(ctx.Path())
	}
	h := stripPrefixFastHTTP("/api/v1/", origin)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/api/v1/")
	h(ctx)
	assert.Equal(t, "/", gotPath)
}

func TestStripPrefixFastHTTP_NoMatchPassthrough(t *testing.T) {
	t.Parallel()
	var gotPath string
	origin := func(ctx *fasthttp.RequestCtx) {
		gotPath = string(ctx.Path())
	}
	h := stripPrefixFastHTTP("/api/v1", origin)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/other/path")
	h(ctx)
	assert.Equal(t, "/other/path", gotPath)
}

func TestStripPrefixFastHTTP_PreservesQuery(t *testing.T) {
	t.Parallel()
	var gotURI string
	origin := func(ctx *fasthttp.RequestCtx) {
		gotURI = string(ctx.RequestURI())
	}
	h := stripPrefixFastHTTP("/api/v1", origin)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/api/v1/users?q=1")
	h(ctx)
	assert.Equal(t, "/users?q=1", gotURI)
}

func TestStripPrefixFastHTTP_ExactPrefixBecomesRoot(t *testing.T) {
	t.Parallel()
	var gotURI string
	origin := func(ctx *fasthttp.RequestCtx) {
		gotURI = string(ctx.RequestURI())
	}
	h := stripPrefixFastHTTP("/api/v1", origin)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/api/v1?q=1")
	h(ctx)
	assert.Equal(t, "/?q=1", gotURI)
}
