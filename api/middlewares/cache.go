package middlewares

import (
	"bytes"

	"github.com/gofiber/fiber/v2"
)

// CacheSkippable returns true if proxied responses shouldn't be cached according to RFC7234 (Bouine default configuration).
func CacheSkippable(c *fiber.Ctx) bool {
	// skip cache middleware on res.statusCode == 500
	if c.Context().Response.StatusCode() == fiber.StatusInternalServerError {
		return true
	}

	// skip cache middleware on 'Cache-Control: no-store'
	if bytes.Equal(c.Context().Response.Header.Peek("Cache-Control"), []byte("no-store")) {
		return true
	}

	// skip cache middleware on 'Cache-Control: no-store'
	if bytes.Equal(c.Context().Response.Header.Peek("Cache-Control"), []byte("no-store")) {
		return true
	}

	// skip cache middleware on 'Cache-Control: private'
	if bytes.Equal(c.Context().Response.Header.Peek("Cache-Control"), []byte("private")) {
		return true
	}

	// FIXME: skip cache middleware on Basic Auth requests without 'Cache-Control: public'
	return false
}
