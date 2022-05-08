package middlewares

import (
	"net"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/utils"
)

func createUpstreamBackendServer(handler fiber.Handler, t *testing.T) (*fiber.App, string) {
	t.Helper()

	target := fiber.New(fiber.Config{DisableStartupMessage: true})
	target.Get("/", handler)

	ln, err := net.Listen(fiber.NetworkTCP4, "127.0.0.1:0")
	utils.AssertEqual(t, nil, err)

	go func() {
		utils.AssertEqual(t, nil, target.Listener(ln))
	}()

	time.Sleep(2 * time.Second)
	addr := ln.Addr().String()

	return target, addr
}

// nostoreCacheControlHandler returns a 200 with 'Cache-Control: no-store'.
func nostoreCacheControlHandler(c *fiber.Ctx) error {
	c.Set("Cache-Control", "no-store")
	return c.SendStatus(200)
}

// privateCacheControlHandler returns a 200 with 'Cache-Control: private'.
func privateCacheControlHandler(c *fiber.Ctx) error {
	c.Set("Cache-Control", "private")
	return c.SendStatus(200)
}

// publicCacheControlHandler returns a 200 with 'Cache-Control: public'.
func publicCacheControlHandler(c *fiber.Ctx) error {
	c.Set("Cache-Control", "public")
	return c.SendStatus(200)
}

// publicMaxAgeCacheControlHandler returns a 200 with 'Cache-Control: public, max-age=1337'.
func publicMaxAgeCacheControlHandler(c *fiber.Ctx) error {
	c.Set("Cache-Control", "public, max-age=1337")
	return c.SendStatus(200)
}

// publicSMaxAgeCacheControlHandler returns a 200 with 'Cache-Control: public, s-maxage=666'.
func publicSMaxAgeCacheControlHandler(c *fiber.Ctx) error {
	c.Set("Cache-Control", "public, s-maxage=666")
	return c.SendStatus(200)
}

// okHandler returns a 200.
func okHandler(c *fiber.Ctx) error {
	c.Set("Authorization", "Basic YWxhZGRpbjpvcGVuc2VzYW1l")
	return c.SendStatus(200)
}

// badGatewayHandler returns a 502.
func badGatewayHandler(c *fiber.Ctx) error {
	return c.SendStatus(502)
}

// serviceUnavailableHandler returns a 503.
func serviceUnavailableHandler(c *fiber.Ctx) error {
	return c.SendStatus(503)
}

// gatewayTimeoutHandler returns a 504.
func gatewayTimeoutHandler(c *fiber.Ctx) error {
	return c.SendStatus(504)
}

// expiresHeaderHandler returns a 200 with an Expires header (holding a future date value).
func expiresHeaderHandler(c *fiber.Ctx) error {
	c.Set("Expires", "Thu, 01 Jan 2043 16:00:00 GMT")
	return c.SendStatus(fiber.StatusOK)
}
