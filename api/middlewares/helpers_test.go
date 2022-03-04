package middlewares

import "github.com/gofiber/fiber/v2"

// nostoreCacheControlHandler returns a 200 with 'Cache-Control: no-store'.
func nostoreCacheControlHandler(c *fiber.Ctx) error {
	c.Set("Cache-Control", "no-store")
	return c.SendStatus(200)
}

// privateCacheControlHandler returns a 200 with 'Cache-Control: no-store'.
func privateCacheControlHandler(c *fiber.Ctx) error {
	c.Set("Cache-Control", "private")
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
