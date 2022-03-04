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
