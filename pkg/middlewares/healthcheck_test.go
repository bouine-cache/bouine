package middlewares

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/proxy"
	"github.com/gofiber/fiber/v2/utils"
)

func Test_Healthcheck_Without_Proxy_middleware(t *testing.T) {
	t.Parallel()

	target, addr := createUpstreamBackendServer(
		func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusTeapot) }, t,
	)

	resp, err := target.Test(httptest.NewRequest("GET", "/", nil), 2000)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusTeapot, resp.StatusCode)

	app := fiber.New()
	app.Use(ClassicHealthcheckMiddleware(defaultConfig()))

	req := httptest.NewRequest("GET", "/", nil)
	req.Host = addr
	resp, err = app.Test(req)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusNotFound, resp.StatusCode)
}

func Test_Healthcheck_Classical_Healthy(t *testing.T) {
	t.Parallel()

	target, addr := createUpstreamBackendServer(
		func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusTeapot) }, t,
	)

	resp, err := target.Test(httptest.NewRequest("GET", "/", nil), -1)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusTeapot, resp.StatusCode)

	app := fiber.New()
	config := defaultConfig()
	config.Period = 70 * time.Millisecond
	app.Use(ClassicHealthcheckMiddleware(config))
	app.Use(ProxyMiddleware(proxy.Config{Servers: []string{addr}}))

	// First request creates the upstream entry
	resp, err = app.Test(httptest.NewRequest("GET", "/", nil), -1)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusTeapot, resp.StatusCode)

	// Fire another request with upstream tagged as unhealthy (unreachable)
	resp, err = app.Test(httptest.NewRequest("GET", "/", nil), -1)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusTeapot, resp.StatusCode)
}

func Test_Healthcheck_Classical_Unhealthy(t *testing.T) {
	t.Parallel()

	target, addr := createUpstreamBackendServer(
		func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusTeapot) }, t,
	)

	resp, err := target.Test(httptest.NewRequest("GET", "/", nil), -1)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusTeapot, resp.StatusCode)

	app := fiber.New()
	config := defaultConfig()
	config.Period = 70 * time.Millisecond
	app.Use(ClassicHealthcheckMiddleware(config))
	app.Use(ProxyMiddleware(proxy.Config{Servers: []string{addr}}))

	// First request creates the upstream entry
	resp, err = app.Test(httptest.NewRequest("GET", "/", nil), -1)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusTeapot, resp.StatusCode)

	// wait past the healthcheck period & probe timeout
	time.Sleep(1200 * time.Millisecond)

	// Fire another request with upstream tagged as unhealthy (unreachable)
	resp, err = app.Test(httptest.NewRequest("GET", "/", nil), -1)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusServiceUnavailable, resp.StatusCode)
	utils.AssertEqual(t, "unavailable", resp.Header.Get("Cache-Upstream-Status"))
}

func Test_Healthcheck_Smart_Healthy(t *testing.T) {
	t.Parallel()

	target, addr := createUpstreamBackendServer(
		func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusTeapot) }, t,
	)

	resp, err := target.Test(httptest.NewRequest("GET", "/", nil), -1)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusTeapot, resp.StatusCode)

	app := fiber.New()
	config := defaultConfig()
	config.Period = 70 * time.Millisecond
	app.Use(SmartHealthcheckMiddleware(config))
	app.Use(ProxyMiddleware(proxy.Config{Servers: []string{addr}}))

	// First request creates the upstream entry
	resp, err = app.Test(httptest.NewRequest("GET", "/", nil), -1)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusTeapot, resp.StatusCode)

	// Fire another request with upstream tagged as unhealthy (unreachable)
	resp, err = app.Test(httptest.NewRequest("GET", "/", nil), -1)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusTeapot, resp.StatusCode)
}

func Test_Healthcheck_SingleUpstream_Smart_Unhealthy(t *testing.T) {
	t.Parallel()

	target, addr := createUpstreamBackendServer(
		func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusTeapot) }, t,
	)

	resp, err := target.Test(httptest.NewRequest("GET", "/", nil), -1)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusTeapot, resp.StatusCode)

	app := fiber.New()
	config := defaultConfig()
	config.Period = 70 * time.Millisecond
	app.Use(SmartHealthcheckMiddleware(config))
	app.Use(ProxyMiddleware(proxy.Config{Servers: []string{addr}}))

	// First request creates the upstream entry
	resp, err = app.Test(httptest.NewRequest("GET", "/", nil), -1)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusTeapot, resp.StatusCode)

	// wait past the healthcheck period & probe timeout
	time.Sleep(1100 * time.Millisecond)

	// Fire another request with upstream tagged as unhealthy (unreachable)
	resp, err = app.Test(httptest.NewRequest("GET", "/", nil), -1)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusServiceUnavailable, resp.StatusCode)
	utils.AssertEqual(t, "unavailable", resp.Header.Get("Cache-Upstream-Status"))
}

func Test_Healthcheck_MultipleUpstreams(t *testing.T) {
	t.Parallel()
}
