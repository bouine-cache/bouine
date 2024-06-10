package upstream

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	fiber "github.com/thylong/fiber/v2"
	"github.com/thylong/fiber/v2/middleware/proxy"
	"github.com/thylong/fiber/v2/utils"
)

func Test_Proxy_WithValidUpstream(t *testing.T) {
	t.Parallel()

	target, addr := createUpstreamBackendServer(
		func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusTeapot) }, t,
	)

	resp, err := target.Test(httptest.NewRequest("GET", "/", nil), 2000)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusTeapot, resp.StatusCode)

	app := fiber.New(fiber.Config{DisableStartupMessage: true})

	app.Use(ProxyMiddleware(proxy.Config{Servers: []string{addr}}))

	req := httptest.NewRequest("GET", "/", nil)
	req.Host = addr
	resp, err = app.Test(req)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusTeapot, resp.StatusCode)
}

func Test_Proxy_WithoutValidUpstream(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r != nil {
			utils.AssertEqual(t, "Servers cannot be empty", r)
		}
	}()
	app := fiber.New()
	app.Use(ProxyMiddleware(proxy.Config{Servers: []string{}}))
}

func Test_Proxy_SlowUpstream(t *testing.T) {
	t.Parallel()

	_, addr := createUpstreamBackendServer(func(c *fiber.Ctx) error {
		time.Sleep(2 * time.Second)
		return c.SendString("fiber is awesome")
	}, t)

	app := fiber.New()
	app.Use(ProxyMiddleware(proxy.Config{
		Servers: []string{addr},
		Timeout: 3 * time.Second,
	}))

	resp, err := app.Test(httptest.NewRequest("GET", "/", nil), 5000)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusOK, resp.StatusCode)

	b, err := io.ReadAll(resp.Body)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, "fiber is awesome", string(b))
}

func Test_Proxy_With_Timeout(t *testing.T) {
	t.Parallel()

	_, addr := createUpstreamBackendServer(func(c *fiber.Ctx) error {
		time.Sleep(1 * time.Second)
		return c.SendString("fiber is awesome")
	}, t)

	app := fiber.New()
	app.Use(ProxyMiddleware(proxy.Config{
		Servers: []string{addr},
		Timeout: 100 * time.Millisecond,
	}))

	resp, err := app.Test(httptest.NewRequest("GET", "/", nil), 2000)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusInternalServerError, resp.StatusCode)

	b, err := io.ReadAll(resp.Body)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, "timeout", string(b))
}

func Test_Proxy_Buffer_Size_Response(t *testing.T) {
	t.Parallel()

	_, addr := createUpstreamBackendServer(func(c *fiber.Ctx) error {
		long := strings.Join(make([]string, 5000), "-")
		c.Set("Very-Long-Header", long)
		return c.SendString("ok")
	}, t)

	app := fiber.New()
	app.Use(ProxyMiddleware(proxy.Config{Servers: []string{addr}}))

	resp, err := app.Test(httptest.NewRequest("GET", "/", nil))
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusInternalServerError, resp.StatusCode)

	app = fiber.New()
	app.Use(ProxyMiddleware(proxy.Config{
		Servers:        []string{addr},
		ReadBufferSize: 1024 * 8,
	}))

	resp, err = app.Test(httptest.NewRequest("GET", "/", nil))
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusOK, resp.StatusCode)
}

func Test_Proxy_SanitizedRequestFromUpstream(t *testing.T) {
	t.Parallel()

	// _, addr := createUpstreamBackendServer(func(c *fiber.Ctx) error {
	// 	b := c.Request().Body()
	// 	return c.SendString(string(b))
	// }, t)

	// app := fiber.New()
	// app.Use(proxy.Balancer(proxy.Config{
	// 	Servers: []string{addr},
	// 	ModifyRequest: func(c *fiber.Ctx) error {
	// 		c.Request().SetBody([]byte("modified request"))
	// 		return nil
	// 	},
	// }))

	// resp, err := app.Test(httptest.NewRequest("GET", "/", nil))
	// utils.AssertEqual(t, nil, err)
	// utils.AssertEqual(t, fiber.StatusOK, resp.StatusCode)

	// b, err := io.ReadAll(resp.Body)
	// utils.AssertEqual(t, nil, err)
	// utils.AssertEqual(t, "modified request", string(b))
}

func Test_Proxy_SanitizedResponseFromUpstream(t *testing.T) {
	t.Parallel()

	// _, addr := createUpstreamBackendServer(func(c *fiber.Ctx) error {
	// 	return c.Status(500).SendString("not modified")
	// }, t)

	// app := fiber.New()
	// app.Use(proxy.Balancer(proxy.Config{
	// 	Servers: []string{addr},
	// 	ModifyResponse: func(c *fiber.Ctx) error {
	// 		c.Response().SetStatusCode(fiber.StatusOK)
	// 		return c.SendString("modified response")
	// 	},
	// }))

	// resp, err := app.Test(httptest.NewRequest("GET", "/", nil))
	// utils.AssertEqual(t, nil, err)
	// utils.AssertEqual(t, fiber.StatusOK, resp.StatusCode)

	// b, err := io.ReadAll(resp.Body)
	// utils.AssertEqual(t, nil, err)
	// utils.AssertEqual(t, "modified response", string(b))
}
