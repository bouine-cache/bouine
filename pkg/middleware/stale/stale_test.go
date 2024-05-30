package stale

import (
	"net/http/httptest"
	"testing"
	"time"

	fiber "github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/proxy"
	"github.com/gofiber/fiber/v2/utils"
	"github.com/thylong/bouine/pkg/middleware/upstream"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
)

func Test_Stale_Healthy_Flow(t *testing.T) {
	t.Parallel()

	target, addr := createUpstreamBackendServer(
		func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusTeapot) }, t,
	)

	resp, err := target.Test(httptest.NewRequest("GET", "/", nil), -1)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusTeapot, resp.StatusCode)

	app := fiber.New()
	app.Use(upstream.ClassicHealthcheckMiddleware(upstream.Config{
		Period:          70 * time.Millisecond,
		Logger:          zap.NewExample(),
		Upstreams:       []string{addr},
		HealthcheckKind: upstream.SmartHealthcheckKind,
		Client:          &fasthttp.Client{},
	}))
	app.Use(upstream.ProxyMiddleware(proxy.Config{Servers: []string{addr}}))

	// First request creates the upstream entry
	resp, err = app.Test(httptest.NewRequest("GET", "/", nil), -1)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusTeapot, resp.StatusCode)

	// Fire another request with upstream tagged as unhealthy (unreachable)
	resp, err = app.Test(httptest.NewRequest("GET", "/", nil), -1)
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusTeapot, resp.StatusCode)
}
