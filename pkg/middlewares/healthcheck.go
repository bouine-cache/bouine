// Copyright 2022 Théotime Levêque
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package middlewares

import (
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
)

type Config struct {
	Period          time.Duration
	Logger          *zap.Logger
	Upstreams       *Upstreams
	HealthcheckKind int
	Client          *fasthttp.Client
}

func defaultConfig(config ...Config) Config {
	if len(config) < 1 {
		return configDefault
	}

	cfg := config[0]
	cfg.Logger = configDefault.Logger

	if time.Duration(cfg.Period.Milliseconds()) == 0 {
		cfg.Period = configDefault.Period
	}
	if cfg.HealthcheckKind == 0 {
		cfg.HealthcheckKind = configDefault.HealthcheckKind
	}
	if cfg.Client == nil {
		cfg.Client = &fasthttp.Client{
			ReadTimeout:                   1 * time.Second,
			WriteTimeout:                  1 * time.Second,
			MaxIdleConnDuration:           1 * time.Hour,
			NoDefaultUserAgentHeader:      true, // Don't send: User-Agent: fasthttp
			DisableHeaderNamesNormalizing: true, // If you set the case on your headers correctly you can enable this
			DisablePathNormalizing:        true,
			// increase DNS cache time to an hour instead of default minute
			Dial: (&fasthttp.TCPDialer{
				Concurrency:      4096,
				DNSCacheDuration: time.Hour,
			}).Dial,
		}
	}

	return cfg
}

var (
	configDefault = Config{
		Period:          10 * time.Second,
		Logger:          zap.NewExample(),
		Upstreams:       &Upstreams{entries: make(map[string]*upstream)},
		HealthcheckKind: smartHealthcheckKind,
		Client:          &fasthttp.Client{},
	}
)

const smartHealthcheckKind = 1
const classicHealthcheckKind = 2

// SmartHealthcheckMiddleware returns a bouine smarthealthcheck middleware.
// This middleware sends healthchecks to the upstream only when the given period
// elapsed without any requests being proxied.
func SmartHealthcheckMiddleware(config Config) fiber.Handler {
	config.HealthcheckKind = smartHealthcheckKind
	return healthcheck(config)
}

// ClassicHealthcheckMiddleware returns a bouine ClassicHealthcheck middleware.
// This healthcheck middlewares sends periodically a request to a given
// upstream healthcheck endpoint.
func ClassicHealthcheckMiddleware(config Config) fiber.Handler {
	config.HealthcheckKind = classicHealthcheckKind
	return healthcheck(config)
}

func healthcheck(config ...Config) fiber.Handler {
	cfg := defaultConfig(config...)
	upstreamC := make(chan string)

	go func() {
		for host := range upstreamC {
			cfg.Upstreams.Set(
				host, upstream{Ticker: time.NewTicker(cfg.Period), Healthy: true},
			)

			// Start a goroutine waiting for upstream ticks
			go func(host string) {
				for range cfg.Upstreams.Get(host).Ticker.C {
					// send healthcheck
					status, err := checkHealth(&cfg, host)
					if err != nil {
						cfg.Logger.Error("healthcheck err",
							zap.String("component", "healthcheck-middleware"),
							zap.String("upstream", host),
							zap.Error(err),
						)
					}

					// Change upstream health status
					entry := cfg.Upstreams.Get(host)
					entry.Healthy = status
					cfg.Upstreams.Set(host, *entry)
				}
			}(host)
		}
	}()

	// Return new handler
	return func(c *fiber.Ctx) error {
		host := string(c.Request().Host())

		// Register potential new upstream
		if !cfg.Upstreams.Contains(host) {
			cfg.Logger.Debug("New upstream",
				zap.String("component", "healthcheck-middleware"),
				zap.String("host", host),
			)

			upstreamC <- host
		} else if up := cfg.Upstreams.Get(host); !up.Healthy {
			// Prevent request to saturate unhealthy upstream
			_ = c.SendStatus(503)
			c.Response().Header.Add("Upstream-Status", "unavailable")
			return nil
		}

		// smartMode: Reset upstream ticker
		if cfg.HealthcheckKind == smartHealthcheckKind {
			cfg.Upstreams.Get(host).Ticker.Reset(cfg.Period)
		}

		// Continue down the middleware chain, return err to Fiber if exist
		return c.Next()
	}
}

func checkHealth(cfg *Config, host string) (bool, error) {
	// Fetch upstream status
	req := fasthttp.AcquireRequest()
	req.SetRequestURI(fmt.Sprintf("http://%s/healthz", host))
	req.Header.SetMethod(fasthttp.MethodGet)
	resp := fasthttp.AcquireResponse()
	err := cfg.Client.Do(req, resp)
	fasthttp.ReleaseRequest(req)
	if err != nil {
		return false, err
	}

	return resp.StatusCode() == 200, nil
}
