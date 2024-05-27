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

package middleware

import (
	"fmt"
	"time"

	fiber "github.com/gofiber/fiber/v2"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
)

type Config struct {
	Period          time.Duration
	Logger          *zap.Logger
	Upstreams       []string
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

var configDefault = Config{
	Period:          10 * time.Second,
	Logger:          zap.NewExample(),
	Upstreams:       []string{},
	HealthcheckKind: smartHealthcheckKind,
	Client:          &fasthttp.Client{},
}

const (
	smartHealthcheckKind   = 1
	classicHealthcheckKind = 2
)

// SmartHealthcheckMiddleware returns a bouine smarthealthcheck middleware.
// This middleware sends healthchecks to the upstream only when the given period
// elapsed without any requests being proxied.
func SmartHealthcheckMiddleware(config ...Config) fiber.Handler {
	cfg := defaultConfig(config...)
	cfg.HealthcheckKind = smartHealthcheckKind
	return healthcheck(cfg)
}

// ClassicHealthcheckMiddleware returns a bouine ClassicHealthcheck middleware.
// This healthcheck middlewares sends periodically a request to a given
// upstream healthcheck endpoint.
func ClassicHealthcheckMiddleware(config ...Config) fiber.Handler {
	cfg := defaultConfig(config...)
	cfg.HealthcheckKind = classicHealthcheckKind
	return healthcheck(cfg)
}

func healthcheck(cfg Config) fiber.Handler {
	upstreams := &Upstreams{entries: make(map[string]*upstream)}

	// Register upstreams
	for _, host := range cfg.Upstreams {
		cfg.Logger.Debug("New upstream",
			zap.String("component", "healthcheck-middleware"),
			zap.String("host", host),
		)
		upstreams.Set(
			host, upstream{Ticker: time.NewTicker(cfg.Period), Healthy: true},
		)
		go func(host string) {
			for range upstreams.Get(host).Ticker.C {
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
				entry := upstreams.Get(host)
				entry.Healthy = status
				upstreams.Set(host, *entry)
				cfg.Logger.Info("healthcheck health",
					zap.String("component", "healthcheck-middleware"),
					zap.String("upstream", host),
					zap.Bool("healthy", status),
				)
			}
		}(host)
	}

	// Return new handler
	return func(c *fiber.Ctx) error {
		// TODO: support multiple upstreams again
		if !upstreams.entries[cfg.Upstreams[0]].Healthy {
			// Prevent request to saturate unhealthy upstream
			_ = c.SendStatus(503)
			c.Response().Header.Add("Cache-Upstream-Status", "unavailable")
			return nil
		}

		// smartMode: Reset upstream ticker
		if cfg.HealthcheckKind == smartHealthcheckKind {
			cfg.Logger.Debug("Resetting ticker",
				zap.String("component", "healthcheck-middleware"),
				zap.String("host", cfg.Upstreams[0]),
			)
			upstreams.Get(cfg.Upstreams[0]).Ticker.Reset(cfg.Period)
		}
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
