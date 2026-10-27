package main

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/valyala/fasthttp"
)

// fakeCollector imitates an OTLP/HTTP trace receiver so tracing-enabled runs
// exercise the real export path (batcher, retries, queue) without a real
// collector. "down" reserves then releases the port: connections refuse, the
// exact production "collector down" condition.
type fakeCollector struct {
	srv     *fasthttp.Server
	addr    string
	exports atomic.Int64
	bytes   atomic.Int64
}

func startCollector(mode string) (*fakeCollector, error) {
	switch mode {
	case "ok", "slow", "down":
	default:
		return nil, fmt.Errorf("unknown collector mode %q", mode)
	}
	c := &fakeCollector{}
	ln, err := listenLocal()
	if err != nil {
		return nil, err
	}
	c.addr = ln.Addr().String()
	if mode == "down" {
		_ = ln.Close()
		return c, nil
	}
	delay := time.Duration(0)
	if mode == "slow" {
		delay = 500 * time.Millisecond
	}
	c.srv = &fasthttp.Server{Handler: func(ctx *fasthttp.RequestCtx) {
		if delay > 0 {
			time.Sleep(delay)
		}
		c.exports.Add(1)
		c.bytes.Add(int64(len(ctx.Request.Body())))
		ctx.SetStatusCode(fasthttp.StatusOK)
	}}
	go func() { _ = c.srv.Serve(ln) }()
	return c, nil
}

func (c *fakeCollector) stop() {
	if c.srv != nil {
		_ = c.srv.Shutdown()
	}
}

func collAddr(c *fakeCollector) string {
	if c == nil {
		return ""
	}
	return c.addr
}

// tracingSpec renders the tracing YAML block; rate < 0 disables tracing.
type tracingSpec struct {
	endpoint string
	rate     float64
}

func (s tracingSpec) yaml() string {
	if s.rate < 0 {
		return ""
	}
	return fmt.Sprintf(`tracing:
  endpoint: "http://%s"
  service_name: bouine-topology
  sampling_rate: %.4f
`, s.endpoint, s.rate)
}
