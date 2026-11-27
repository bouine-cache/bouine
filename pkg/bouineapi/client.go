package bouineapi

import (
	"context"
	jsonv2 "encoding/json/v2"
	"fmt"
	"strings"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// defaultClientTimeout is applied to the client constructed by New.
const defaultClientTimeout = 10 * time.Second

// maxErrorBody caps how many bytes of an error response body are read
// and embedded in the returned error.
const maxErrorBody = 4096

// RoundTripper performs one admin API request against a transport
// other than the default direct-HTTP one (e.g. the Kubernetes
// API-server services/proxy subresource, issue #953). It exists so
// the CLI can swap transports without the SDK taking a dependency on
// kubeconfig handling.
//
// Stable.
type RoundTripper interface {
	Do(ctx context.Context, method, path string, body []byte, bearerToken string) (status int, respBody []byte, err error)
}

// Client is the bouine admin API client.
//
// Stable.
type Client struct {
	// HTTPClient is the underlying fasthttp client. If nil, a default
	// client with a 10s timeout is used.
	HTTPClient *fasthttp.Client
	// Transport, when non-nil, replaces the default fasthttp exchange.
	// BaseURL is then only informational.
	Transport RoundTripper
	// BaseURL is the admin server base URL (e.g. "http://127.0.0.1:9000").
	BaseURL string
	// Token is the optional bearer token for admin authentication.
	Token string
}

// New creates a Client with the given base URL. The returned client
// uses a fresh *fasthttp.Client with a 10s read timeout. A bare
// host:port (no scheme) is normalized to http://.
func New(baseURL string) *Client {
	if !strings.Contains(baseURL, "://") {
		baseURL = "http://" + baseURL
	}
	return &Client{
		BaseURL: strings.TrimSuffix(baseURL, "/"),
		HTTPClient: &fasthttp.Client{
			ReadTimeout:  defaultClientTimeout,
			WriteTimeout: defaultClientTimeout,
		},
	}
}

// WithToken returns a copy of the client with the given bearer token.
func (c *Client) WithToken(token string) *Client {
	cc := *c
	cc.Token = token
	return &cc
}

func (c *Client) httpClient() *fasthttp.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &fasthttp.Client{
		ReadTimeout:  defaultClientTimeout,
		WriteTimeout: defaultClientTimeout,
	}
}

// Healthz checks the health endpoint.
func (c *Client) Healthz(ctx context.Context) (*api.HealthStatus, error) {
	var out api.HealthStatus
	if err := c.get(ctx, "/healthz", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Readyz checks the readiness endpoint.
func (c *Client) Readyz(ctx context.Context) (*api.HealthStatus, error) {
	var out api.HealthStatus
	if err := c.get(ctx, "/readyz", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Version returns the server version.
func (c *Client) Version(ctx context.Context) (*api.VersionInfo, error) {
	var out api.VersionInfo
	if err := c.get(ctx, "/version", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Peers returns the cluster peer list.
func (c *Client) Peers(ctx context.Context) ([]api.PeerInfo, error) {
	var out []api.PeerInfo
	if err := c.get(ctx, "/v1/cluster/peers", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Stats returns storage stats.
func (c *Client) Stats(ctx context.Context) (*api.Stats, error) {
	var out api.Stats
	if err := c.get(ctx, "/v1/stats", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// PurgeResult is the response from a purge operation.
type PurgeResult struct {
	Status string `json:"status"`
}

// Purge invalidates a URL from the cache.
func (c *Client) Purge(ctx context.Context, url string) (*PurgeResult, error) {
	body := map[string]string{"url": url}
	var out PurgeResult
	if err := c.post(ctx, "/v1/purge", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// BatchPurgeResult is the response from a batch purge operation.
type BatchPurgeResult struct {
	Status string `json:"status"`
	Count  int    `json:"count"`
	Failed int    `json:"failed"`
}

// BatchPurge invalidates multiple URLs in a single request.
func (c *Client) BatchPurge(ctx context.Context, urls []string) (*BatchPurgeResult, error) {
	body := map[string][]string{"urls": urls}
	var out BatchPurgeResult
	if err := c.post(ctx, "/v1/purge/batch", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// AuthCheck verifies that the admin token is valid and the server is
// reachable. Returns nil on success, an error otherwise.
func (c *Client) AuthCheck(ctx context.Context) error {
	var out map[string]string
	return c.get(ctx, "/v1/auth/check", &out)
}

// BanResult is the response from a ban operation.
type BanResult struct {
	Status string `json:"status"`
	Count  int    `json:"count"`
}

// Ban issues a predicate-based cache invalidation.
func (c *Client) Ban(ctx context.Context, expr api.BanExpr) (*BanResult, error) {
	var out BanResult
	if err := c.post(ctx, "/v1/ban", expr, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RefreshResult is the response from a refresh operation.
type RefreshResult struct {
	Status string `json:"status"`
}

// Refresh performs a soft-purge.
func (c *Client) Refresh(ctx context.Context, url string) (*RefreshResult, error) {
	body := map[string]string{"url": url}
	var out RefreshResult
	if err := c.post(ctx, "/v1/refresh", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RefreshSurrogateResult is the response from a surrogate-key refresh.
type RefreshSurrogateResult struct {
	Status string `json:"status"`
	// Count is how many locally stored keys were soft-purged.
	Count int `json:"count"`
}

// RefreshSurrogate soft-purges every cached object carrying the given
// surrogate key (grouped invalidation by tag, e.g. Surrogate-Key /
// Cache-Tag).
func (c *Client) RefreshSurrogate(ctx context.Context, surrogateKey string) (*RefreshSurrogateResult, error) {
	body := map[string]string{"surrogate_key": surrogateKey}
	var out RefreshSurrogateResult
	if err := c.post(ctx, "/v1/refresh", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) get(ctx context.Context, path string, out any) error {
	return c.exchange(ctx, "GET", path, nil, out)
}

func (c *Client) post(ctx context.Context, path string, body, out any) error {
	var b []byte
	if body != nil {
		var err error
		if b, err = jsonv2.Marshal(body); err != nil {
			return err
		}
	}
	return c.exchange(ctx, "POST", path, b, out)
}

// exchangeResult carries the response across the exchange goroutine:
// a status, a private copy of the body, or an error. The fasthttp
// objects never leave the goroutine.
type exchangeResult struct { //nolint:govet // fieldalignment: cold-path CLI envelope; order follows the status-then-body-then-error reading order
	status int
	body   []byte
	err    error
}

// exchange runs one admin API request. fasthttp in the pinned
// version has no context-aware Do, so the wire exchange runs on a
// goroutine and the caller selects on ctx.Done: cancellation (Ctrl-C
// during `bouine status --watch`) returns promptly instead of
// waiting out the client timeout. The abandoned exchange is bounded
// by the client's ReadTimeout and the goroutine owns every pooled
// object, so nothing is released under the winner's feet.
func (c *Client) exchange(ctx context.Context, method, path string, body []byte, out any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.Transport != nil {
		return c.exchangeViaTransport(ctx, method, path, body, out)
	}
	done := make(chan exchangeResult, 1)
	go func() {
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)

		req.SetRequestURI(c.BaseURL + path)
		req.Header.SetMethod(method)
		if body != nil {
			req.Header.SetContentType("application/json")
			req.SetBody(body)
		}
		c.setAuth(req)

		hc := c.httpClient()
		var err error
		if deadline, ok := ctx.Deadline(); ok {
			err = hc.DoDeadline(req, resp, deadline)
		} else if hc.ReadTimeout > 0 {
			err = hc.DoTimeout(req, resp, hc.ReadTimeout)
		} else {
			err = hc.Do(req, resp)
		}
		r := exchangeResult{err: err}
		if err == nil {
			r.status = resp.StatusCode()
			r.body = append([]byte(nil), resp.Body()...)
		}
		done <- r
	}()
	select {
	case r := <-done:
		if r.err != nil {
			return r.err
		}
		if r.status < 200 || r.status >= 300 {
			return fmt.Errorf("bouineapi: %s %s: status %d: %s",
				method, path, r.status, sanitizeErrorBody(r.body))
		}
		if out != nil && len(r.body) > 0 {
			return jsonv2.Unmarshal(r.body, out)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// exchangeViaTransport handles one request through a custom RoundTripper
// (e.g. the kube-apiserver proxy) with the same status/error semantics
// as the default fasthttp path.
func (c *Client) exchangeViaTransport(ctx context.Context, method, path string, body []byte, out any) error {
	status, respBody, err := c.Transport.Do(ctx, method, path, body, c.Token)
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("bouineapi: %s %s: status %d: %s",
			method, path, status, sanitizeErrorBody(respBody))
	}
	if out != nil && len(respBody) > 0 {
		return jsonv2.Unmarshal(respBody, out)
	}
	return nil
}

func (c *Client) setAuth(req *fasthttp.Request) {
	if c.Token != "" {
		req.Header.Set(header.Authorization, "Bearer "+c.Token)
	}
}

// sanitizeErrorBody truncates the body to maxErrorBody bytes and strips
// control characters.
func sanitizeErrorBody(body []byte) string {
	truncated := false
	if len(body) > maxErrorBody {
		body = body[:maxErrorBody]
		truncated = true
	}
	s := strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, string(body))
	if truncated {
		s += " … (truncated)"
	}
	return s
}
