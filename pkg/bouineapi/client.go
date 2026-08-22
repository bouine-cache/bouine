package bouineapi

import (
	"bytes"
	"context"
	"encoding/json"
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

// Client is the bouine admin API client.
//
// Stable.
type Client struct {
	// BaseURL is the admin server base URL (e.g. "http://127.0.0.1:9000").
	BaseURL string
	// Token is the optional bearer token for admin authentication.
	Token string
	// HTTPClient is the underlying fasthttp client. If nil, a default
	// client with a 10s timeout is used.
	HTTPClient *fasthttp.Client
}

// New creates a Client with the given base URL. The returned client
// uses a fresh *fasthttp.Client with a 10s read timeout.
func New(baseURL string) *Client {
	return &Client{
		BaseURL: baseURL,
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

func (c *Client) get(ctx context.Context, path string, out any) error {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(c.BaseURL + path)
	req.Header.SetMethod("GET")
	c.setAuth(req)

	return c.doJSON(ctx, req, resp, "GET", path, out)
}

func (c *Client) post(ctx context.Context, path string, body, out any) error {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(c.BaseURL + path)
	req.Header.SetMethod("POST")
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		req.Header.SetContentType("application/json")
		req.SetBody(b)
	}
	c.setAuth(req)

	return c.doJSON(ctx, req, resp, "POST", path, out)
}

func (c *Client) setAuth(req *fasthttp.Request) {
	if c.Token != "" {
		req.Header.Set(header.Authorization, "Bearer "+c.Token)
	}
}

func (c *Client) doJSON(ctx context.Context, req *fasthttp.Request, resp *fasthttp.Response, method, path string, out any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline, ok := ctx.Deadline()
	if ok {
		if err := c.httpClient().DoDeadline(req, resp, deadline); err != nil {
			return err
		}
	} else {
		// Use the client's ReadTimeout as the default timeout to avoid
		// hanging indefinitely on unresponsive servers.
		hc := c.httpClient()
		if hc.ReadTimeout > 0 {
			if err := hc.DoTimeout(req, resp, hc.ReadTimeout); err != nil {
				return err
			}
		} else {
			if err := hc.Do(req, resp); err != nil {
				return err
			}
		}
	}

	statusCode := resp.StatusCode()
	if statusCode < 200 || statusCode >= 300 {
		body := resp.Body()
		return fmt.Errorf("bouineapi: %s %s: status %d: %s",
			method, path, statusCode, sanitizeErrorBody(body))
	}

	respBody := resp.Body()
	if out != nil && len(respBody) > 0 {
		return json.Unmarshal(respBody, out)
	}
	return nil
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

var _ = bytes.NewReader
