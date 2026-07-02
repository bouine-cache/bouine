package bouineapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/thylong/bouine/pkg/api"
	"github.com/thylong/bouine/pkg/header"
)

// Client is the bouine admin API client.
//
// Stable.
type Client struct {
	// BaseURL is the admin server base URL (e.g. "http://127.0.0.1:9000").
	BaseURL string
	// Token is the optional bearer token for admin authentication.
	Token string
	// HTTPClient is the underlying HTTP client. Defaults to
	// http.DefaultClient if nil.
	HTTPClient *http.Client
}

// New creates a Client with the given base URL.
func New(baseURL string) *Client {
	return &Client{BaseURL: baseURL}
}

// WithToken returns a copy of the client with the given bearer token.
func (c *Client) WithToken(token string) *Client {
	cc := *c
	cc.Token = token
	return &cc
}

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
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
// The server caps the batch size (default 1000, configurable via
// admin.max_batch_size). Exceeding the cap returns an error.
func (c *Client) BatchPurge(ctx context.Context, urls []string) (*BatchPurgeResult, error) {
	body := map[string][]string{"urls": urls}
	var out BatchPurgeResult
	if err := c.post(ctx, "/v1/purge/batch", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// AuthCheck verifies that the admin token is valid and the server is
// reachable. Returns nil on success, an error otherwise. Useful as a
// readiness probe for invalidation wiring.
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

// Refresh performs a soft-purge: marks the URL stale so the next
// request triggers revalidation.
func (c *Client) Refresh(ctx context.Context, url string) (*RefreshResult, error) {
	body := map[string]string{"url": url}
	var out RefreshResult
	if err := c.post(ctx, "/v1/refresh", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ReloadResult is the response from a config reload.
type ReloadResult struct {
	Status string `json:"status"`
}

// Reload triggers a config reload on the server.
func (c *Client) Reload(ctx context.Context) (*ReloadResult, error) {
	var out ReloadResult
	if err := c.post(ctx, "/v1/config/reload", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return err
	}
	c.setAuth(req)
	return c.doJSON(req, out)
}

func (c *Client) post(ctx context.Context, path string, body, out any) error {
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, bodyReader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set(header.ContentType, "application/json")
	}
	c.setAuth(req)
	return c.doJSON(req, out)
}

func (c *Client) setAuth(req *http.Request) {
	if c.Token != "" {
		req.Header.Set(header.Authorization, "Bearer "+c.Token)
	}
}

func (c *Client) doJSON(req *http.Request, out any) error {
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("bouineapi: %s %s: status %d: %s",
			req.Method, req.URL.Path, resp.StatusCode, respBody)
	}

	if out != nil && len(respBody) > 0 {
		return json.Unmarshal(respBody, out)
	}
	return nil
}
