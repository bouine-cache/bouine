// Package cloudflare provides a Cloudflare Cache API client for bouine's
// invalidation propagation feature. When bouine receives a purge, ban, or
// refresh operation it forwards the equivalent invalidation to Cloudflare
// so both the local cache and the downstream CDN stay in sync.
//
// The retry strategy, rate-limit handling, and error classification are
// modelled after cache-lifecycle (github.com/backmarket/cache-lifecycle).
package cloudflare

import (
	"context"
	"time"

	"github.com/cloudflare/cloudflare-go/v4"
	"github.com/cloudflare/cloudflare-go/v4/cache"
	"github.com/cloudflare/cloudflare-go/v4/option"

	"github.com/bouine-cache/bouine/internal/observability/tracing"
)

const defaultTimeout = 10 * time.Second

// CachePurger is the Cloudflare SDK cache-purge interface.
// Defined as an interface so tests can inject a fake without network access.
type CachePurger interface {
	Purge(ctx context.Context, params cache.CachePurgeParams, opts ...option.RequestOption) (*cache.CachePurgeResponse, error)
}

// Invalidator is the interface the rest of bouine uses.
// A nil *Client satisfies this interface with no-ops, so the integration
// can be disabled by leaving the pointer nil.
type Invalidator interface {
	PurgeURLs(ctx context.Context, urls []string) error
	PurgeTags(ctx context.Context, tags []string) error
	PurgePrefixes(ctx context.Context, prefixes []string) error
	PurgeHosts(ctx context.Context, hosts []string) error
}

// Client wraps the Cloudflare cache purge API with retry logic.
//
// Stable.
type Client struct {
	purger     CachePurger
	zoneID     string
	timeout    time.Duration
	retryDelay time.Duration
}

// Config carries the credentials and behaviour knobs for the Cloudflare client.
type Config struct {
	ZoneID     string
	APIToken   string
	Timeout    time.Duration
	RetryDelay time.Duration // 0 = defaultRetryDelay
}

// New creates a Client from Config. Returns an error if credentials are missing.
func New(cfg Config) (*Client, error) {
	if cfg.ZoneID == "" {
		return nil, &ZoneConfigError{Msg: "cloudflare: zone_id must not be empty"}
	}
	if cfg.APIToken == "" {
		return nil, &ZoneConfigError{Msg: "cloudflare: api_token must not be empty"}
	}

	sdkClient := cloudflare.NewClient(option.WithAPIToken(cfg.APIToken))

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	rd := cfg.RetryDelay
	if rd <= 0 {
		rd = defaultRetryDelay
	}

	return &Client{
		purger:     sdkClient.Cache,
		zoneID:     cfg.ZoneID,
		timeout:    timeout,
		retryDelay: rd,
	}, nil
}

// NewWithPurger creates a Client with a custom CachePurger (test helper).
func NewWithPurger(purger CachePurger, zoneID string, retryDelay time.Duration) *Client {
	return &Client{
		purger:     purger,
		zoneID:     zoneID,
		timeout:    defaultTimeout,
		retryDelay: retryDelay,
	}
}

// ZoneID returns the configured Cloudflare zone ID (non-secret, safe to log).
func (c *Client) ZoneID() string {
	if c == nil {
		return ""
	}
	return c.zoneID
}

// PurgeURLs invalidates exact URLs (≤30 per CF API call).
func (c *Client) PurgeURLs(ctx context.Context, urls []string) error {
	if c == nil || len(urls) == 0 {
		return nil
	}
	ctx, span := tracing.StartSpan(ctx, "bouine.cloudflare.purge_urls")
	defer span.End()
	return c.doPurge(ctx, &cache.CachePurgeParamsBodyCachePurgeSingleFile{
		Files: cloudflare.F(urls),
	})
}

// PurgeTags invalidates all objects carrying the given cache tags.
func (c *Client) PurgeTags(ctx context.Context, tags []string) error {
	if c == nil || len(tags) == 0 {
		return nil
	}
	ctx, span := tracing.StartSpan(ctx, "bouine.cloudflare.purge_tags")
	defer span.End()
	return c.doPurge(ctx, &cache.CachePurgeParamsBodyCachePurgeFlexPurgeByTags{
		Tags: cloudflare.F(tags),
	})
}

// PurgePrefixes invalidates all objects whose URL starts with one of the
// given prefixes (e.g. "/api/v1/products/").
func (c *Client) PurgePrefixes(ctx context.Context, prefixes []string) error {
	if c == nil || len(prefixes) == 0 {
		return nil
	}
	ctx, span := tracing.StartSpan(ctx, "bouine.cloudflare.purge_prefixes")
	defer span.End()
	return c.doPurge(ctx, &cache.CachePurgeParamsBodyCachePurgeFlexPurgeByPrefixes{
		Prefixes: cloudflare.F(prefixes),
	})
}

// PurgeHosts invalidates all objects served under the given hostnames.
func (c *Client) PurgeHosts(ctx context.Context, hosts []string) error {
	if c == nil || len(hosts) == 0 {
		return nil
	}
	ctx, span := tracing.StartSpan(ctx, "bouine.cloudflare.purge_hosts")
	defer span.End()
	return c.doPurge(ctx, &cache.CachePurgeParamsBodyCachePurgeFlexPurgeByHostnames{
		Hosts: cloudflare.F(hosts),
	})
}

func (c *Client) doPurge(ctx context.Context, body cache.CachePurgeParamsBodyUnion) error {
	decision := firstTry(c.retryDelay)
	for decision.shouldTry {
		if decision.delay > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(decision.delay):
			}
		}
		callCtx, cancel := context.WithTimeout(ctx, c.timeout)
		_, err := c.purger.Purge(callCtx, cache.CachePurgeParams{
			ZoneID: cloudflare.F(c.zoneID),
			Body:   body,
		})
		cancel()
		if err == nil {
			return nil
		}
		decision = decision.next(err)
	}
	return decision.finalError
}

// Ensure *Client satisfies Invalidator at compile time.
var _ Invalidator = (*Client)(nil)
