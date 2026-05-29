// Package prefetch implements background cache warming via Link
// rel=preload headers and sitemap crawling. It runs as a supervised
// goroutine and never competes with the data plane for resources.
//
// The prefetcher inspects origin responses for Link headers with
// rel=preload hints and schedules background fetches to warm the
// cache before a client requests the linked resource.
package prefetch

import (
	"context"
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Config configures the prefetcher.
type Config struct {
	// Handler is the HTTP handler to send prefetch requests through
	// (typically the cache handler so responses get stored).
	Handler http.Handler
	// MaxConcurrency limits simultaneous prefetch requests.
	MaxConcurrency int
	// SitemapURLs is a list of sitemap URLs to crawl periodically.
	SitemapURLs []string
	// SitemapInterval is the crawl interval. Zero disables.
	SitemapInterval time.Duration
	// Logger is the structured logger.
	Logger *slog.Logger
}

// Prefetcher warms the cache by following Link: rel=preload headers
// and crawling sitemaps.
type Prefetcher struct {
	cfg    Config
	sem    chan struct{}
	seen   sync.Map
	logger *slog.Logger
}

// New creates a Prefetcher.
func New(cfg Config) *Prefetcher {
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 4
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Prefetcher{
		cfg:    cfg,
		sem:    make(chan struct{}, cfg.MaxConcurrency),
		logger: cfg.Logger,
	}
}

// Run starts the sitemap crawler loop. Blocks until ctx is cancelled.
func (p *Prefetcher) Run(ctx context.Context) error {
	if len(p.cfg.SitemapURLs) == 0 || p.cfg.SitemapInterval <= 0 {
		<-ctx.Done()
		return nil
	}

	ticker := time.NewTicker(p.cfg.SitemapInterval)
	defer ticker.Stop()

	// Initial crawl.
	p.crawlSitemaps(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			p.crawlSitemaps(ctx)
		}
	}
}

// OnResponse inspects a response for Link: rel=preload headers and
// schedules background prefetch requests. Call this from middleware
// after the origin response is received.
func (p *Prefetcher) OnResponse(ctx context.Context, reqHost string, header http.Header) {
	links := header.Values("Link")
	for _, link := range links {
		urls := parseLinkPreload(link, reqHost)
		for _, u := range urls {
			p.scheduleWarm(ctx, u)
		}
	}
}

// Stats returns prefetch statistics.
type Stats struct {
	// Scheduled is the total number of URLs scheduled for prefetch.
	Scheduled int64
	// Fetched is the total number of successful prefetch requests.
	Fetched int64
}

func (p *Prefetcher) scheduleWarm(ctx context.Context, url string) {
	if _, loaded := p.seen.LoadOrStore(url, struct{}{}); loaded {
		return
	}

	select {
	case p.sem <- struct{}{}:
	default:
		return
	}

	go func() {
		defer func() { <-p.sem }()
		p.warmURL(ctx, url)
	}()
}

func (p *Prefetcher) warmURL(ctx context.Context, url string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		p.logger.Debug("prefetch: invalid URL", "url", url, "error", err)
		return
	}
	req.Header.Set("X-Bouine-Prefetch", "1")

	rec := &discardWriter{header: make(http.Header)}
	p.cfg.Handler.ServeHTTP(rec, req)
	p.logger.Debug("prefetch: warmed", "url", url, "status", rec.status)
}

func (p *Prefetcher) crawlSitemaps(ctx context.Context) {
	for _, sitemapURL := range p.cfg.SitemapURLs {
		urls, err := fetchSitemap(ctx, sitemapURL)
		if err != nil {
			p.logger.Warn("prefetch: sitemap fetch failed", "url", sitemapURL, "error", err)
			continue
		}
		p.logger.Info("prefetch: sitemap crawled", "url", sitemapURL, "urls", len(urls))
		for _, u := range urls {
			p.scheduleWarm(ctx, u)
		}
	}
}

// parseLinkPreload extracts URLs from Link header values with
// rel=preload. Example: <https://example.com/style.css>; rel=preload
func parseLinkPreload(header, host string) []string {
	var urls []string
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(part)
		if !strings.Contains(strings.ToLower(part), "rel=preload") &&
			!strings.Contains(strings.ToLower(part), `rel="preload"`) {
			continue
		}
		start := strings.IndexByte(part, '<')
		end := strings.IndexByte(part, '>')
		if start < 0 || end <= start {
			continue
		}
		u := part[start+1 : end]
		if strings.HasPrefix(u, "/") {
			u = "http://" + host + u
		}
		urls = append(urls, u)
	}
	return urls
}

// sitemap XML structures.
type sitemapURLSet struct {
	URLs []sitemapURL `xml:"url"`
}

type sitemapURL struct {
	Loc string `xml:"loc"`
}

func fetchSitemap(ctx context.Context, url string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return nil, err
	}

	var urlset sitemapURLSet
	if err := xml.Unmarshal(body, &urlset); err != nil {
		return nil, err
	}

	urls := make([]string, 0, len(urlset.URLs))
	for _, u := range urlset.URLs {
		if u.Loc != "" {
			urls = append(urls, u.Loc)
		}
	}
	return urls, nil
}

// discardWriter captures only status, discards body.
type discardWriter struct {
	header http.Header
	status int
}

func (d *discardWriter) Header() http.Header { return d.header }

func (d *discardWriter) WriteHeader(code int) { d.status = code }

func (d *discardWriter) Write(b []byte) (int, error) { return len(b), nil }

// Middleware returns an http.Handler that calls p.OnResponse for every
// response that carries an X-Cache: MISS or X-Cache: REVALIDATED header
// (i.e., responses that were just stored). This decouples the prefetcher
// from the cache handler's internal struct so the handler can be built
// before the prefetcher is created without a circular dependency.
func (p *Prefetcher) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		xc := w.Header().Get("X-Cache")
		if xc == "MISS" || xc == "REVALIDATED" {
			p.OnResponse(r.Context(), r.Host, w.Header())
		}
	})
}
