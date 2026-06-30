package observability

import (
	"net/http"
	"sync"
)

const originHeaderRingCap = 1000

// HeaderSample is a single sampled origin response header audit record.
type HeaderSample struct {
	Pool            string
	Timestamp       int64
	HasCacheControl bool
	HasETag         bool
	HasLastModified bool
	HasSurrogateKey bool
	StatusCode      int
	CacheControlVal string
}

// HeaderAuditSummary is the aggregated per-pool header audit statistics.
type HeaderAuditSummary struct {
	SampleCount        int64
	HasCacheControlPct float64
	HasETagPct         float64
	HasLastModifiedPct float64
	HasSurrogateKeyPct float64
	SampleCacheControl string
}

// OriginHeaderRing is a fixed-size circular buffer that samples origin
// response headers to audit Cache-Control, ETag, Last-Modified, and
// Surrogate-Key presence per upstream pool.
type OriginHeaderRing struct {
	mu      sync.Mutex
	samples [originHeaderRingCap]HeaderSample
	head    int
	count   int
}

// NewOriginHeaderRing creates a new OriginHeaderRing.
func NewOriginHeaderRing() *OriginHeaderRing {
	return &OriginHeaderRing{}
}

// Sample records a single origin response header audit. Called from
// the data plane after receiving an origin response. The caller is
// responsible for sampling decisions (e.g. 1:100) to limit overhead.
func (r *OriginHeaderRing) Sample(pool string, header http.Header, statusCode int) {
	if header == nil {
		return
	}

	s := HeaderSample{
		Pool:            pool,
		HasCacheControl: header.Get("Cache-Control") != "",
		HasETag:         header.Get("ETag") != "",
		HasLastModified: header.Get("Last-Modified") != "",
		HasSurrogateKey: header.Get("Surrogate-Key") != "",
		StatusCode:      statusCode,
		CacheControlVal: truncate(header.Get("Cache-Control"), 256),
	}

	r.mu.Lock()
	r.samples[r.head] = s
	r.head = (r.head + 1) % originHeaderRingCap
	if r.count < originHeaderRingCap {
		r.count++
	}
	r.mu.Unlock()
}

// HeaderAudit returns per-pool aggregated header audit statistics.
func (r *OriginHeaderRing) HeaderAudit() map[string]HeaderAuditSummary {
	out := make(map[string]HeaderAuditSummary)

	r.mu.Lock()
	for i := range r.count {
		s := r.samples[(r.head-1-i+originHeaderRingCap)%originHeaderRingCap]
		summary, ok := out[s.Pool]
		if !ok {
			summary = HeaderAuditSummary{}
		}
		summary.SampleCount++
		if s.HasCacheControl {
			summary.HasCacheControlPct++
		}
		if s.HasETag {
			summary.HasETagPct++
		}
		if s.HasLastModified {
			summary.HasLastModifiedPct++
		}
		if s.HasSurrogateKey {
			summary.HasSurrogateKeyPct++
		}
		if summary.SampleCacheControl == "" && s.CacheControlVal != "" {
			summary.SampleCacheControl = s.CacheControlVal
		}
		out[s.Pool] = summary
	}
	r.mu.Unlock()

	for pool, s := range out {
		if s.SampleCount > 0 {
			s.HasCacheControlPct = s.HasCacheControlPct / float64(s.SampleCount) * 100
			s.HasETagPct = s.HasETagPct / float64(s.SampleCount) * 100
			s.HasLastModifiedPct = s.HasLastModifiedPct / float64(s.SampleCount) * 100
			s.HasSurrogateKeyPct = s.HasSurrogateKeyPct / float64(s.SampleCount) * 100
		}
		out[pool] = s
	}

	return out
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}
