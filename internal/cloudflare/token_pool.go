package cloudflare

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudflare/cloudflare-go/v4"
	"github.com/cloudflare/cloudflare-go/v4/cache"
	"github.com/cloudflare/cloudflare-go/v4/option"

	"github.com/bouine-cache/bouine/pkg/header"
)

// TokenPool manages a pool of Cloudflare API tokens for rate limit
// spreading. Each token has its own rate limit quota. The pool rotates
// across tokens in round-robin order. When a token receives a 429, it is
// marked as rate-limited for a cooldown period and skipped during rotation.
type TokenPool struct {
	// onRotate is called when a token is rotated due to rate limiting.
	// Optional, nil-safe.
	onRotate    func(tokenIndex int)
	tokens      []string
	cooldowns   []time.Time // rate-limited-until timestamps
	rrIndex     atomic.Uint64
	cooldownDur time.Duration
	mu          sync.Mutex
}

// NewTokenPool creates a TokenPool from a list of API tokens. If only one
// token is provided, the pool is a thin wrapper that always uses that token.
// cooldownDur is how long a rate-limited token is skipped. Default 60s when 0.
func NewTokenPool(tokens []string, cooldownDur time.Duration) *TokenPool {
	if cooldownDur <= 0 {
		cooldownDur = 60 * time.Second
	}
	return &TokenPool{
		tokens:      tokens,
		cooldowns:   make([]time.Time, len(tokens)),
		cooldownDur: cooldownDur,
	}
}

// Next returns the next available token and its index. Tokens that are
// currently in cooldown are skipped. If all tokens are in cooldown, the
// one with the earliest cooldown expiry is returned.
func (p *TokenPool) Next() (token string, index int) {
	if len(p.tokens) == 0 {
		return "", 0
	}
	if len(p.tokens) == 1 {
		return p.tokens[0], 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	next := p.rrIndex.Add(1)
	startIdx := int(next % uint64(len(p.tokens))) //nolint:gosec // G115: next is bounded by len(p.tokens) which is always small

	// Try to find a token not in cooldown.
	var earliestIdx int
	var earliestTime time.Time
	for i := range len(p.tokens) {
		idx := (startIdx + i) % len(p.tokens)
		if p.cooldowns[idx].IsZero() || now.After(p.cooldowns[idx]) {
			p.cooldowns[idx] = time.Time{} // clear expired cooldown
			return p.tokens[idx], idx
		}
		if earliestTime.IsZero() || p.cooldowns[idx].Before(earliestTime) {
			earliestTime = p.cooldowns[idx]
			earliestIdx = idx
		}
	}

	// All tokens in cooldown — return the one with the earliest expiry.
	return p.tokens[earliestIdx], earliestIdx
}

// MarkRateLimited marks the token at the given index as rate-limited for
// the cooldown duration. The pool will skip this token until the cooldown
// expires. If retryAfter is non-zero, it overrides the default cooldown.
func (p *TokenPool) MarkRateLimited(index int, retryAfter time.Duration) {
	if index < 0 || index >= len(p.tokens) {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	dur := p.cooldownDur
	if retryAfter > 0 && retryAfter < 5*time.Minute {
		dur = retryAfter
	}
	p.cooldowns[index] = time.Now().Add(dur)

	if p.onRotate != nil {
		p.onRotate(index)
	}
}

// OnRotate sets a callback invoked when a token is marked as rate-limited.
func (p *TokenPool) OnRotate(fn func(tokenIndex int)) {
	p.onRotate = fn
}

// Len returns the number of tokens in the pool.
func (p *TokenPool) Len() int {
	return len(p.tokens)
}

// AvailableCount returns the number of tokens not currently in cooldown.
func (p *TokenPool) AvailableCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	avail := 0
	for i := range p.cooldowns {
		if p.cooldowns[i].IsZero() || now.After(p.cooldowns[i]) {
			avail++
		}
	}
	return avail
}

// multiTokenPurger implements CachePurger by routing requests through a
// TokenPool, selecting the next available token for each call. When a call
// returns a 429, the token is marked as rate-limited and the error is
// returned so the retry logic in Client.doPurge can retry with a different
// token.
type multiTokenPurger struct {
	pool   *TokenPool
	zoneID string
}

// newMultiTokenPurger creates a CachePurger that rotates across multiple
// API tokens. Each call to Purge uses the next available token.
func newMultiTokenPurger(pool *TokenPool, zoneID string) *multiTokenPurger {
	return &multiTokenPurger{
		pool:   pool,
		zoneID: zoneID,
	}
}

func (m *multiTokenPurger) Purge(
	ctx context.Context,
	params cache.CachePurgeParams,
	_ ...option.RequestOption,
) (*cache.CachePurgeResponse, error) {
	token, tokenIdx := m.pool.Next()
	if token == "" {
		return nil, &ZoneConfigError{Msg: "cloudflare: no API tokens available"}
	}

	sdkClient := cloudflare.NewClient(option.WithAPIToken(token))
	_, err := sdkClient.Cache.Purge(ctx, cache.CachePurgeParams{
		ZoneID: cloudflare.F(m.zoneID),
		Body:   params.Body,
	})
	if err == nil {
		return &cache.CachePurgeResponse{}, nil
	}

	// Check if this was a rate limit error — if so, mark the token.
	var apiErr *cloudflare.Error
	if errors.As(err, &apiErr) && apiErr.StatusCode == 429 {
		var retryAfter time.Duration
		if apiErr.Response != nil {
			retryAfter = parseRetryAfterValue(apiErr.Response.Header.Get(header.RetryAfter))
		}
		m.pool.MarkRateLimited(tokenIdx, retryAfter)
	}

	return nil, err
}
