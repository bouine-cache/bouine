package cloudflare

import (
	"errors"
	"time"

	cfsdk "github.com/cloudflare/cloudflare-go/v4"
)

// Error type constants for metric / span label classification.
const (
	ErrTypeRateLimit    = "rate_limit"
	ErrTypeZoneConfig   = "zone_config"
	ErrTypeServerError  = "server_error"
	ErrTypeClientError  = "client_error"
	ErrTypeNetworkError = "network_error"
)

// RateLimitError is returned when the Cloudflare API rate limit is exceeded
// and all retries are exhausted.
type RateLimitError struct {
	RetryAfter time.Duration
}

func (e *RateLimitError) Error() string {
	if e.RetryAfter > 0 {
		return "cloudflare: rate limit exceeded, retry after " + e.RetryAfter.String()
	}
	return "cloudflare: rate limit exceeded"
}

// ZoneConfigError is returned when the zone ID is invalid or absent.
type ZoneConfigError struct {
	Msg string
}

func (e *ZoneConfigError) Error() string { return e.Msg }

// ErrorType classifies a Cloudflare purge error into a metric-friendly string.
func ErrorType(err error) string {
	var rl *RateLimitError
	if errors.As(err, &rl) {
		return ErrTypeRateLimit
	}
	var zc *ZoneConfigError
	if errors.As(err, &zc) {
		return ErrTypeZoneConfig
	}
	var cfErr *cfsdk.Error
	if errors.As(err, &cfErr) {
		if cfErr.StatusCode >= 500 {
			return ErrTypeServerError
		}
		return ErrTypeClientError
	}
	return ErrTypeNetworkError
}
