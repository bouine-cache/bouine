package cloudflare

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"

	cfsdk "github.com/cloudflare/cloudflare-go/v4"

	"github.com/bouine-cache/bouine/pkg/header"
)

const (
	defaultRetryDelay = 250 * time.Millisecond
	maxRetries        = 2
	jitterRatio       = 0.25
	// maxRetryAfter caps the delay parsed from a Retry-After header.
	// Cloudflare's real Retry-After is typically < 60 s; a malicious or
	// buggy response with Retry-After: 999999999 would otherwise park
	// the propagator goroutine for ~31 years. The cap prevents this DoS
	// while still honouring legitimate rate-limit backoff.
	maxRetryAfter = 60 * time.Second
)

// httpTimeFormat is the HTTP-date format used in Retry-After headers,
// equivalent to net/http.TimeFormat.
const httpTimeFormat = "Mon, 02 Jan 2006 15:04:05 GMT"

// httpStatusCoder is implemented by *cfsdk.Error and also by test fakes.
type httpStatusCoder interface {
	error
	HTTPStatus() int
}

type tryDecision struct {
	shouldTry    bool
	attempt      int
	delay        time.Duration
	defaultDelay time.Duration
	finalError   error
}

func firstTry(defaultDelay time.Duration) tryDecision {
	return tryDecision{shouldTry: true, defaultDelay: defaultDelay}
}

func (d tryDecision) next(err error) tryDecision {
	var statusCode int
	var retryAfter time.Duration

	var apiErr *cfsdk.Error
	if errors.As(err, &apiErr) {
		statusCode = apiErr.StatusCode
		if apiErr.Response != nil {
			retryAfter = parseRetryAfterValue(apiErr.Response.Header.Get(header.RetryAfter))
		}
	} else {
		var sc httpStatusCoder
		if errors.As(err, &sc) {
			statusCode = sc.HTTPStatus()
		} else {
			// Network error (DNS, connection refused, TCP reset, TLS
			// handshake, timeout). Retry with backoff — transient network
			// blips should not permanently lose a purge.
			if d.attempt < maxRetries {
				return tryDecision{
					shouldTry: true, attempt: d.attempt + 1,
					delay:        withJitter(d.defaultDelay),
					defaultDelay: d.defaultDelay,
				}
			}
			return tryDecision{finalError: fmt.Errorf("cloudflare: network error after %d retries: %w", d.attempt, err)}
		}
	}

	if statusCode == 429 {
		if d.attempt < maxRetries {
			delay := retryAfter
			if delay <= 0 {
				delay = withJitter(d.defaultDelay)
			} else {
				delay = withJitter(delay)
			}
			return tryDecision{
				shouldTry: true, attempt: d.attempt + 1,
				delay: delay, defaultDelay: d.defaultDelay,
			}
		}
		return tryDecision{finalError: &RateLimitError{RetryAfter: retryAfter}}
	}

	if statusCode >= 500 && d.attempt < maxRetries {
		return tryDecision{
			shouldTry: true, attempt: d.attempt + 1,
			delay: withJitter(d.defaultDelay), defaultDelay: d.defaultDelay,
		}
	}

	return tryDecision{finalError: fmt.Errorf("cloudflare: API error %d: %w", statusCode, err)}
}

func withJitter(d time.Duration) time.Duration {
	return d + time.Duration(float64(d)*jitterRatio*(2*cryptoFloat64()-1))
}

func cryptoFloat64() float64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0.5
	}
	//nolint:mnd
	return float64(binary.LittleEndian.Uint64(b[:])&((1<<53)-1)) / (1 << 53)
}

// parseRetryAfterValue parses a Retry-After header value (either
// delta-seconds or HTTP-date) and returns the remaining duration,
// capped at maxRetryAfter.
func parseRetryAfterValue(v string) time.Duration {
	if v == "" {
		return 0
	}
	var d time.Duration
	if secs, err := strconv.Atoi(v); err == nil {
		d = time.Duration(secs) * time.Second
	} else if t, err := time.Parse(httpTimeFormat, v); err == nil {
		d = time.Until(t)
	} else {
		return 0
	}
	if d > maxRetryAfter {
		return maxRetryAfter
	}
	if d < 0 {
		return 0
	}
	return d
}
