package cloudflare

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	cfsdk "github.com/cloudflare/cloudflare-go/v2"
)

const (
	defaultRetryDelay = 250 * time.Millisecond
	maxRetries        = 2
	jitterRatio       = 0.25
)

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
		retryAfter = parseRetryAfter(apiErr.Response)
	} else {
		var sc httpStatusCoder
		if errors.As(err, &sc) {
			statusCode = sc.HTTPStatus()
		} else {
			return tryDecision{finalError: fmt.Errorf("cloudflare: network error: %w", err)}
		}
	}

	if statusCode == http.StatusTooManyRequests {
		if d.attempt < maxRetries {
			delay := retryAfter
			if delay == 0 {
				delay = withJitter(d.defaultDelay)
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

func parseRetryAfter(resp *http.Response) time.Duration {
	if resp == nil {
		return 0
	}
	v := resp.Header.Get("Retry-After")
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		return time.Until(t)
	}
	return 0
}
