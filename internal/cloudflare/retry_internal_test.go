package cloudflare

import (
	"net/http"
	"testing"
	"time"

	cfsdk "github.com/cloudflare/cloudflare-go/v4"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/header"
)

func TestParseRetryAfter_NilResponse(t *testing.T) {
	t.Parallel()
	require.Equal(t, time.Duration(0), parseRetryAfter(nil))
}

func TestParseRetryAfter_EmptyHeader(t *testing.T) {
	t.Parallel()
	resp := &http.Response{Header: http.Header{}}
	require.Equal(t, time.Duration(0), parseRetryAfter(resp))
}

func TestParseRetryAfter_Seconds(t *testing.T) {
	t.Parallel()
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set(header.RetryAfter, "30")
	require.Equal(t, 30*time.Second, parseRetryAfter(resp))
}

func TestParseRetryAfter_HTTPDate(t *testing.T) {
	t.Parallel()
	resp := &http.Response{Header: http.Header{}}
	future := time.Now().Add(45 * time.Second)
	resp.Header.Set(header.RetryAfter, future.UTC().Format(http.TimeFormat))
	d := parseRetryAfter(resp)
	require.InDelta(t, 45, d.Seconds(), 2)
}

func TestParseRetryAfter_PastHTTPDate(t *testing.T) {
	t.Parallel()
	resp := &http.Response{Header: http.Header{}}
	past := time.Now().Add(-10 * time.Second)
	resp.Header.Set(header.RetryAfter, past.UTC().Format(http.TimeFormat))
	require.Equal(t, time.Duration(0), parseRetryAfter(resp))
}

func TestParseRetryAfter_InvalidValue(t *testing.T) {
	t.Parallel()
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set(header.RetryAfter, "not-a-date")
	require.Equal(t, time.Duration(0), parseRetryAfter(resp))
}

func TestParseRetryAfter_CappedAtMax(t *testing.T) {
	t.Parallel()
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set(header.RetryAfter, "999999999")
	require.Equal(t, maxRetryAfter, parseRetryAfter(resp))
}

func TestParseRetryAfter_NegativeSecondsCappedToZero(t *testing.T) {
	t.Parallel()
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set(header.RetryAfter, "-5")
	require.Equal(t, time.Duration(0), parseRetryAfter(resp))
}

func TestParseRetryAfter_ExactCap(t *testing.T) {
	t.Parallel()
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set(header.RetryAfter, "60")
	require.Equal(t, 60*time.Second, parseRetryAfter(resp))
}

func TestParseRetryAfter_BelowCap(t *testing.T) {
	t.Parallel()
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set(header.RetryAfter, "59")
	require.Equal(t, 59*time.Second, parseRetryAfter(resp))
}

func TestParseRetryAfter_FutureHTTPDateCapped(t *testing.T) {
	t.Parallel()
	resp := &http.Response{Header: http.Header{}}
	future := time.Now().Add(365 * 24 * time.Hour)
	resp.Header.Set(header.RetryAfter, future.UTC().Format(http.TimeFormat))
	require.Equal(t, maxRetryAfter, parseRetryAfter(resp))
}

func TestTryDecision_Next_RetryAfterCapped(t *testing.T) {
	t.Parallel()
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set(header.RetryAfter, "999999999")
	apiErr := &cfsdk.Error{
		StatusCode: 429,
		Response:   resp,
	}

	d := firstTry(defaultRetryDelay)
	decision := d.next(apiErr)

	require.True(t, decision.shouldTry)
	require.Equal(t, 1, decision.attempt)
	// The delay should be jittered around maxRetryAfter, not 999999999 seconds.
	require.LessOrEqual(t, decision.delay, maxRetryAfter+withJitter(maxRetryAfter))
	require.GreaterOrEqual(t, decision.delay, maxRetryAfter-withJitter(maxRetryAfter))
}
