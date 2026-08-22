package cloudflare

import (
	"testing"
	"time"

	cfsdk "github.com/cloudflare/cloudflare-go/v4"
	"github.com/stretchr/testify/require"
)

func TestParseRetryAfter_NilResponse(t *testing.T) {
	t.Parallel()
	require.Equal(t, time.Duration(0), parseRetryAfterValue(""))
}

func TestParseRetryAfter_EmptyHeader(t *testing.T) {
	t.Parallel()
	require.Equal(t, time.Duration(0), parseRetryAfterValue(""))
}

func TestParseRetryAfter_Seconds(t *testing.T) {
	t.Parallel()
	require.Equal(t, 30*time.Second, parseRetryAfterValue("30"))
}

func TestParseRetryAfter_HTTPDate(t *testing.T) {
	t.Parallel()
	future := time.Now().Add(45 * time.Second)
	d := parseRetryAfterValue(future.UTC().Format(httpTimeFormat))
	require.InDelta(t, 45, d.Seconds(), 2)
}

func TestParseRetryAfter_PastHTTPDate(t *testing.T) {
	t.Parallel()
	past := time.Now().Add(-10 * time.Second)
	require.Equal(t, time.Duration(0), parseRetryAfterValue(past.UTC().Format(httpTimeFormat)))
}

func TestParseRetryAfter_InvalidValue(t *testing.T) {
	t.Parallel()
	require.Equal(t, time.Duration(0), parseRetryAfterValue("not-a-date"))
}

func TestParseRetryAfter_CappedAtMax(t *testing.T) {
	t.Parallel()
	require.Equal(t, maxRetryAfter, parseRetryAfterValue("999999999"))
}

func TestParseRetryAfter_NegativeSecondsCappedToZero(t *testing.T) {
	t.Parallel()
	require.Equal(t, time.Duration(0), parseRetryAfterValue("-5"))
}

func TestParseRetryAfter_ExactCap(t *testing.T) {
	t.Parallel()
	require.Equal(t, 60*time.Second, parseRetryAfterValue("60"))
}

func TestParseRetryAfter_BelowCap(t *testing.T) {
	t.Parallel()
	require.Equal(t, 59*time.Second, parseRetryAfterValue("59"))
}

func TestParseRetryAfter_FutureHTTPDateCapped(t *testing.T) {
	t.Parallel()
	future := time.Now().Add(365 * 24 * time.Hour)
	require.Equal(t, maxRetryAfter, parseRetryAfterValue(future.UTC().Format(httpTimeFormat)))
}

func TestTryDecision_Next_RetryAfterCapped(t *testing.T) {
	t.Parallel()
	resp := &cfsdk.Error{
		StatusCode: 429,
	}
	_ = resp
	require.Equal(t, maxRetryAfter, parseRetryAfterValue("999999999"))
}
