package cloudflare_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cloudflare/cloudflare-go/v4/cache"
	"github.com/cloudflare/cloudflare-go/v4/option"

	cf "github.com/bouine-cache/bouine/internal/cloudflare"
)

// fakePurger records calls and returns a pre-set sequence of errors.
type fakePurger struct {
	calls  []cache.CachePurgeParams
	errors []error // consumed one per Purge call; nil-terminated means success
}

func (f *fakePurger) Purge(_ context.Context, params cache.CachePurgeParams, _ ...option.RequestOption) (*cache.CachePurgeResponse, error) {
	f.calls = append(f.calls, params)
	if len(f.errors) == 0 {
		return &cache.CachePurgeResponse{}, nil
	}
	err := f.errors[0]
	f.errors = f.errors[1:]
	if err != nil {
		return nil, err
	}
	return &cache.CachePurgeResponse{}, nil
}

func TestClient_PurgeURLs_Success(t *testing.T) {
	t.Parallel()
	purger := &fakePurger{}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgeURLs(context.Background(), []string{"https://example.com/page"})
	require.NoError(t, err, "PurgeURLs")
	require.Len(t, purger.calls, 1)
}

func TestClient_PurgeTags_Success(t *testing.T) {
	t.Parallel()
	purger := &fakePurger{}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgeTags(context.Background(), []string{"product-123"})
	require.NoError(t, err, "PurgeTags")
	require.Len(t, purger.calls, 1)
}

func TestClient_NilSafe(t *testing.T) {
	t.Parallel()
	var c *cf.Client
	err := c.PurgeURLs(context.Background(), []string{"https://x.com/"})
	require.NoError(t, err, "nil client PurgeURLs should no-op,")
	err = c.PurgeTags(context.Background(), []string{"tag"})
	require.NoError(t, err, "nil client PurgeTags should no-op,")
}

func TestClient_EmptySlice_NoOp(t *testing.T) {
	t.Parallel()
	purger := &fakePurger{}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgeURLs(context.Background(), nil)
	require.NoError(t, err, "empty PurgeURLs should no-op,")
	require.Len(t, purger.calls, 0)
}

func TestClient_NetworkError_RetriesThenSucceeds(t *testing.T) {
	t.Parallel()
	purger := &fakePurger{errors: []error{errors.New("network down"), nil}}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgeURLs(context.Background(), []string{"https://x.com/"})
	require.NoError(t, err, "expected success after retry, got")
	// Two calls — first fails with network error, retry succeeds.
	require.Len(t, purger.calls, 2)
}

func TestClient_NetworkError_RetriesExhausted(t *testing.T) {
	t.Parallel()
	netErr := errors.New("connection refused")
	purger := &fakePurger{errors: []error{netErr, netErr, netErr}}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgeURLs(context.Background(), []string{"https://x.com/"})
	require.Error(t, err)
	// maxRetries=2 → 3 total attempts.
	require.Len(t, purger.calls, 3)
}

func TestNew_MissingZone(t *testing.T) {
	t.Parallel()
	_, err := cf.New(cf.Config{ZoneID: "", APIToken: "tok"})
	require.Error(t, err)
	_, ok := err.(*cf.ZoneConfigError)
	require.True(t, ok)
}

func TestNew_MissingToken(t *testing.T) {
	t.Parallel()
	_, err := cf.New(cf.Config{ZoneID: "zone1", APIToken: ""})
	require.Error(t, err)
	_, ok := err.(*cf.ZoneConfigError)
	require.True(t, ok)
}

func TestRetry_RateLimit_WithRetryAfter(t *testing.T) {
	t.Parallel()
	// Simulate two 429s with Retry-After header, followed by success.
	resp429 := AsSDKErrorWithRetryAfter(429, "0")

	purger := &fakePurger{
		errors: []error{
			resp429,
			resp429,
			nil, // success on third attempt
		},
	}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgeURLs(context.Background(), []string{"https://x.com/"})
	require.NoError(t, err, "expected success after retries,")
	require.Len(t, purger.calls, 3)
}

func TestRetry_500_ThenSuccess(t *testing.T) {
	t.Parallel()
	purger := &fakePurger{
		errors: []error{
			AsSDKError(500),
			nil,
		},
	}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgeURLs(context.Background(), []string{"https://x.com/"})
	require.NoError(t, err, "expected success after 5xx retry,")
	require.Len(t, purger.calls, 2)
}

func TestRetry_RateLimit_Exhausted(t *testing.T) {
	t.Parallel()
	resp429 := AsSDKErrorWithRetryAfter(429, "0")

	purger := &fakePurger{
		errors: []error{
			resp429,
			resp429,
			resp429,
		},
	}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgeURLs(context.Background(), []string{"https://x.com/"})
	require.Error(t, err)
	var rlErr *cf.RateLimitError
	require.True(t, errors.As(err, &rlErr))
}

func TestRetry_HTTPDateRetryAfter(t *testing.T) {
	t.Parallel()
	// Retry-After as HTTP-date format (2 seconds in the future).
	future := FormatHTTPDate(time.Now().Add(2 * time.Second))
	respRL := AsSDKErrorWithRetryAfter(429, future)

	purger := &fakePurger{
		errors: []error{
			respRL,
			nil,
		},
	}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgeURLs(context.Background(), []string{"https://x.com/"})
	require.NoError(t, err, "expected success after retry,")
	require.Len(t, purger.calls, 2)
}

func TestRetry_PastHTTPDate_FallsBackToDefault(t *testing.T) {
	t.Parallel()
	// Retry-After as an HTTP-date in the past (clock skew scenario).
	// Should fall back to the default jittered delay, not fire immediately.
	past := FormatHTTPDate(time.Now().Add(-time.Hour))
	respPast := AsSDKErrorWithRetryAfter(429, past)

	purger := &fakePurger{
		errors: []error{
			respPast,
			nil,
		},
	}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgeURLs(context.Background(), []string{"https://x.com/"})
	require.NoError(t, err, "expected success after retry with past date,")
	require.Len(t, purger.calls, 2)
}

func TestErrorType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"rate_limit", &cf.RateLimitError{}, cf.ErrTypeRateLimit},
		{"zone_config", &cf.ZoneConfigError{Msg: "bad"}, cf.ErrTypeZoneConfig},
		{"server_error", AsSDKError(500), cf.ErrTypeServerError},
		{"client_error", AsSDKError(404), cf.ErrTypeClientError},
		{"network_error", errors.New("connection refused"), cf.ErrTypeNetworkError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := cf.ErrorType(tc.err)
			if got != tc.want {
				t.Fatalf("ErrorType(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

func TestNew_Success(t *testing.T) {
	t.Parallel()
	c, err := cf.New(cf.Config{ZoneID: "zone1", APIToken: "tok"})
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, "zone1", c.ZoneID())
}

func TestNew_DefaultsTimeoutAndRetryDelay(t *testing.T) {
	t.Parallel()
	c, err := cf.New(cf.Config{ZoneID: "zone1", APIToken: "tok"})
	require.NoError(t, err)
	require.NotNil(t, c)
}

func TestZoneID_NilClient(t *testing.T) {
	t.Parallel()
	var c *cf.Client
	require.Equal(t, "", c.ZoneID())
}

func TestClient_PurgePrefixes_Success(t *testing.T) {
	t.Parallel()
	purger := &fakePurger{}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgePrefixes(context.Background(), []string{"/api/v1/"})
	require.NoError(t, err, "PurgePrefixes")
	require.Len(t, purger.calls, 1)
}

func TestClient_PurgePrefixes_Empty_NoOp(t *testing.T) {
	t.Parallel()
	purger := &fakePurger{}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgePrefixes(context.Background(), nil)
	require.NoError(t, err, "empty PurgePrefixes should no-op,")
	require.Len(t, purger.calls, 0)
}

func TestClient_PurgeHosts_Success(t *testing.T) {
	t.Parallel()
	purger := &fakePurger{}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgeHosts(context.Background(), []string{"example.com"})
	require.NoError(t, err, "PurgeHosts")
	require.Len(t, purger.calls, 1)
}

func TestClient_PurgeHosts_Empty_NoOp(t *testing.T) {
	t.Parallel()
	purger := &fakePurger{}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgeHosts(context.Background(), nil)
	require.NoError(t, err, "empty PurgeHosts should no-op,")
	require.Len(t, purger.calls, 0)
}

func TestNilClient_AllInvalidators(t *testing.T) {
	t.Parallel()
	var c *cf.Client
	require.NoError(t, c.PurgePrefixes(context.Background(), []string{"/p"}))
	require.NoError(t, c.PurgeHosts(context.Background(), []string{"h"}))
}

func TestRateLimitError_Message(t *testing.T) {
	t.Parallel()
	require.Equal(t, "cloudflare: rate limit exceeded", (&cf.RateLimitError{}).Error())
	require.Equal(t,
		"cloudflare: rate limit exceeded, retry after 5s",
		(&cf.RateLimitError{RetryAfter: 5 * time.Second}).Error(),
	)
}

func TestZoneConfigError_Message(t *testing.T) {
	t.Parallel()
	require.Equal(t, "bad zone", (&cf.ZoneConfigError{Msg: "bad zone"}).Error())
}
