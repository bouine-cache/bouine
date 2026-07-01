package cloudflare_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/cloudflare/cloudflare-go/v4"
	"github.com/cloudflare/cloudflare-go/v4/cache"
	"github.com/cloudflare/cloudflare-go/v4/option"

	cf "github.com/thylong/bouine/internal/cloudflare"
	"github.com/thylong/bouine/pkg/header"
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

	if err := c.PurgeURLs(context.Background(), []string{"https://example.com/page"}); err != nil {
		t.Fatalf("PurgeURLs: %v", err)
	}
	if len(purger.calls) != 1 {
		t.Fatalf("expected 1 API call, got %d", len(purger.calls))
	}
}

func TestClient_PurgeTags_Success(t *testing.T) {
	t.Parallel()
	purger := &fakePurger{}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	if err := c.PurgeTags(context.Background(), []string{"product-123"}); err != nil {
		t.Fatalf("PurgeTags: %v", err)
	}
	if len(purger.calls) != 1 {
		t.Fatalf("expected 1 API call, got %d", len(purger.calls))
	}
}

func TestClient_NilSafe(t *testing.T) {
	t.Parallel()
	var c *cf.Client
	if err := c.PurgeURLs(context.Background(), []string{"https://x.com/"}); err != nil {
		t.Fatalf("nil client PurgeURLs should no-op, got %v", err)
	}
	if err := c.PurgeTags(context.Background(), []string{"tag"}); err != nil {
		t.Fatalf("nil client PurgeTags should no-op, got %v", err)
	}
}

func TestClient_EmptySlice_NoOp(t *testing.T) {
	t.Parallel()
	purger := &fakePurger{}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	if err := c.PurgeURLs(context.Background(), nil); err != nil {
		t.Fatalf("empty PurgeURLs should no-op, got %v", err)
	}
	if len(purger.calls) != 0 {
		t.Fatalf("expected 0 API calls for empty slice, got %d", len(purger.calls))
	}
}

func TestClient_NetworkError_RetriesThenSucceeds(t *testing.T) {
	t.Parallel()
	purger := &fakePurger{errors: []error{errors.New("network down"), nil}}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgeURLs(context.Background(), []string{"https://x.com/"})
	if err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}
	// Two calls — first fails with network error, retry succeeds.
	if len(purger.calls) != 2 {
		t.Fatalf("expected 2 calls (retry on network error), got %d", len(purger.calls))
	}
}

func TestClient_NetworkError_RetriesExhausted(t *testing.T) {
	t.Parallel()
	netErr := errors.New("connection refused")
	purger := &fakePurger{errors: []error{netErr, netErr, netErr}}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgeURLs(context.Background(), []string{"https://x.com/"})
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	// maxRetries=2 → 3 total attempts.
	if len(purger.calls) != 3 {
		t.Fatalf("expected 3 calls (2 retries), got %d", len(purger.calls))
	}
}

func TestNew_MissingZone(t *testing.T) {
	t.Parallel()
	_, err := cf.New(cf.Config{ZoneID: "", APIToken: "tok"})
	if err == nil {
		t.Fatal("expected error for missing zone_id")
	}
	if _, ok := err.(*cf.ZoneConfigError); !ok {
		t.Fatalf("expected *ZoneConfigError, got %T: %v", err, err)
	}
}

func TestNew_MissingToken(t *testing.T) {
	t.Parallel()
	_, err := cf.New(cf.Config{ZoneID: "zone1", APIToken: ""})
	if err == nil {
		t.Fatal("expected error for missing api_token")
	}
	if _, ok := err.(*cf.ZoneConfigError); !ok {
		t.Fatalf("expected *ZoneConfigError, got %T: %v", err, err)
	}
}

// asSDKError wraps a status code and optional response in a real
// *cloudflare.Error so the errors.As(err, &apiErr) branch in retry.go
// is exercised, including parseRetryAfter(apiErr.Response).
func asSDKError(statusCode int, resp *http.Response) *cloudflare.Error {
	return &cloudflare.Error{
		StatusCode: statusCode,
		Response:   resp,
	}
}

func TestRetry_RateLimit_WithRetryAfter(t *testing.T) {
	t.Parallel()
	// Simulate two 429s with Retry-After header, followed by success.
	resp429 := &http.Response{Header: http.Header{header.RetryAfter: {"0"}}}

	purger := &fakePurger{
		errors: []error{
			asSDKError(429, resp429),
			asSDKError(429, resp429),
			nil, // success on third attempt
		},
	}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgeURLs(context.Background(), []string{"https://x.com/"})
	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if len(purger.calls) != 3 {
		t.Fatalf("expected 3 calls (2 retries), got %d", len(purger.calls))
	}
}

func TestRetry_500_ThenSuccess(t *testing.T) {
	t.Parallel()
	purger := &fakePurger{
		errors: []error{
			asSDKError(500, nil),
			nil,
		},
	}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgeURLs(context.Background(), []string{"https://x.com/"})
	if err != nil {
		t.Fatalf("expected success after 5xx retry, got %v", err)
	}
	if len(purger.calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(purger.calls))
	}
}

func TestRetry_RateLimit_Exhausted(t *testing.T) {
	t.Parallel()
	resp429 := &http.Response{Header: http.Header{header.RetryAfter: {"0"}}}

	purger := &fakePurger{
		errors: []error{
			asSDKError(429, resp429),
			asSDKError(429, resp429),
			asSDKError(429, resp429),
		},
	}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgeURLs(context.Background(), []string{"https://x.com/"})
	if err == nil {
		t.Fatal("expected rate limit error after retries exhausted")
	}
	var rlErr *cf.RateLimitError
	if !errors.As(err, &rlErr) {
		t.Fatalf("expected *RateLimitError, got %T: %v", err, err)
	}
}

func TestRetry_HTTPDateRetryAfter(t *testing.T) {
	t.Parallel()
	// Retry-After as HTTP-date format (2 seconds in the future).
	future := time.Now().Add(2 * time.Second).UTC().Format(http.TimeFormat)
	respRL := &http.Response{Header: http.Header{header.RetryAfter: {future}}}

	purger := &fakePurger{
		errors: []error{
			asSDKError(429, respRL),
			nil,
		},
	}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgeURLs(context.Background(), []string{"https://x.com/"})
	if err != nil {
		t.Fatalf("expected success after retry, got %v", err)
	}
	if len(purger.calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(purger.calls))
	}
}

func TestRetry_PastHTTPDate_FallsBackToDefault(t *testing.T) {
	t.Parallel()
	// Retry-After as an HTTP-date in the past (clock skew scenario).
	// Should fall back to the default jittered delay, not fire immediately.
	past := time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)
	respPast := &http.Response{Header: http.Header{header.RetryAfter: {past}}}

	purger := &fakePurger{
		errors: []error{
			asSDKError(429, respPast),
			nil,
		},
	}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgeURLs(context.Background(), []string{"https://x.com/"})
	if err != nil {
		t.Fatalf("expected success after retry with past date, got %v", err)
	}
	if len(purger.calls) != 2 {
		t.Fatalf("expected 2 calls (retry with fallback delay), got %d", len(purger.calls))
	}
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
		{"server_error", asSDKError(500, nil), cf.ErrTypeServerError},
		{"client_error", asSDKError(404, nil), cf.ErrTypeClientError},
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
