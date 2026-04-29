package cloudflare_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/cloudflare/cloudflare-go/v2/cache"
	"github.com/cloudflare/cloudflare-go/v2/option"

	cf "github.com/thylong/bouine/internal/cloudflare"
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
	var c *cf.Client
	if err := c.PurgeURLs(context.Background(), []string{"https://x.com/"}); err != nil {
		t.Fatalf("nil client PurgeURLs should no-op, got %v", err)
	}
	if err := c.PurgeTags(context.Background(), []string{"tag"}); err != nil {
		t.Fatalf("nil client PurgeTags should no-op, got %v", err)
	}
}

func TestClient_EmptySlice_NoOp(t *testing.T) {
	purger := &fakePurger{}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	if err := c.PurgeURLs(context.Background(), nil); err != nil {
		t.Fatalf("empty PurgeURLs should no-op, got %v", err)
	}
	if len(purger.calls) != 0 {
		t.Fatalf("expected 0 API calls for empty slice, got %d", len(purger.calls))
	}
}

func TestClient_NetworkError_NoRetry(t *testing.T) {
	purger := &fakePurger{errors: []error{errors.New("network down"), nil}}
	c := cf.NewWithPurger(purger, "zone1", time.Millisecond)

	err := c.PurgeURLs(context.Background(), []string{"https://x.com/"})
	if err == nil {
		t.Fatal("expected error from network failure, got nil")
	}
	// Only one call — network errors are not retried.
	if len(purger.calls) != 1 {
		t.Fatalf("expected 1 call (no retry on network error), got %d", len(purger.calls))
	}
}

func TestNew_MissingZone(t *testing.T) {
	_, err := cf.New(cf.Config{ZoneID: "", APIToken: "tok"})
	if err == nil {
		t.Fatal("expected error for missing zone_id")
	}
}

func TestNew_MissingToken(t *testing.T) {
	_, err := cf.New(cf.Config{ZoneID: "zone1", APIToken: ""})
	if err == nil {
		t.Fatal("expected error for missing api_token")
	}
}

func TestRetry_RateLimit(t *testing.T) {
	// Simulate two 429s followed by success.
	resp429 := &http.Response{Header: http.Header{"Retry-After": {"0"}}}
	_ = resp429 // only used for parseRetryAfter; fakePurger error doesn't carry response

	purger := &fakePurger{
		errors: []error{
			&fakeAPIError{status: 429},
			&fakeAPIError{status: 429},
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
	purger := &fakePurger{
		errors: []error{
			&fakeAPIError{status: 500},
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

// fakeAPIError mimics cloudflare-go's *cloudflare.Error for retry testing.
type fakeAPIError struct {
	status int
}

func (e *fakeAPIError) Error() string   { return http.StatusText(e.status) }
func (e *fakeAPIError) HTTPStatus() int { return e.status }
