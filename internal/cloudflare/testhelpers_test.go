package cloudflare_test

import (
	"net/http"
	"time"

	"github.com/cloudflare/cloudflare-go/v4"
)

// AsSDKErrorWithRetryAfter creates a *cloudflare.Error with the given
// status code and Retry-After header value. This isolates the net/http
// dependency to this helper file.
func AsSDKErrorWithRetryAfter(statusCode int, retryAfter string) *cloudflare.Error {
	resp := &http.Response{
		Header: http.Header{},
	}
	if retryAfter != "" {
		resp.Header.Set("Retry-After", retryAfter)
	}
	return &cloudflare.Error{
		StatusCode: statusCode,
		Response:   resp,
	}
}

// AsSDKError creates a *cloudflare.Error with the given status code
// and no response.
func AsSDKError(statusCode int) *cloudflare.Error {
	return &cloudflare.Error{
		StatusCode: statusCode,
	}
}

// FormatHTTPDate formats a time as an HTTP-date per RFC 9110.
func FormatHTTPDate(t time.Time) string {
	return t.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")
}
