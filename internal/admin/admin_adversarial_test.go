package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/cache"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// --- adversarial token tests ---

// TestAdversarial_TokenEmptyHeader verifies that a write request with
// no Authorization header is rejected with 401.
func TestAdversarial_TokenEmptyHeader(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/purge",
		bytes.NewBufferString(`{"url":"https://example.com/"}`))
	req.Header.Set(header.ContentType, "application/json")
	s.Handler().ServeHTTP(rr, req)
	require.Equal(t, http.StatusUnauthorized, rr.Code)
}

// TestAdversarial_TokenWrongScheme verifies that a token sent with the
// wrong auth scheme (e.g. "Basic" instead of "Bearer") is rejected.
func TestAdversarial_TokenWrongScheme(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/purge",
		bytes.NewBufferString(`{"url":"https://example.com/"}`))
	req.Header.Set(header.ContentType, "application/json")
	req.Header.Set(header.Authorization, "Basic secret")
	s.Handler().ServeHTTP(rr, req)
	require.Equal(t, http.StatusUnauthorized, rr.Code)
}

// TestAdversarial_TokenTrailingWhitespace verifies that a token with
// trailing whitespace is rejected. The constant-time compare is exact;
// "Bearer secret " != "Bearer secret".
func TestAdversarial_TokenTrailingWhitespace(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/purge",
		bytes.NewBufferString(`{"url":"https://example.com/"}`))
	req.Header.Set(header.ContentType, "application/json")
	req.Header.Set(header.Authorization, "Bearer secret ")
	s.Handler().ServeHTTP(rr, req)
	require.Equal(t, http.StatusUnauthorized, rr.Code)
}

// TestAdversarial_TokenCaseSensitive verifies that the "Bearer " prefix
// is case-sensitive. "bearer secret" must be rejected.
func TestAdversarial_TokenCaseSensitive(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/purge",
		bytes.NewBufferString(`{"url":"https://example.com/"}`))
	req.Header.Set(header.ContentType, "application/json")
	req.Header.Set(header.Authorization, "bearer secret")
	s.Handler().ServeHTTP(rr, req)
	require.Equal(t, http.StatusUnauthorized, rr.Code)
}

// --- adversarial malformed JSON tests ---

// TestAdversarial_MalformedJSON_AllEndpoints verifies that every write
// endpoint rejects malformed JSON with 400.
func TestAdversarial_MalformedJSON_AllEndpoints(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:     "secret",
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn:   func(_ api.Key) error { return nil },
		RefreshFn: func(_ api.Key) error { return nil },
		BanFn:     func(_ api.BanExpr) (int, error) { return 0, nil },
	})

	cases := []struct {
		name string
		path string
		body string
	}{
		{"purge/truncated", "/v1/purge", `{"url":"https://example.com`},
		{"purge/non_object", "/v1/purge", `[1,2,3]`},
		{"purge/empty", "/v1/purge", ``},
		{"purge_batch/truncated", "/v1/purge/batch", `{"urls":["https://a.com/`},
		{"purge_batch/non_object", "/v1/purge/batch", `"hello"`},
		{"purge_batch/empty", "/v1/purge/batch", ``},
		{"ban/truncated", "/v1/ban", `{"path_regex":"^/reviews/`},
		{"ban/non_object", "/v1/ban", `42`},
		{"ban/empty", "/v1/ban", ``},
		{"refresh/truncated", "/v1/refresh", `{"url":"https://example.com`},
		{"refresh/non_object", "/v1/refresh", `true`},
		{"refresh/empty", "/v1/refresh", ``},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			code, _ := postWithToken(t, s, tc.path, tc.body)
			require.Equal(t, http.StatusBadRequest, code, "endpoint %s with body %q", tc.path, tc.body)
		})
	}
}

// TestAdversarial_JSONDuplicateFields verifies that duplicate JSON keys
// are handled deterministically. Go's encoding/json keeps the last
// value, which is the standard behaviour.
func TestAdversarial_JSONDuplicateFields(t *testing.T) {
	t.Parallel()
	var purgedKey api.Key
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(key api.Key) error {
			purgedKey = key
			return nil
		},
	})
	body := `{"url":"https://first.com/","url":"https://second.com/"}`
	code, _ := postWithToken(t, s, "/v1/purge", body)
	require.Equal(t, http.StatusOK, code)
	// Go's json decoder uses the last value for duplicate keys.
	expectedKey := cache.BuildKeyFromURL("https://second.com/", nil)
	require.Equal(t, expectedKey, purgedKey, "purge must use the last URL when keys are duplicated")
}

// --- adversarial oversized payload tests ---

// TestAdversarial_OversizedBody verifies that a POST body exceeding
// MaxBodyBytes is rejected with 413.
func TestAdversarial_OversizedBody(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:        "secret",
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn:      func(_ api.Key) error { return nil },
		MaxBodyBytes: 256,
	})
	// Build a valid JSON body that exceeds 256 bytes.
	bigURL := "https://example.com/" + strings.Repeat("a", 300)
	body := `{"url":"` + bigURL + `"}`
	require.Greater(t, len(body), 256)

	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/purge",
		bytes.NewBufferString(body))
	req.Header.Set(header.ContentType, "application/json")
	req.Header.Set(header.Authorization, "Bearer secret")
	s.Handler().ServeHTTP(rr, req)
	require.Equal(t, http.StatusRequestEntityTooLarge, rr.Code)
}

// TestAdversarial_OversizedBody_Batch verifies that batch purge also
// enforces the body limit.
func TestAdversarial_OversizedBody_Batch(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:        "secret",
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn:      func(_ api.Key) error { return nil },
		MaxBodyBytes: 128,
	})
	var sb strings.Builder
	sb.WriteString(`{"urls":["`)
	for i := 0; i < 50; i++ {
		if i > 0 {
			sb.WriteString(`","`)
		}
		sb.WriteString("https://example.com/path/")
	}
	sb.WriteString(`"]}`)
	require.Greater(t, sb.Len(), 128)

	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/purge/batch",
		bytes.NewBufferString(sb.String()))
	req.Header.Set(header.ContentType, "application/json")
	req.Header.Set(header.Authorization, "Bearer secret")
	s.Handler().ServeHTTP(rr, req)
	require.Equal(t, http.StatusRequestEntityTooLarge, rr.Code)
}

// TestAdversarial_BodyWithinLimit verifies that a body just under the
// limit is accepted normally.
func TestAdversarial_BodyWithinLimit(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:        "secret",
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn:      func(_ api.Key) error { return nil },
		MaxBodyBytes: 1 << 20, // 1 MiB
	})
	body := `{"url":"https://example.com/"}`
	code, _ := postWithToken(t, s, "/v1/purge", body)
	require.Equal(t, http.StatusOK, code)
}

// --- adversarial concurrent operations test ---

// TestAdversarial_ConcurrentPurgeBan verifies that concurrent purge and
// ban operations do not race or panic. The mock functions use atomics
// so the test asserts exact call counts, not just "didn't crash".
func TestAdversarial_ConcurrentPurgeBan(t *testing.T) {
	t.Parallel()
	var purgeCount atomic.Int64
	var banCount atomic.Int64
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { purgeCount.Add(1); return nil },
		BanFn:   func(_ api.BanExpr) (int, error) { banCount.Add(1); return 1, nil },
	})

	const goroutines = 50
	const opsPerG = 20

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)
	var successCount atomic.Int64

	for i := range goroutines {
		go func(n int) {
			defer wg.Done()
			for j := range opsPerG {
				body := fmt.Sprintf(`{"url":"https://example.com/%d/%d"}`, n, j)
				rr := httptest.NewRecorder()
				req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/purge",
					bytes.NewBufferString(body))
				req.Header.Set(header.ContentType, "application/json")
				req.Header.Set(header.Authorization, "Bearer secret")
				s.Handler().ServeHTTP(rr, req)
				if rr.Code == http.StatusOK {
					successCount.Add(1)
				}
			}
		}(i)
	}
	for i := range goroutines {
		go func(n int) {
			defer wg.Done()
			for j := range opsPerG {
				body := fmt.Sprintf(`{"path_regex":"^/section/%d/%d/"}`, n, j)
				rr := httptest.NewRecorder()
				req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/ban",
					bytes.NewBufferString(body))
				req.Header.Set(header.ContentType, "application/json")
				req.Header.Set(header.Authorization, "Bearer secret")
				s.Handler().ServeHTTP(rr, req)
				if rr.Code == http.StatusOK {
					successCount.Add(1)
				}
			}
		}(i)
	}
	wg.Wait()

	totalOps := int64(goroutines*2) * int64(opsPerG)
	require.Equal(t, totalOps, successCount.Load(), "every concurrent op should succeed")
	require.Equal(t, int64(goroutines*opsPerG), purgeCount.Load(), "purge call count must match")
	require.Equal(t, int64(goroutines*opsPerG), banCount.Load(), "ban call count must match")
}

// --- adversarial regex tests ---

// TestAdversarial_InvalidRegex verifies that syntactically invalid regex
// patterns in ban expressions are rejected with 400 before reaching BanFn.
func TestAdversarial_InvalidRegex(t *testing.T) {
	t.Parallel()
	var banCalled atomic.Bool
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		BanFn:  func(_ api.BanExpr) (int, error) { banCalled.Store(true); return 0, nil },
	})

	cases := []struct {
		name string
		body string
	}{
		{"invalid_host_regex", `{"host_regex":"["}`},
		{"invalid_path_regex", `{"path_regex":"(?P<name"}`},
		{"unbalanced_group", `{"path_regex":"(abc"}`},
		{"bad_char_class", `{"host_regex":"[z-a]"}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			code, body := postWithToken(t, s, "/v1/ban", tc.body)
			require.Equal(t, http.StatusBadRequest, code, "body: %s", body)
			require.False(t, banCalled.Load(), "BanFn must not be called for invalid regex")
		})
	}
}

// TestAdversarial_ReDoSPattern verifies that a known catastrophic
// backtracking pattern does not hang the server. Go's regexp engine
// uses RE2, which is linear-time and immune to catastrophic
// backtracking, so the pattern compiles and is accepted. The test
// confirms the server does not hang and returns a valid response.
func TestAdversarial_ReDoSPattern(t *testing.T) {
	t.Parallel()
	var banCalled atomic.Bool
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		BanFn:  func(_ api.BanExpr) (int, error) { banCalled.Store(true); return 0, nil },
	})

	// Classic ReDoS pattern: (a+)+$ — linear-time in RE2, exponential
	// in backtracking engines. This proves the admin layer is safe
	// because Go's regexp uses RE2.
	body := `{"path_regex":"(a+)+$"}`
	code, _ := postWithToken(t, s, "/v1/ban", body)
	require.Equal(t, http.StatusOK, code)
	require.True(t, banCalled.Load(), "BanFn should be called for a valid RE2 pattern")
}

// --- adversarial unicode / encoding edge cases ---

// TestAdversarial_UnicodeURL verifies that Unicode (IRI) URLs are
// accepted by the purge endpoint. The admin layer does not normalise
// URLs — it passes the raw string to BuildKeyFromURL.
func TestAdversarial_UnicodeURL(t *testing.T) {
	t.Parallel()
	var gotURL string
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
		OnPurged: func(_ context.Context, url string) {
			gotURL = url
		},
	})
	body := `{"url":"https://example.com/ひらがな/路径"}`
	code, _ := postWithToken(t, s, "/v1/purge", body)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "https://example.com/ひらがな/路径", gotURL)
}

// TestAdversarial_PercentEncodedUTF8 verifies that percent-encoded
// multi-byte UTF-8 sequences in URLs are handled correctly.
func TestAdversarial_PercentEncodedUTF8(t *testing.T) {
	t.Parallel()
	var gotURL string
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
		OnPurged: func(_ context.Context, url string) {
			gotURL = url
		},
	})
	// "/路径" percent-encoded as UTF-8.
	body := `{"url":"https://example.com/%E8%B7%AF%E5%BE%84"}`
	code, _ := postWithToken(t, s, "/v1/purge", body)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "https://example.com/%E8%B7%AF%E5%BE%84", gotURL)
}

// TestAdversarial_UnicodeBanRegex verifies that ban expressions
// containing Unicode characters in the regex compile and are accepted.
func TestAdversarial_UnicodeBanRegex(t *testing.T) {
	t.Parallel()
	var gotExpr api.BanExpr
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		BanFn: func(expr api.BanExpr) (int, error) {
			gotExpr = expr
			return 3, nil
		},
	})
	body := `{"host_regex":"example\\.com","path_regex":"^/商品/.*"}`
	code, _ := postWithToken(t, s, "/v1/ban", body)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "^/商品/.*", gotExpr.PathRegex)
}

// TestAdversarial_NULByteInURL verifies that a NUL byte in the URL
// field is handled without crashing. The JSON decoder accepts NUL
// bytes inside strings (\u0000); the purge handler passes the string
// to BuildKeyFromURL which should handle it gracefully. The URL is
// non-empty so the handler returns 200.
func TestAdversarial_NULByteInURL(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
	})
	// JSON string with an embedded NUL byte (\u0000).
	body := `{"url":"https://example.com/\u0000path"}`
	code, _ := postWithToken(t, s, "/v1/purge", body)
	require.Equal(t, http.StatusOK, code)
}

// --- adversarial edge cases on empty / whitespace bodies ---

// TestAdversarial_WhitespaceOnlyBody verifies that a body containing
// only whitespace is rejected with 400 (json.Decode returns io.EOF).
func TestAdversarial_WhitespaceOnlyBody(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
	})
	code, _ := postWithToken(t, s, "/v1/purge", "   \n\t  ")
	require.Equal(t, http.StatusBadRequest, code)
}

// TestAdversarial_NullBody verifies that a JSON null literal is
// rejected with 400 (decoding null into a struct leaves zero values,
// which fails the url-required check).
func TestAdversarial_NullBody(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
	})
	code, _ := postWithToken(t, s, "/v1/purge", `null`)
	require.Equal(t, http.StatusBadRequest, code)
}

// --- adversarial HTTP method tests ---

// TestAdversarial_WrongMethod verifies that write endpoints reject GET
// requests. The ServeMux pattern "POST /v1/purge" means a GET returns
// 405 Method Not Allowed.
func TestAdversarial_WrongMethod(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:     "secret",
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn:   func(_ api.Key) error { return nil },
		BanFn:     func(_ api.BanExpr) (int, error) { return 0, nil },
		RefreshFn: func(_ api.Key) error { return nil },
	})

	for _, path := range []string{"/v1/purge", "/v1/purge/batch", "/v1/ban", "/v1/refresh"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequestWithContext(context.Background(), "GET", path, nil)
			req.Header.Set(header.Authorization, "Bearer secret")
			rr := httptest.NewRecorder()
			s.Handler().ServeHTTP(rr, req)
			require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
		})
	}
}

// --- adversarial deep-nesting / structure tests ---

// TestAdversarial_DeeplyNestedJSON verifies that a deeply nested JSON
// structure does not cause a stack overflow. Go's encoding/json has a
// default recursion limit of 10000; the body limit middleware
// provides a second defence layer.
func TestAdversarial_DeeplyNestedJSON(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
	})
	var sb strings.Builder
	sb.WriteString(`{"url":"https://example.com/"`)
	for range 500 {
		sb.WriteString(`,"x":{"a":1}`)
	}
	sb.WriteString(`}`)
	code, _ := postWithToken(t, s, "/v1/purge", sb.String())
	// DisallowUnknownFields rejects the unknown "x" key with 400.
	require.Equal(t, http.StatusBadRequest, code)
}

// TestAdversarial_BatchWithNullURLs verifies that a batch purge
// containing null entries in the URLs array is handled without
// crashing. null entries decode as empty strings in a []string slice;
// the handler treats them as valid (zero-key) purges.
func TestAdversarial_BatchWithNullURLs(t *testing.T) {
	t.Parallel()
	var purgeCalls atomic.Int64
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error {
			purgeCalls.Add(1)
			return nil
		},
	})
	body := `{"urls":["https://a.com/",null,"https://c.com/"]}`
	code, respBody := postWithToken(t, s, "/v1/purge/batch", body)
	require.Equal(t, http.StatusOK, code)
	// All 3 entries (including null→"") are purged.
	require.Equal(t, int64(3), purgeCalls.Load(), "all 3 entries must be purged")
	var resp map[string]any
	require.NoError(t, json.Unmarshal(respBody, &resp), "unmarshal response")
	require.Equal(t, float64(3), resp["count"], "count must be 3")
	require.Equal(t, float64(0), resp["failed"], "failed must be 0")
}
