package dashboard

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/api"
)

func TestHandler_ClusterWithStoreData(t *testing.T) {
	rings := observability.NewRings("self")
	h := &Handler{
		cfg: Config{
			Token:        "test",
			HotMaxBytes:  512 << 20, // 512 MiB
			WarmMaxBytes: 20 << 30,  // 20 GiB
			StoreFn: func() api.Stats {
				return api.Stats{
					HotBytes:    14 << 20, // 14 MiB
					HotEntries:  4303,
					WarmBytes:   9 << 20, // 9 MiB
					WarmEntries: 110,
					Evictions:   0,
				}
			},
			Rings: rings,
		},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", nil),
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/dashboard/cluster", nil)
	h.cluster(w, r)

	body := w.Body.String()
	if !strings.Contains(body, "total cached") {
		t.Error("expected 'total cached' in response body")
	}
	if !strings.Contains(body, "total objects") {
		t.Error("expected 'total objects' in response body")
	}
	if !strings.Contains(body, "tier-bar-fill warm") {
		t.Error("expected 'tier-bar-fill warm' class in response body")
	}
	// Legend should contain percentage for hot bytes share.
	if !strings.Contains(body, "(61%)") {
		t.Errorf("expected hot bytes percentage '(61%%)' in legend, body snippet: %s",
			substringAround(body, "cache-legend", 200))
	}
	if !strings.Contains(body, "(39%)") {
		t.Errorf("expected warm bytes percentage '(39%%)' in legend, body snippet: %s",
			substringAround(body, "cache-legend", 200))
	}
	// Entries legend: 4303 / 4413 ≈ 98%, 110 / 4413 ≈ 2%.
	if !strings.Contains(body, "(98%)") {
		t.Errorf("expected hot entries percentage '(98%%)' in legend")
	}
	if !strings.Contains(body, "(2%)") {
		t.Errorf("expected warm entries percentage '(2%%)' in legend")
	}
}

func TestHandler_ClusterWithoutStoreData(t *testing.T) {
	rings := observability.NewRings("self")
	h := &Handler{
		cfg:  Config{Token: "test", Rings: rings},
		auth: newSessionAuth("test"),
		agg:  NewAggregator(rings, nil, "self:9999", nil),
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/dashboard/cluster", nil)
	h.cluster(w, r)

	body := w.Body.String()
	if strings.Contains(body, "c-cache-bytes") {
		t.Error("cache storage section should be absent when StoreFn is nil")
	}
	if strings.Contains(body, "total cached") {
		t.Error("'total cached' should be absent when StoreFn is nil")
	}
}

// substringAround returns a window of +/- n chars around the first
// occurrence of needle in s, for debugging test failures.
func substringAround(s, needle string, n int) string {
	idx := strings.Index(s, needle)
	if idx < 0 {
		return "(not found)"
	}
	start := idx - n
	if start < 0 {
		start = 0
	}
	end := idx + len(needle) + n
	if end > len(s) {
		end = len(s)
	}
	return s[start:end]
}
