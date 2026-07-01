package observability

import (
	"net/http"
	"sync"
	"testing"

	"github.com/thylong/bouine/pkg/header"
)

func TestOriginHeaderRing_SampleAndAudit(t *testing.T) {
	t.Parallel()
	r := NewOriginHeaderRing()

	r.Sample("api-pool", http.Header{
		header.CacheControl: []string{"max-age=60"},
		"Etag":              []string{"\"abc\""},
	}, 200)
	r.Sample("api-pool", http.Header{
		"Etag": []string{"\"def\""},
	}, 200)
	r.Sample("static-pool", http.Header{
		header.CacheControl: []string{"max-age=3600"},
		header.LastModified: []string{"Mon, 01 Jan 2024 00:00:00 GMT"},
		header.SurrogateKey: []string{"product-42"},
	}, 200)

	audit := r.HeaderAudit()
	if len(audit) != 2 {
		t.Fatalf("expected 2 pools in audit, got %d", len(audit))
	}

	api := audit["api-pool"]
	if api.SampleCount != 2 {
		t.Errorf("api-pool sample count: want 2, got %d", api.SampleCount)
	}
	if api.HasCacheControlPct != 50 {
		t.Errorf("api-pool CC%%: want 50, got %f", api.HasCacheControlPct)
	}
	if api.HasETagPct != 100 {
		t.Errorf("api-pool ETag%%: want 100, got %f", api.HasETagPct)
	}
	if api.SampleCacheControl != "max-age=60" {
		t.Errorf("api-pool sample CC: want 'max-age=60', got %q", api.SampleCacheControl)
	}

	st := audit["static-pool"]
	if st.HasLastModifiedPct != 100 {
		t.Errorf("static-pool LM%%: want 100, got %f", st.HasLastModifiedPct)
	}
	if st.HasSurrogateKeyPct != 100 {
		t.Errorf("static-pool SK%%: want 100, got %f", st.HasSurrogateKeyPct)
	}
}

func TestOriginHeaderRing_NilHeader(t *testing.T) {
	t.Parallel()
	r := NewOriginHeaderRing()
	r.Sample("p", nil, 200)
	audit := r.HeaderAudit()
	if len(audit) != 0 {
		t.Fatalf("expected empty audit for nil header, got %d pools", len(audit))
	}
}

func TestOriginHeaderRing_Wraparound(t *testing.T) {
	t.Parallel()
	r := NewOriginHeaderRing()
	// Fill past capacity to verify circular buffer wraparound.
	for range originHeaderRingCap + 50 {
		r.Sample("p", http.Header{header.CacheControl: []string{"x"}}, 200)
	}
	audit := r.HeaderAudit()
	s := audit["p"]
	if s.SampleCount != originHeaderRingCap {
		t.Errorf("sample count after wraparound: want %d, got %d", originHeaderRingCap, s.SampleCount)
	}
	if s.HasCacheControlPct != 100 {
		t.Errorf("CC%% after wraparound: want 100, got %f", s.HasCacheControlPct)
	}
}

func TestOriginHeaderRing_Concurrent(t *testing.T) {
	t.Parallel()
	r := NewOriginHeaderRing()
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				r.Sample("p", http.Header{header.CacheControl: []string{"x"}}, 200)
			}
		}()
	}
	wg.Wait()
	audit := r.HeaderAudit()
	s := audit["p"]
	if s.SampleCount != 1000 {
		t.Errorf("concurrent sample count: want 1000, got %d", s.SampleCount)
	}
}
