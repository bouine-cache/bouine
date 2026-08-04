package observability

import (
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/header"
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
	require.Len(t, audit, 2)

	api := audit["api-pool"]
	assert.Equal(t, int64(2), api.SampleCount)
	assert.Equal(t, float64(50), api.HasCacheControlPct)
	assert.Equal(t, float64(100), api.HasETagPct)
	assert.Equal(t, "max-age=60", api.SampleCacheControl)

	st := audit["static-pool"]
	assert.Equal(t, float64(100), st.HasLastModifiedPct)
	assert.Equal(t, float64(100), st.HasSurrogateKeyPct)
}

func TestOriginHeaderRing_NilHeader(t *testing.T) {
	t.Parallel()
	r := NewOriginHeaderRing()
	r.Sample("p", nil, 200)
	audit := r.HeaderAudit()
	require.Len(t, audit, 0)
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
	assert.Equal(t, int64(originHeaderRingCap), s.SampleCount)
	assert.Equal(t, float64(100), s.HasCacheControlPct)
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
	assert.Equal(t, int64(1000), s.SampleCount)
}
