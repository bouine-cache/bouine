package observability

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

func makeRespHeader(kvs ...string) *fasthttp.ResponseHeader {
	h := &fasthttp.ResponseHeader{}
	for i := 0; i < len(kvs); i += 2 {
		h.Set(kvs[i], kvs[i+1])
	}
	return h
}

func TestOriginHeaderRing_SampleAndAudit(t *testing.T) {
	t.Parallel()
	r := NewOriginHeaderRing()

	r.SampleFastHTTP("api-pool", makeRespHeader(
		header.CacheControl, "max-age=60",
		header.ETag, `"abc"`,
	), 200)
	r.SampleFastHTTP("api-pool", makeRespHeader(
		header.ETag, `"def"`,
	), 200)
	r.SampleFastHTTP("static-pool", makeRespHeader(
		header.CacheControl, "max-age=3600",
		header.LastModified, "Mon, 01 Jan 2024 00:00:00 GMT",
		header.SurrogateKey, "product-42",
	), 200)

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
	r.SampleFastHTTP("p", nil, 200)
	audit := r.HeaderAudit()
	require.Len(t, audit, 0)
}

func TestOriginHeaderRing_Wraparound(t *testing.T) {
	t.Parallel()
	r := NewOriginHeaderRing()
	for range originHeaderRingCap + 50 {
		r.SampleFastHTTP("p", makeRespHeader(header.CacheControl, "x"), 200)
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
				r.SampleFastHTTP("p", makeRespHeader(header.CacheControl, "x"), 200)
			}
		}()
	}
	wg.Wait()
	audit := r.HeaderAudit()
	s := audit["p"]
	assert.Equal(t, int64(1000), s.SampleCount)
}

func TestTruncate(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "short", truncate("short", 256))
	assert.Equal(t, "ab", truncate("abcdef", 2))
	assert.Equal(t, "", truncate("", 10))
}
