//go:build integration

package integration_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/test/integration/driver"
)

// TestStrong_CompressedVariantPeerFetch pins the cluster behavior of
// fill-path edge compression (docs/plans/edge-compression.md): a node
// fills an identity HTML response, stores it gzip-compressed under the
// negotiated AE bucket, and another node must peer-fetch HIT those
// compressed bytes with no second origin request.
func TestStrong_CompressedVariantPeerFetch(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong", CompressionRoute: true})

	path := "/compressed/html?x=e2e"
	gzipAE := "gzip, deflate, br"

	get := func(n int, ae string) *driver.Response {
		return s.GetWithHostAndHeaders(t, n, path, driver.CrossNodeHost, map[string]string{
			"Accept-Encoding": ae,
		})
	}

	// Warm node 0 with a br-capable client: bucket is br (ties prefer br),
	// so the stored variant must be brotli-encoded.
	resp := get(0, gzipAE)
	require.Equal(t, "MISS", resp.Header.Get("X-Cache"))
	require.Equal(t, "br", resp.Header.Get("Content-Encoding"),
		"br-capable client must get the br-compressed variant")

	// Second request on node 0 hits the stored compressed variant.
	resp = get(0, gzipAE)
	require.Equal(t, "HIT", resp.Header.Get("X-Cache"))
	brBody := resp.Body
	require.Less(t, len(brBody), 70*1024, "compressed body must be smaller than the ~64 KiB raw body")

	// Node 1 asks with a different spelling of the same negotiation:
	// it must peer-fetch HIT node 0's stored compressed variant.
	before := s.OriginRequests()
	respB := get(1, "br, gzip, deflate")
	require.Equal(t, "HIT", respB.Header.Get("X-Cache"),
		"node 1 must peer-fetch the compressed variant stored by node 0")
	require.Equal(t, "br", respB.Header.Get("Content-Encoding"))
	require.Equal(t, brBody, respB.Body, "peer-fetched bytes must equal the stored compressed variant")
	require.Equal(t, before, s.OriginRequests(), "no second origin fill for the shared bucket")
}

// TestStrong_CompressionGzipBucket verifies the gzip bucket end-to-end:
// a gzip-only client fills, and the stored variant is gzip with a
// decodable body.
func TestStrong_CompressionGzipBucket(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong", CompressionRoute: true})

	path := "/compressed/html?x=gz"
	get := func(n int) *driver.Response {
		return s.GetWithHostAndHeaders(t, n, path, driver.CrossNodeHost, map[string]string{
			"Accept-Encoding": "gzip",
		})
	}

	resp := get(0)
	require.Equal(t, "MISS", resp.Header.Get("X-Cache"))
	require.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))

	// The MISS body itself is already gzip: decode it directly.
	gz, err := gzip.NewReader(bytes.NewReader(resp.Body))
	require.NoError(t, err, "fill body must be valid gzip")
	raw, err := io.ReadAll(gz)
	require.NoError(t, err)
	require.Contains(t, string(raw), "Lorem ipsum", "decompressed body must be the origin HTML")
	rawLen := len("<div class=\"card\"><h2>Product name</h2><p>Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt.</p></div>\n")
	require.Equal(t, 700*rawLen, len(raw))

	// Second request hits the stored compressed variant verbatim.
	resp = get(0)
	require.Equal(t, "HIT", resp.Header.Get("X-Cache"))
	require.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
}
