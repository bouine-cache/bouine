//go:build integration

package integration_test

import (
	"encoding/binary"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/cache"
	"github.com/bouine-cache/bouine/internal/storage/warm"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
	"github.com/bouine-cache/bouine/test/integration/driver"
)

// This file pins ADR-0068's legacy-resolver immunity at cluster level.
// A warm-tier blob written by a pre-ADR build decodes as a BODYFUL
// resolver (VaryKey blank by protocol, VaryValue set, PairedKey zero —
// codec v6). Such an entry must never leak its embedded body to any
// client: the owner's peer-fetch withhold gate 404s blank and asserted
// asks against it, so requesters fall back to their own origin fills,
// and direct lookups route variant-misses to fresh fills. The poisoned
// body bytes below exist nowhere in the origin's vocabulary, so any
// response carrying them is a leak by construction.

// appendV6String writes the codec's string layout: uvarint length +
// raw bytes. Mirrors internal/storage's appendString — copied here
// because the integration package cannot reach the codec's unexported
// helpers, and the blob must be byte-exact for decodeObject.
func appendV6String(buf []byte, s string) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

// appendV6Time writes the codec's time layout: zero-marker byte or
// 1 + unix-nano varint. Mirrors internal/storage's appendTime.
func appendV6Time(buf []byte, t time.Time) []byte {
	if t.IsZero() {
		return append(buf, 0)
	}
	buf = append(buf, 1)
	return binary.AppendVarint(buf, t.UnixNano())
}

// encodeLegacyV6Resolver builds the pre-ADR-0068 wire format for a
// bodyful Vary resolver (codec version 6 — v7 minus the PairedKey
// bytes after the flags byte). Byte-exact mirror of
// internal/storage/resolver_test.go's encodeObjectV6, pinned there by
// TestCodec_V6BlobDecodesZeroPairedKey; a v6 blob decodes with a zero
// PairedKey, which is exactly the legacy shape this test injects.
func encodeLegacyV6Resolver(obj *api.Object) []byte {
	buf := make([]byte, 0, len(obj.Body)+256)
	buf = append(buf, 6) // objCodecVersionV6, pre-PairedKey layout
	buf = append(buf, obj.Key[:]...)
	buf = appendV6String(buf, obj.VaryKey)
	buf = appendV6String(buf, obj.VaryValue)
	buf = binary.AppendUvarint(buf, uint64(obj.StatusCode))
	buf = binary.AppendVarint(buf, int64(obj.TTL))
	buf = binary.AppendVarint(buf, int64(obj.StaleWhileRevalidate))
	buf = binary.AppendVarint(buf, int64(obj.StaleIfError))
	buf = appendV6Time(buf, obj.StoredAt)
	buf = appendV6Time(buf, obj.LastModified)
	buf = binary.AppendUvarint(buf, obj.Hits)
	buf = appendV6String(buf, obj.ETag)
	if obj.KeepGrace {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	buf = appendV6String(buf, obj.Pool)
	buf = appendV6String(buf, obj.CacheControl)
	buf = binary.AppendVarint(buf, int64(obj.OriginAge))
	// The v6 transient-field flags byte: no Date, no no-cache/must-
	// revalidate, no Connection list — a plain cacheable response.
	buf = append(buf, 0)
	buf = binary.AppendUvarint(buf, uint64(obj.Header.Len()))
	obj.Header.Range(func(k, v string) bool {
		buf = appendV6String(buf, k)
		buf = appendV6String(buf, v)
		return true
	})
	buf = binary.AppendUvarint(buf, uint64(len(obj.SurrogateKeys)))
	for _, sk := range obj.SurrogateKeys {
		buf = appendV6String(buf, sk)
	}
	buf = binary.AppendUvarint(buf, uint64(len(obj.Body)))
	return append(buf, obj.Body...)
}

// TestStrong_LegacyResolverNeverServed boots a warm-tier cluster with
// a pre-upgrade bodyful resolver blob injected at a fresh URL's
// primary key in EVERY node's warm directory (the ring may place the
// primary anywhere; covering all nodes needs no ring knowledge). No
// variant and no metadata resolver for that URL exists anywhere else.
// Every request for the URL must return the origin's own per-language
// body — the injected resolver's embedded bytes must never surface,
// neither to a direct client lookup nor to a peer answering a blank
// assertion ask.
func TestStrong_LegacyResolverNeverServed(t *testing.T) {
	s := driver.BootCluster(t, driver.ClusterOptions{Mode: "strong", WarmTier: true})

	const path = "/vary-legacy"
	primaryKey := cache.BuildKeyFast(
		[]byte("GET"), []byte(path), []byte(driver.CrossNodeHost), []byte(path), false, nil)

	poisoned := []byte("legacy-resolver-body-POISONED")
	hm := header.NewMap(2)
	hm.AppendEntry("Cache-Type", "text/plain")
	hm.AppendEntry(header.Vary, "Accept-Language")
	blob := encodeLegacyV6Resolver(&api.Object{
		Key:        primaryKey,
		VaryKey:    "", // resolver by protocol
		VaryValue:  "Accept-Language",
		StatusCode: 200,
		TTL:        time.Hour,
		StoredAt:   time.Now().UTC(),
		ETag:       `"legacy"`,
		Header:     hm,
		Body:       poisoned,
	})

	// Inject the blob into every node's warm tier while the node is
	// down, then bring the fleet back. The warm segments are the
	// durable format, so a second warm.Store handle writing into the
	// same directory is exactly what the reloaded node will read.
	for n := range 3 {
		s.KillNode(t, n)
		w, err := warm.NewStore(warm.Config{Dir: s.NodeWarmDir(n)})
		require.NoError(t, err, "open warm store of killed node %d", n)
		_, _, err = w.Put(primaryKey, blob)
		require.NoError(t, err, "inject legacy resolver blob into node %d", n)
		require.NoError(t, w.Close())
	}
	for n := range 3 {
		s.RestartNode(t, n)
	}
	for n := range 3 {
		s.WaitNodeReady(t, n, 30*time.Second)
	}

	get := func(n int, lang string) *driver.Response {
		return s.GetWithHostAndHeaders(t, n, path, driver.CrossNodeHost, map[string]string{
			"Accept-Language": lang,
		})
	}

	// Every node × every language must answer with the origin's own
	// per-language body. Whichever node owns the primary loads the
	// legacy resolver from its warm tier: its blank-ask withhold gate
	// must 404 peer fetches (so non-owners fill their own variants),
	// and its own variant-miss lookups must route to fresh fills. The
	// poisoned bytes appear in no legitimate response.
	for _, n := range s.AliveNodes() {
		for _, lang := range []string{"fr", "it", "de"} {
			resp := get(n, lang)
			require.Equal(t, 200, resp.StatusCode)
			require.Contains(t, string(resp.Body), "lang="+lang,
				"node %d lang %s: the legacy resolver must not mask the origin fill", n, lang)
			require.NotContains(t, string(resp.Body), "POISONED",
				"node %d lang %s: a pre-upgrade bodyful resolver leaked its embedded body — cache poisoning", n, lang)
			cl, err := strconv.Atoi(resp.Header.Get("Content-Length"))
			require.NoError(t, err)
			require.Equal(t, len(resp.Body), cl,
				"node %d lang %s: Content-Length must describe the served fill, never the resolver", n, lang)
		}
	}
}
