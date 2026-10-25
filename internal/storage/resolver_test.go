package storage

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage/warm"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// encodeObjectV6 mirrors the pre-v7 wire format (v6 = v7 minus the
// PairedKey bytes after the flags byte). Used to prove decodeObject's
// rolling-deploy handling of blobs written by an older build (ADR-0066):
// they decode with a zero PairedKey — a legacy resolver whose own Body
// may still be present.
func encodeObjectV6(obj *api.Object) []byte {
	buf := make([]byte, 0, len(obj.Body)+256)
	buf = append(buf, objCodecVersionV6)
	buf = append(buf, obj.Key[:]...)
	buf = appendString(buf, obj.VaryKey)
	buf = appendString(buf, obj.VaryValue)
	buf = binary.AppendUvarint(buf, uint64(obj.StatusCode))
	buf = binary.AppendVarint(buf, int64(obj.TTL))
	buf = binary.AppendVarint(buf, int64(obj.StaleWhileRevalidate))
	buf = binary.AppendVarint(buf, int64(obj.StaleIfError))
	buf = appendTime(buf, obj.StoredAt)
	buf = appendTime(buf, obj.LastModified)
	buf = binary.AppendUvarint(buf, obj.Hits)
	buf = appendString(buf, obj.ETag)
	if obj.KeepGrace {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	buf = appendString(buf, obj.Pool)
	buf = appendString(buf, obj.CacheControl)
	buf = binary.AppendVarint(buf, int64(obj.OriginAge))
	var flags byte
	if obj.HasDate {
		flags |= objFlagHasDate
	}
	if obj.RespNoCache {
		flags |= objFlagRespNoCache
	}
	if obj.RespMustRevalidate {
		flags |= objFlagRespMustRevalid
	}
	if obj.HasConnectionList {
		flags |= objFlagHasConnectionList
	}
	if obj.HasNoCacheFields {
		flags |= objFlagHasNoCacheFields
	}
	buf = append(buf, flags)
	buf = binary.AppendUvarint(buf, uint64(obj.Header.Len()))
	obj.Header.Range(func(k, v string) bool {
		buf = appendString(buf, k)
		buf = appendString(buf, v)
		return true
	})
	buf = binary.AppendUvarint(buf, uint64(len(obj.SurrogateKeys)))
	for _, sk := range obj.SurrogateKeys {
		buf = appendString(buf, sk)
	}
	buf = binary.AppendUvarint(buf, uint64(len(obj.Body)))
	buf = append(buf, obj.Body...)
	return buf
}

// TestCodec_PairedKeyRoundTrip pins codec v7 (ADR-0066): the Vary
// resolver's PairedKey survives the warm tier and the peer wire, so a
// resolver forwarded to its owner can still resolve its mirror variant.
func TestCodec_PairedKeyRoundTrip(t *testing.T) {
	t.Parallel()
	primaryKey := testkey.Hash([]byte("codec-resolver/primary"))
	variantKey := testkey.Hash([]byte("codec-resolver/variant"))
	orig := &api.Object{
		Key:        primaryKey,
		PairedKey:  variantKey,
		VaryValue:  "X-Test-Variant",
		StatusCode: 200,
		Header:     headerMap(header.ContentType, "text/html", header.ETag, `"v1"`),
		ETag:       `"v1"`,
		StoredAt:   time.Unix(1_760_000_000, 0).UTC(),
		TTL:        time.Minute,
	}

	got, err := decodeObject(encodeObject(orig))
	require.NoError(t, err, "decode v7 blob")
	assert.Equal(t, orig.PairedKey, got.PairedKey, "PairedKey must round-trip")
	assert.Equal(t, orig.Key, got.Key)
	assert.Equal(t, orig.VaryValue, got.VaryValue)
	assert.Empty(t, got.Body, "the metadata-only resolver carries no body bytes")
	assert.Zero(t, got.BodySize)
}

// TestCodec_V6BlobDecodesZeroPairedKey covers warm-tier and peer-wire
// blobs written by the v6 codec during a rolling deploy: they carry no
// PairedKey bytes, so decodeObject must yield a zero PairedKey — the
// legacy-resolver shape whose own Body may still be present.
func TestCodec_V6BlobDecodesZeroPairedKey(t *testing.T) {
	t.Parallel()
	orig := &api.Object{
		Key:        testkey.Key(42),
		VaryValue:  "X-Test-Variant",
		StatusCode: 200,
		Header:     headerMap(header.ContentType, "text/html"),
		ETag:       `"legacy"`,
		Body:       []byte("legacy resolver body"),
		StoredAt:   time.Unix(1_760_000_000, 0).UTC(),
		TTL:        time.Minute,
	}

	got, err := decodeObject(encodeObjectV6(orig))
	require.NoError(t, err, "decode v6 blob")
	assert.Equal(t, api.Key{}, got.PairedKey,
		"v6 blobs predate PairedKey — they must decode as legacy entries")
	assert.Equal(t, orig.ETag, got.ETag)
	assert.Equal(t, orig.Body, got.Body, "the legacy resolver keeps its own body")
}

// TestTiered_ResolverWarmReloadRestoresRouting pins ADR-0066's
// warm-back gate: a metadata-only resolver (BodySize 0, below any
// threshold) must still be written to the warm tier and WAL, because
// lookup() reaches warm variants only through the Vary list carried by
// the entry at the primary key. After a restart, the reloaded resolver
// must route a variant lookup straight to the warm variant — the
// stranding case the gate exists to prevent.
func TestTiered_ResolverWarmReloadRestoresRouting(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	warmDir := filepath.Join(dir, "warm")
	walPath := filepath.Join(dir, "index.wal")
	ctx := context.Background()

	newStore := func() *TieredStore {
		ts, err := NewTieredStore(TieredConfig{
			Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
			Warm:                   &warm.Config{Dir: warmDir, MaxBytes: 100 << 20, SegMax: 1 << 20},
			WALDir:                 walPath,
			BodyThreshold:          512,
			TombstoneDrainInterval: -1,
		})
		require.NoError(t, err)
		return ts
	}

	primaryKey := testkey.Hash([]byte("resolver-routing/primary"))
	variantKey := testkey.Hash([]byte("resolver-routing/variant"))

	ts1 := newStore()
	variant := bigObj(variantKey, 1024)
	variant.VaryValue = "X-Test-Variant"
	variant.ETag = `"v1"`
	require.NoError(t, ts1.Put(ctx, variantKey, variant))
	resolver := &api.Object{
		Key:        primaryKey,
		PairedKey:  variantKey,
		VaryValue:  "X-Test-Variant",
		StatusCode: 200,
		Header:     headerMap(header.ContentType, "text/html", header.ETag, `"v1"`),
		ETag:       `"v1"`,
		StoredAt:   time.Now(),
		TTL:        time.Hour,
	}
	require.NoError(t, ts1.Put(ctx, primaryKey, resolver))
	require.NoError(t, ts1.Close(ctx))

	ts2 := newStore()
	t.Cleanup(func() { _ = ts2.Close(ctx) })

	gotResolver, _, err := ts2.Get(ctx, primaryKey)
	require.NoError(t, err, "resolver Get after reopen")
	require.NotNil(t, gotResolver, "the metadata-only resolver must survive restart via the warm tier")
	assert.Equal(t, variantKey, gotResolver.PairedKey, "PairedKey survives the warm reload")
	assert.Equal(t, "X-Test-Variant", gotResolver.VaryValue, "the Vary list must survive the warm reload")

	gotVariant, _, err := ts2.Get(ctx, variantKey)
	require.NoError(t, err, "variant Get after reopen")
	require.NotNil(t, gotVariant, "the variant must be reachable through the reloaded resolver's routing")
	assert.Len(t, gotVariant.Body, 1024)
}
