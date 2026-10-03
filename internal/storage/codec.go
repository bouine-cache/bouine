package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// objCodecVersion is the warm-tier object encoding version. It is the
// first byte of every encoded blob so the decoder can reject blobs
// written by an incompatible codec (including legacy JSON blobs, which
// begin with '{' = 0x7B and therefore never collide with a version byte).
// Version 6 adds the transient-field block after Pool: the pre-merged
// CacheControl string, OriginAge, and the gate/serialization flags
// (HasDate, RespNoCache, RespMustRevalidate, HasConnectionList,
// HasNoCacheFields). They MUST survive the wire: the peer-put path
// stored v5-decoded objects with all five flags false, which served a
// duplicate Date header on every fast-path hit and silently disabled
// RFC 9111 §5.2.2 revalidation for no-cache/must-revalidate responses
// (ADR-0053). v5 blobs backfill HasDate and the gate flags from the
// header map at decode time; v4 and v3 blobs decode as before with
// KeepGrace=false, Pool="" and an empty VaryValue (v3 only).
const objCodecVersion byte = 6

// objCodecVersionV5 is the grace-stamps encoding version, still
// accepted by the decoder so warm-tier blobs and peer-wire frames
// written before the transient-field block survive a rolling deploy.
// The v5 layout is identical to v4 plus KeepGrace/Pool; the v6
// transient block is simply absent.
const objCodecVersionV5 byte = 5

// objCodecVersionV4 is the previous encoding version, still accepted by
// the decoder so warm-tier blobs written before the grace-retention
// stamps survive an upgrade. New writes always use objCodecVersion.
const objCodecVersionV4 byte = 4

// objCodecVersionV3 is the encoding version before VaryValue was added to
// the wire format. Still accepted: v3 blobs decode with an empty
// VaryValue (re-derivable from the stored headers on load).
const objCodecVersionV3 byte = 3

// objFlagsV6 packs the Object's transient boolean flags into one wire
// byte (codec v6, ADR-0053). Bit order is arbitrary but fixed; bit 7
// through bit 5 are reserved for future flags and must be written 0.
const (
	objFlagHasDate           byte = 1 << 0
	objFlagRespNoCache       byte = 1 << 1
	objFlagRespMustRevalid   byte = 1 << 2
	objFlagHasConnectionList byte = 1 << 3
	objFlagHasNoCacheFields  byte = 1 << 4
)

// errCorrupt is returned when an encoded object blob is truncated or
// otherwise malformed. TieredStore.Get treats it as a durable eviction:
// the blob is removed from the warm tier (tombstone + WAL delete) and
// the call returns a clean miss so the next Put rewrites it. This is
// distinct from warm.Get errors (CRC mismatch, segment-not-found),
// which still propagate as hard errors.
var errCorrupt = errors.New("storage: corrupt object blob")

// EncodeObject is the exported form of encodeObject. It serialises an
// Object into the compact binary form used by the warm tier and the
// peer-fetch wire protocol (issue #187): a varint-framed metadata header
// followed by the raw body bytes. The body is written last and
// length-prefixed so the decoder can alias it directly out of the
// backing blob without a copy.
//
// Stable.
func EncodeObject(obj *api.Object) []byte {
	return encodeObject(obj)
}

// EncodeObjectInto serialises an Object into the provided buffer,
// appending to it. Callers can use a sync.Pool to reuse buffers
// across calls, eliminating per-request allocation on the peer-fetch
// server path.
//
// Stable.
func EncodeObjectInto(obj *api.Object, buf []byte) []byte {
	return encodeObjectInto(obj, buf)
}

// DecodeObject is the exported form of decodeObject. The returned
// Object's Body aliases blob (no copy); callers must treat blob as
// immutable for the object's lifetime. BodySize is set to the inline
// body length. The transient CacheControl / OriginAge fields are left
// zero for the caller to re-derive from the headers.
//
// Stable.
func DecodeObject(blob []byte) (*api.Object, error) {
	return decodeObject(blob)
}

// encodeObject serialises an Object into the compact binary form used by
// the warm tier: a varint-framed metadata header followed by the raw
// body bytes. This replaces json.Marshal, which base64-encoded the body
// (~33% inflation) and paid reflection cost on every demotion. The body
// is written last and length-prefixed so decodeObject can alias it
// directly out of the backing blob without a copy.
//
// Fields tagged json:"-" on api.Object (CacheControl, OriginAge) are not
// stored; they are re-derived from the headers on load, exactly as the
// JSON path did.
func encodeObject(obj *api.Object) []byte {
	return encodeObjectInto(obj, make([]byte, 0, len(obj.Body)+256))
}

func encodeObjectInto(obj *api.Object, buf []byte) []byte {
	buf = append(buf, objCodecVersion)
	buf = append(buf, obj.Key[:]...)
	buf = appendString(buf, obj.VaryKey)
	buf = appendString(buf, obj.VaryValue)
	buf = binary.AppendUvarint(buf, uint64(obj.StatusCode)) //nolint:gosec // HTTP status is small and non-negative
	buf = binary.AppendVarint(buf, int64(obj.TTL))
	buf = binary.AppendVarint(buf, int64(obj.StaleWhileRevalidate))
	buf = binary.AppendVarint(buf, int64(obj.StaleIfError))
	buf = appendTime(buf, obj.StoredAt)
	buf = appendTime(buf, obj.LastModified)
	// Atomic load: hot.Get increments Hits under the shard lock while
	// this encoder runs outside it (tiered.writeHotOnlyToWarm), so the
	// read must pair with the increment's atomic store (issue #218).
	buf = binary.AppendUvarint(buf, atomic.LoadUint64(&obj.Hits))
	buf = appendString(buf, obj.ETag)
	// Grace-retention stamps (ADR-0051, codec v5): 1-byte KeepGrace flag
	// followed by the Pool name. Written for every object (graced or
	// not) so the field position is version-stable.
	if obj.KeepGrace {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	buf = appendString(buf, obj.Pool)

	// Transient-field block (ADR-0053, codec v6): the pre-merged
	// Cache-Control string, OriginAge, and the gate/serialization
	// flags. The header map alone cannot re-derive these faithfully on
	// every path: buildObject merges multi-line Cache-Control values
	// and applies CDN-Cache-Control precedence (RFC 9213) at fill time,
	// so re-deriving from the map can disagree with what was evaluated
	// at store time. Carrying them also keeps decodeObject O(1) in
	// header count instead of re-parsing Cache-Control per blob.
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

	// Header map: count, then (key, value) per entry.
	buf = binary.AppendUvarint(buf, uint64(obj.Header.Len())) //nolint:gosec // Len() returns int len of a slice, always non-negative and bounded by memory
	obj.Header.Range(func(k, v string) bool {
		buf = appendString(buf, k)
		buf = appendString(buf, v)
		return true
	})

	// Surrogate keys.
	buf = binary.AppendUvarint(buf, uint64(len(obj.SurrogateKeys)))
	for _, sk := range obj.SurrogateKeys {
		buf = appendString(buf, sk)
	}

	// Body last: length-prefixed raw bytes.
	buf = binary.AppendUvarint(buf, uint64(len(obj.Body)))
	buf = append(buf, obj.Body...)
	return buf
}

// decodeObject is the inverse of encodeObject. The returned Object's Body
// aliases blob (no copy); callers must treat blob as immutable for the
// object's lifetime. BodySize is set to the inline body length. The
// transient CacheControl / OriginAge fields are left zero for the caller
// to re-derive from the headers.
func decodeObject(blob []byte) (*api.Object, error) {
	r := objReader{b: blob}

	ver := r.byte()
	if r.err != nil {
		return nil, r.err
	}
	// v4 adds VaryValue after VaryKey; v3 blobs (pre-upgrade warm tier)
	// decode unchanged with an empty VaryValue — the field is re-derivable
	// from the stored headers on load, matching the v3 behavior.
	// v5 adds the grace-retention stamps after ETag; v4 and v3 blobs
	// decode with KeepGrace=false and Pool="" — grace cannot engage on
	// them, matching their pre-upgrade reap behavior (ADR-0051).
	// v6 adds the transient-field block after Pool (ADR-0053); v5-and
	// older blobs backfill HasDate and the gate flags from the header
	// map below.
	if ver != objCodecVersion && ver != objCodecVersionV5 && ver != objCodecVersionV4 && ver != objCodecVersionV3 {
		return nil, fmt.Errorf("storage: unknown object codec version %d", ver)
	}

	obj := &api.Object{}
	copy(obj.Key[:], r.bytes(16))
	obj.VaryKey = r.str()
	if ver >= objCodecVersionV4 {
		obj.VaryValue = r.str()
	}
	obj.StatusCode = int(r.uvarint()) //nolint:gosec // bounded by encoder
	obj.TTL = time.Duration(r.varint())
	obj.StaleWhileRevalidate = time.Duration(r.varint())
	obj.StaleIfError = time.Duration(r.varint())
	obj.StoredAt = r.time()
	obj.LastModified = r.time()
	obj.Hits = r.uvarint()
	obj.ETag = r.str()
	if ver >= objCodecVersionV5 {
		obj.KeepGrace = r.byte() == 1
		obj.Pool = r.str()
	}
	if ver >= objCodecVersion {
		obj.CacheControl = r.str()
		obj.OriginAge = time.Duration(r.varint())
		flags := r.byte()
		obj.HasDate = flags&objFlagHasDate != 0
		obj.RespNoCache = flags&objFlagRespNoCache != 0
		obj.RespMustRevalidate = flags&objFlagRespMustRevalid != 0
		obj.HasConnectionList = flags&objFlagHasConnectionList != 0
		obj.HasNoCacheFields = flags&objFlagHasNoCacheFields != 0
	}

	obj.Header = decodeHeaderMap(&r, r.count())

	// Pre-v6 blobs (rolling-deploy warm tier, in-flight peer frames)
	// carry no flags byte: backfill via the header map so the fast path
	// does not synthesize a second Date header on top of the stored one
	// (the duplicate-Date bug) and the RFC 9111 §5.2.2 gate is restored
	// for no-cache/must-revalidate responses.
	if ver < objCodecVersion {
		backfillPreV6Flags(obj)
	}

	if nsk := r.count(); nsk > 0 {
		sks := make([]string, 0, min(nsk, 16))
		for range nsk {
			sks = append(sks, r.str())
		}
		obj.SurrogateKeys = sks
	}

	blen := r.uvarint()
	body := r.bytes(int(blen)) //nolint:gosec // bounds-checked in bytes()
	if r.err != nil {
		return nil, r.err
	}
	obj.Body = body
	obj.BodySize = int64(len(body))

	return obj, nil
}

// appendString writes a uvarint length prefix followed by the raw bytes.
func appendString(buf []byte, s string) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

// decodeHeaderMap reads nh (key, value) pairs from r into a sorted
// header.Map. A zero count yields the empty Map.
func decodeHeaderMap(r *objReader, nh int) header.Map {
	if nh <= 0 {
		return header.Map{}
	}
	hm := header.NewMap(min(nh, 32))
	for range nh {
		k := r.str()
		v := r.str()
		hm.AppendEntry(k, v)
	}
	hm.SortEntries()
	return hm
}

// backfillPreV6Flags re-derives the transient flags that codec v6
// added to the wire format (ADR-0053) for blobs written by older
// builds: warm-tier entries from before an upgrade and peer frames
// in flight during a rolling deploy.
//
// HasDate comes straight from the header map so the fast path does not
// synthesize a second Date header on top of the stored one. The
// RFC 9111 §5.2.2 gate flags are re-parsed from the stored
// Cache-Control so a no-cache/must-revalidate response never degrades
// into an unvalidated fresh hit. Bare no-cache="fields" lists set
// neither gate flag — matching buildObject, which keeps RespNoCache
// false for field-list no-cache and only pre-computes HasNoCacheFields.
func backfillPreV6Flags(obj *api.Object) {
	obj.HasDate = obj.Header.Has(header.Date)
	cc := obj.Header.GetAll(header.CacheControl)
	if cc == "" {
		return
	}
	parsed := header.ParseCacheControl(cc)
	obj.RespNoCache = parsed.NoCache
	obj.RespMustRevalidate = parsed.MustRevalidate || parsed.ProxyRevalidate
	obj.HasNoCacheFields = parsed.NoCacheFields != ""
}

// appendTime writes a 1-byte presence flag (0 = zero time) followed, when
// present, by the varint UnixNano. This preserves IsZero() across a round
// trip, which UnixNano alone cannot (the zero time has no meaningful nanos).
func appendTime(buf []byte, t time.Time) []byte {
	if t.IsZero() {
		return append(buf, 0)
	}
	buf = append(buf, 1)
	return binary.AppendVarint(buf, t.UnixNano())
}

// objReader is a cursor over an encoded blob. Once err is set every
// subsequent read is a no-op, so callers can decode optimistically and
// check err once at the end (or at each allocation-sizing boundary).
type objReader struct {
	err error
	b   []byte
	pos int
}

func (r *objReader) byte() byte {
	b := r.bytes(1)
	if r.err != nil {
		return 0
	}
	return b[0]
}

func (r *objReader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b[r.pos:])
	if n <= 0 {
		r.err = errCorrupt
		return 0
	}
	r.pos += n
	return v
}

func (r *objReader) varint() int64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Varint(r.b[r.pos:])
	if n <= 0 {
		r.err = errCorrupt
		return 0
	}
	r.pos += n
	return v
}

// bytes returns the next n bytes as a sub-slice of the blob (no copy).
func (r *objReader) bytes(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || n > len(r.b)-r.pos {
		r.err = errCorrupt
		return nil
	}
	out := r.b[r.pos : r.pos+n]
	r.pos += n
	return out
}

func (r *objReader) str() string {
	n := r.uvarint()
	if r.err != nil {
		return ""
	}
	return string(r.bytes(int(n))) //nolint:gosec // bounds-checked in bytes()
}

func (r *objReader) time() time.Time {
	present := r.byte()
	if r.err != nil || present == 0 {
		return time.Time{}
	}
	// Reconstruct in UTC: only the instant matters for freshness math, and
	// callers that format LastModified already normalise to UTC.
	return time.Unix(0, r.varint()).UTC()
}

// count reads a uvarint element count and rejects values larger than the
// number of bytes remaining. Since every element consumes at least one
// byte, this bounds allocation sizes against a crafted or corrupt blob.
func (r *objReader) count() int {
	v := r.uvarint()
	if r.err != nil {
		return 0
	}
	if v > uint64(len(r.b)-r.pos) { //nolint:gosec // pos <= len(b) invariant: difference is non-negative
		r.err = errCorrupt
		return 0
	}
	return int(v) //nolint:gosec // bounded above by remaining length
}
