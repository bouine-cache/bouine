package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/thylong/bouine/pkg/api"
	"github.com/thylong/bouine/pkg/header"
)

// objCodecVersion is the warm-tier object encoding version. It is the
// first byte of every encoded blob so the decoder can reject blobs
// written by an incompatible codec (including legacy JSON blobs, which
// begin with '{' = 0x7B and therefore never collide with a version byte).
const objCodecVersion byte = 2

// errCorrupt is returned when an encoded object blob is truncated or
// otherwise malformed. The caller (TieredStore.Get) treats it like any
// other store error: log and fall through to a miss.
var errCorrupt = errors.New("storage: corrupt object blob")

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
	buf := make([]byte, 0, len(obj.Body)+256)

	buf = append(buf, objCodecVersion)
	buf = binary.AppendUvarint(buf, uint64(obj.Key))
	buf = appendString(buf, obj.VaryKey)
	buf = binary.AppendUvarint(buf, uint64(obj.StatusCode)) //nolint:gosec // HTTP status is small and non-negative
	buf = binary.AppendVarint(buf, int64(obj.TTL))
	buf = binary.AppendVarint(buf, int64(obj.StaleWhileRevalidate))
	buf = binary.AppendVarint(buf, int64(obj.StaleIfError))
	buf = appendTime(buf, obj.StoredAt)
	buf = appendTime(buf, obj.LastModified)
	buf = binary.AppendUvarint(buf, obj.Hits)
	buf = appendString(buf, obj.ETag)

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
	if ver != objCodecVersion {
		return nil, fmt.Errorf("storage: unknown object codec version %d", ver)
	}

	obj := &api.Object{}
	obj.Key = api.Key(r.uvarint())
	obj.VaryKey = r.str()
	obj.StatusCode = int(r.uvarint()) //nolint:gosec // bounded by encoder
	obj.TTL = time.Duration(r.varint())
	obj.StaleWhileRevalidate = time.Duration(r.varint())
	obj.StaleIfError = time.Duration(r.varint())
	obj.StoredAt = r.time()
	obj.LastModified = r.time()
	obj.Hits = r.uvarint()
	obj.ETag = r.str()

	if nh := r.count(); nh > 0 {
		hm := header.NewMap(min(nh, 32))
		for range nh {
			k := r.str()
			v := r.str()
			hm.AppendEntry(k, v)
		}
		obj.Header = hm
	} else {
		obj.Header = header.Map{}
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
	b   []byte
	pos int
	err error
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
