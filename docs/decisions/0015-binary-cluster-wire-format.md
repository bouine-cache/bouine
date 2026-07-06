# ADR-0015: Binary wire format for cluster key replication

- **Status**: Accepted
- **Date**: 2026-07-02
- **Deciders**: @chris.dupin
- **Phase**: phase 4.5 (hardening)

## Context

All cluster peer communication used `encoding/json`:

- **KeySet** (anti-entropy): `GET /v1/peer/keys` returned a JSON
  `KeySet{NodeName, Keys []uint64}`. ADR-0014 flagged "~8 MB of JSON per
  peer per round" for 1M keys. The JSON encoder uses reflection and
  per-field interface boxing, generating GC pressure proportional to key
  count.
- **Gossip purge/ban**: `NotifyMsg` did **two** `json.Unmarshal` calls per
  message — one to peek the `"type"` discriminator, one to decode the full
  event. Reflection allocations on every gossip message.
- **HTTP peer-purge/peer-ban**: the admin server JSON-decoded the body
  before calling the purge/ban function.

The `ReplicationEvent` (full-object gossip in `full` mode) also uses JSON,
but its `Object` payload contains an `http.Header` map and body bytes —
complex enough that a binary encoder would not justify the complexity.

The peer-fetch HTTP response (`POST /v1/peer/fetch`) previously used JSON
as well, re-encoding the storage layer's compact binary `Object` into JSON
on every fetch. This caused ~200 MB/s of allocation churn in preprod
(issue #187): `header.Map.MarshalJSON` allocated a `map[string][]string`
per header, `json.Marshal` base64-encoded the body (33% inflation), and
the JSON decoder on the requesting side reversed all of it. The
peer-fetch response now uses the storage layer's binary object codec
(`storage.EncodeObject` / `storage.DecodeObject`) directly — no JSON
tower, no base64, no reflection. The request body stays JSON (3 fields,
trivially small). See the "Peer-fetch response" section below.

## Decision

Replace JSON with a hand-rolled binary codec (stdlib `encoding/binary`
only, no new dependencies) for **KeySet**, **PurgeEvent**, and
**BanEvent**. Keep JSON for **ReplicationEvent**.

### Wire format

All integers are little-endian. A magic byte (`0x42`, ASCII `'B'`)
prefixes every binary frame so `NotifyMsg` can distinguish binary from
JSON (replication) by checking the first byte (`'{'` = 0x7B vs `'B'` =
0x42).

**Gossip frame** (purge/ban over memberlist):

```
[magic:1][version:1][msgType:1][payload]
```

- `msgType`: 1 = purge, 2 = ban

**HTTP keyset** (`GET /v1/peer/keys`):

```
[magic:1][version:1][nameLen:2][name:N][keyCount:4][keys: N×8]
```

**HTTP purge** (`POST /v1/peer/purge`):

```
[magic:1][version:1][key:8][varyKeyLen:2][varyKey:N][issuerLen:2][issuer:N][issuedAt:8][seq:8]
```

**HTTP ban** (`POST /v1/peer/ban`):

```
[magic:1][version:1][hostRegexLen:2][hostRegex:N][pathRegexLen:2][pathRegex:N]
[surrogateKeyLen:2][surrogateKey:N][createdAt:8][issuerLen:2][issuer:N][issuedAt:8][seq:8]
```

**HTTP peer-fetch response** (`POST /v1/peer/fetch`, 200 OK):

```
Content-Type: application/octet-stream
Body: storage.EncodeObject(obj) — varint-framed metadata + raw body bytes
```

The response uses the storage layer's object codec (`internal/storage/codec.go`),
not the cluster gossip codec. This is intentional: the peer-fetch response
carries the same `*api.Object` that the warm tier stores on disk, so reusing
the storage codec eliminates a binary→JSON→binary translation tower (issue
#187). A 404 response signals a miss (no body); a 200 response always
contains a valid encoded object.

The peer-fetch *request* remains JSON (`PeerFetchRequest`: key, vary_key,
hops — 3 fields, trivially small). The allocation problem was in the
response path, not the request.

Timestamps are encoded as `int64` unix-nano; a zero `time.Time` maps to 0
and round-trips correctly (Go's `time.Time{}.UnixNano()` returns a huge
negative that does not invert cleanly).

### Hard cutover

No JSON fallback. `ClusterProtocolVersion` bumped from `"1"` to `"2"`.
All nodes in a cluster must upgrade together — a mixed-version cluster
will have v2 nodes that cannot decode v1 JSON gossip and vice versa.
This is acceptable for bouine's target deployment model (small clusters,
coordinated rolling restarts).

### Layering

The binary codec lives in `internal/cluster/codec.go`. The admin server
(L4) does not import the cluster package (L3) — instead,
`NewPeerPurgeHandler` and `NewPeerBanHandler` are `http.Handler`
constructors in the cluster package, wired into the admin mux the same
way `PeerKeysHandler` and `PeerFetchHandler` already were.

The peer-fetch response codec (`storage.EncodeObject` /
`storage.DecodeObject`) lives in `internal/storage` (L2). The cluster
package (L5) imports it directly — L5→L2 is an allowed dependency. This
couples the peer-fetch wire format to the storage object codec version
byte (`objCodecVersion`). The coupling is intentional: peer-fetch
responses carry the same bytes the warm tier stores, so a version bump
in the storage codec is a wire-format change for peer-fetch too. A
future refactor could extract a shared `ObjectCodec` interface in the
cluster package to decouple the two, but the current design favours
zero-copy translation over indirection.

## Consequences

### Positive

- **Zero reflection** on the KeySet and gossip purge/ban paths. One
  `make` per encode, one `make` per decode — no per-field boxing, no
  string interning.
- **~50% smaller** KeySet payload (8 bytes/key vs ~20 ASCII digits +
  commas + brackets).
- **Single decode pass** for gossip: the msgType byte replaces the JSON
  `"type"` peek, eliminating the double-unmarshal.
- **No new dependencies** — stdlib `encoding/binary` only.
- `KeySet` was already `Unstable`, so the wire-format change does not
  require a public API migration.

### Negative / trade-offs

- **No rolling upgrade**: all nodes must upgrade together. Mitigated by
  bouine's deployment model (small clusters, coordinated restarts).
- **Binary is harder to debug** than JSON. Mitigated by the versioned
  magic byte; a future debug tool can decode frames offline.
- `PurgeEvent` and `BanEvent` Go structs are marked `Stable`, but the
  wire format changed. This is fine — the `Stable` annotation governs
  the Go type shape for SDK consumers, not the internal cluster wire
  format.

### Risks

- A codec bug could silently corrupt gossip messages. Mitigated by
  round-trip tests in `codec_test.go` and the existing gossip tests.
- The `time.Time` zero-value edge case (handled by `encodeTime` /
  `decodeTime` helpers).

## Alternatives considered

- **Protobuf**: would require a new dependency (ADR + `deps.md` + `deps`-
  labelled reviewer + codegen). Overkill for `[]uint64` and two event
  structs. Rejected.
- **Pool JSON buffers**: does not fix the root cause — `encoding/json`'s
  reflection allocates per-field regardless of buffer reuse. Rejected.
- **Content negotiation (Accept header)**: would allow rolling upgrades
  but adds complexity for a small-cluster product. Rejected in favour of
  hard cutover.

## References

- ADR-0014: Anti-entropy reconciliation for full cluster mode
- ADR-0008: Cluster consistency modes — strong, eventual, full
- `internal/cluster/codec.go` — the binary codec
- `internal/cluster/handlers.go` — peer-purge/peer-ban HTTP handlers
