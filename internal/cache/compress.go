package cache

import (
	"bytes"
	"io"
	"strings"
	"sync"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	brotli "github.com/molecule-man/go-brrr"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/pkg/header"
)

// Fill-path edge compression (docs/plans/edge-compression.md): on a
// cacheable fill whose origin response is identity-encoded and eligible,
// the body is compressed to the negotiated Accept-Encoding bucket coding
// before storage. The hit path never calls into this file.

// Compress flush interval: every compressFlushEvery input bytes the writer
// is sync-flushed so the filling client still sees progressive delivery
// while the compressed bytes stream to the store buffer. 128 KiB keeps the
// ratio loss under ~5% while bounding client-side batching.
const compressFlushEvery = 128 * 1024

// Compression levels are tuned for latency on bodies up to 1 MiB, not
// ratio: the fill already pays the origin RTT + transfer.
const (
	// gzipLevel 5 balances speed (~15-20 ms/MiB) against ratio; flate's
	// BestSpeed (1) is roughly 2x faster at ~15% worse ratio and exists
	// as the fallback if the fill p99 gate is tight.
	gzipLevel = 5
	// brLevel 4: brotli q11 is an order of magnitude slower and never
	// acceptable on the fill path.
	brLevel = 4
)

// streamCompressor is a pooled, re-usable streaming compressor for one
// content coding. The encoder writes into two sinks: out (a small scratch
// buffer the compressor drains from) and, on drain, buf (the store
// buffer). They MUST be distinct buffers: the encoder's huffman writer
// holds a reference to out across Write/Flush/Close, and draining does
// buf.Write(out.Bytes()) followed by out.Reset() — if buf == out, Reset
// truncates the store copy to zero. Encoders are NOT safe for concurrent
// use, so all calls on a streamCompressor happen on the single fill
// goroutine.
type streamCompressor struct {
	// gz, zs, br hold the active encoder; exactly one is non-nil.
	gz *gzip.Writer
	zs *zstd.Encoder
	br *brotli.Writer
	// out is the encoder's output scratch buffer; drained into buf.
	out *bytes.Buffer
	// buf is the store buffer receiving the drained compressed bytes.
	// Set at acquire time together with out; never aliased with out.
	buf    *bytes.Buffer
	coding string
}

var (
	gzipCompressorPool = sync.Pool{New: func() any { return &streamCompressor{coding: "gzip"} }}
	zstdCompressorPool = sync.Pool{New: func() any { return &streamCompressor{coding: "zstd"} }}
	brCompressorPool   = sync.Pool{New: func() any { return &streamCompressor{coding: "br"} }}
)

// acquireCompressor returns a reset compressor for coding ("gzip",
// "zstd" or "br") writing into out, or nil for an unknown coding. out is
// the encoder's scratch sink; storeBuf receives the drained bytes and
// must NOT be the same buffer as out.
func acquireCompressor(coding string, out, storeBuf *bytes.Buffer) *streamCompressor {
	var pool *sync.Pool
	switch coding {
	case "gzip":
		pool = &gzipCompressorPool
	case "zstd":
		pool = &zstdCompressorPool
	case "br":
		pool = &brCompressorPool
	default:
		return nil
	}
	sc := pool.Get().(*streamCompressor)
	sc.out = out
	sc.buf = storeBuf
	out.Reset()
	storeBuf.Reset()
	switch coding {
	case "gzip":
		if sc.gz == nil {
			w, err := gzip.NewWriterLevel(out, gzipLevel)
			if err != nil {
				pool.Put(sc)
				return nil
			}
			sc.gz = w
		} else {
			sc.gz.Reset(out)
		}
	case "zstd":
		if sc.zs == nil {
			// Concurrency 1: the fill path is single-goroutine per stream;
			// a single worker avoids the encoder's internal job queue
			// (which spawns goroutines per stream) entirely.
			e, err := zstd.NewWriter(out, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
			if err != nil {
				pool.Put(sc)
				return nil
			}
			sc.zs = e
		} else {
			sc.zs.Reset(out)
		}
	case "br":
		if sc.br == nil {
			w, err := brotli.NewWriter(out, brLevel)
			if err != nil {
				pool.Put(sc)
				return nil
			}
			sc.br = w
		} else {
			sc.br.Reset(out)
		}
	}
	return sc
}

// releaseCompressor returns sc to its pool. Encoders are closed only via
// finish; release does not flush remaining input.
func releaseCompressor(sc *streamCompressor) {
	sc.out = nil
	sc.buf = nil
	switch sc.coding {
	case "gzip":
		gzipCompressorPool.Put(sc)
	case "zstd":
		zstdCompressorPool.Put(sc)
	case "br":
		brCompressorPool.Put(sc)
	}
}

// write feeds p to the encoder.
func (sc *streamCompressor) write(p []byte) error {
	var err error
	switch sc.coding {
	case "gzip":
		_, err = sc.gz.Write(p)
	case "zstd":
		_, err = sc.zs.Write(p)
	case "br":
		_, err = sc.br.Write(p)
	}
	return err
}

// flush sync-flushes the encoder so buffered output reaches out now.
func (sc *streamCompressor) flush() error {
	switch sc.coding {
	case "gzip":
		return sc.gz.Flush()
	case "zstd":
		return sc.zs.Flush()
	case "br":
		return sc.br.Flush()
	}
	return nil
}

// close finalizes the stream (writes the trailer/checksum).
func (sc *streamCompressor) close() error {
	switch sc.coding {
	case "gzip":
		return sc.gz.Close()
	case "zstd":
		return sc.zs.Close()
	case "br":
		return sc.br.Close()
	}
	return nil
}

// compressBytes is the buffered-path one-shot compressor: it compresses
// src fully and returns the compressed bytes, or ok=false when compression
// expands the input (incompressible body — the caller keeps the identity
// bytes). Used by the buffered fill path and by tests.
func compressBytes(coding string, src []byte) (dst []byte, ok bool) {
	var out, store bytes.Buffer
	sc := acquireCompressor(coding, &out, &store)
	if sc == nil {
		return nil, false
	}
	if err := sc.write(src); err != nil {
		releaseCompressor(sc)
		return nil, false
	}
	if err := sc.close(); err != nil {
		releaseCompressor(sc)
		return nil, false
	}
	// The encoder wrote straight into out; copy out into dst (store is
	// unused on this one-shot path — both sinks exist because the pooled
	// encoder always has the pair, and drain is what fills store).
	res := out.Bytes()
	releaseCompressor(sc)
	if len(res) >= len(src) {
		return nil, false
	}
	cp := make([]byte, len(res))
	copy(cp, res)
	return cp, true
}

// CompressionSpec is the wire shape of routes.*.cache.compression as the
// cache layer receives it from the builder (the config package's
// CompressionConfig is mirrored here so internal/cache stays free of
// config imports). Zero value = disabled.
//
//nolint:govet // fieldalignment: fields mirror the config-file order
type CompressionSpec struct {
	Enabled      bool
	Encodings    []string
	ContentTypes []string
	MinSize      int64
	MaxSize      int64
}

// compressionPolicy is the boot-compiled per-route compression config:
// nil when disabled (the common case — one nil check per fill). All
// matching is precompiled so the fill path does no string parsing beyond
// the Content-Type essence extraction.
//
//nolint:govet // fieldalignment: pointer-heavy fields first
type compressionPolicy struct {
	// encodings is the set of codings this route may produce; empty
	// means all supported. Keyed by the canonical bucket token.
	encodings map[string]bool
	// contentTypes holds lower-cased essences; a "type/*" entry is
	// stored as "type/" prefix matcher via ctWildcardPre.
	contentTypes  map[string]bool
	ctWildcardPre []string
	minSize       int64
	maxSize       int64
}

// newCompressionPolicy compiles cfg; returns nil when disabled.
func newCompressionPolicy(spec CompressionSpec) *compressionPolicy {
	if !spec.Enabled {
		return nil
	}
	encodings, minSize, maxSize, contentTypes := spec.Encodings, spec.MinSize, spec.MaxSize, spec.ContentTypes
	p := &compressionPolicy{
		encodings:    make(map[string]bool, len(encodings)),
		minSize:      minSize,
		maxSize:      maxSize,
		contentTypes: make(map[string]bool, len(contentTypes)),
	}
	if p.minSize <= 0 {
		p.minSize = 1024
	}
	if p.maxSize <= 0 {
		p.maxSize = 1 << 20
	}
	for _, e := range encodings {
		p.encodings[strings.ToLower(strings.TrimSpace(e))] = true
	}
	for _, ct := range contentTypes {
		essence := strings.ToLower(strings.TrimSpace(ct))
		if strings.HasSuffix(essence, "/*") {
			p.ctWildcardPre = append(p.ctWildcardPre, strings.TrimSuffix(essence, "*"))
			continue
		}
		p.contentTypes[essence] = true
	}
	return p
}

// allowsCoding reports whether the route may produce coding (a canonical
// bucket token: "br", "zstd", "gzip").
func (p *compressionPolicy) allowsCoding(coding string) bool {
	if len(p.encodings) == 0 {
		return coding == "br" || coding == "zstd" || coding == "gzip"
	}
	return p.encodings[coding]
}

// allowsContentType reports whether an origin Content-Type essence is
// compressible per this route's allow-list. The value may carry
// parameters ("text/html; charset=utf-8"); the essence is cut at ';'.
func (p *compressionPolicy) allowsContentType(contentType string) bool {
	essence, _, _ := strings.Cut(contentType, ";")
	essence = strings.ToLower(strings.TrimSpace(essence))
	if p.contentTypes[essence] {
		return true
	}
	for _, pre := range p.ctWildcardPre {
		if strings.HasPrefix(essence, pre) {
			return true
		}
	}
	return false
}

// fillCompressor is the streaming tee used on the fill path: each origin
// chunk is fed through the encoder and the compressed bytes are written
// to the client writer w and appended to buf (store). It sync-flushes
// every compressFlushEvery input bytes. skipStore is set at finish when
// the fill must not be stored compressed (expansion / under minSize /
// body incomplete); the client has already received valid compressed
// bytes, only the cache write is skipped.
type fillCompressor struct {
	sc *streamCompressor
	// buf is the store buffer receiving the drained compressed bytes.
	buf *bytes.Buffer
	// out is the encoder's scratch buffer (sc.out == out, kept here so
	// the tee can return it to the pool without reaching through sc
	// after releaseCompressor nils sc.out).
	out        *bytes.Buffer
	sinceFlush int
	rawSeen    int64
	skipStore  bool
}

// writeChunk feeds one origin chunk to the encoder and drains the
// emitted compressed bytes to w (client) and buf (store).
func (fc *fillCompressor) writeChunk(w io.Writer, p []byte) error {
	fc.rawSeen += int64(len(p))
	if err := fc.sc.write(p); err != nil {
		return err
	}
	fc.sinceFlush += len(p)
	if fc.sinceFlush >= compressFlushEvery {
		if err := fc.drain(w); err != nil {
			return err
		}
		if err := fc.sc.flush(); err != nil {
			return err
		}
		fc.sinceFlush = 0
		return fc.drain(w)
	}
	return fc.drain(w)
}

// drain copies whatever the encoder has emitted into sc.out to w and
// buf, then resets sc.out. w may be a *bufio.Writer whose own buffer has
// not been flushed yet; the bytes still reach the client in order.
func (fc *fillCompressor) drain(w io.Writer) error {
	out := fc.sc.out
	if out.Len() == 0 {
		return nil
	}
	chunk := out.Bytes()
	if _, err := w.Write(chunk); err != nil {
		return err
	}
	if _, err := fc.buf.Write(chunk); err != nil {
		return err
	}
	out.Reset()
	return nil
}

// finish closes the encoder, drains the final bytes to w and buf, and
// returns the compressed length.
func (fc *fillCompressor) finish(w io.Writer) (int, error) {
	if err := fc.sc.close(); err != nil {
		return 0, err
	}
	if err := fc.drain(w); err != nil {
		return 0, err
	}
	return fc.buf.Len(), nil
}

// finalizeTeeFill closes the streaming fill's compressor and decides
// whether the compressed variant is stored: it stamps Content-Encoding
// on the stored header map (and drops the stale Content-Length) when the
// body completed cleanly, met minSize, actually shrank, and stayed under
// every cap; otherwise skipStore is set and the caller treats the fill as
// unstorable (the client already received valid compressed bytes). The
// encoder is released back to its pool either way.
func (fc *fillCompressor) finalizeTeeFill(
	w io.Writer,
	resMap *header.Map,
	bodyComplete, cacheExceeded bool,
	totalBytes int64,
	minSize, maxSize int64,
) {
	compressed, cerr := fc.finish(w)
	switch {
	case cerr != nil:
		fc.skipStore = true
	case !bodyComplete:
		fc.skipStore = true
	case totalBytes < minSize:
		fc.skipStore = true
	case maxSize > 0 && totalBytes > maxSize:
		fc.skipStore = true
	case compressed >= int(totalBytes):
		fc.skipStore = true
	case cacheExceeded:
		fc.skipStore = true
	default:
		resMap.Set(header.ContentEncoding, fc.sc.coding)
		resMap.Del(header.ContentLength)
	}
	releaseCompressor(fc.sc)
	fc.sc = nil
}

// stampAEDependentVary adds "Accept-Encoding" to the response's Vary
// list on both the stored header map (resMap — so effectiveVary computes
// the bucketed variant key) and the client-facing fasthttp header (dst —
// so downstream caches and browsers vary too). Idempotent: a field list
// already containing accept-encoding (case-insensitive) is left alone.
// Called on cacheable fills of compression-enabled routes, before the
// variant key is computed (ADR-0075; RFC 9111 §4.1, RFC 9110 §12.5.5).
func stampAEDependentVary(resMap *header.Map, dst *fasthttp.ResponseHeader) {
	const field = "accept-encoding"
	joined := resMap.GetAll(header.Vary)
	if joined != "" {
		for f := range strings.SplitSeq(joined, ",") {
			if strings.EqualFold(strings.TrimSpace(f), field) {
				// Already varies; make sure the client sees it too
				// (origin may have sent it, which CopyToFastHTTP
				// already copied to dst).
				return
			}
		}
		resMap.Set(header.Vary, joined+", Accept-Encoding")
	} else {
		resMap.Set(header.Vary, "Accept-Encoding")
	}
	if dst != nil {
		dst.SetCanonical(header.S2b(header.Vary), header.S2b("Accept-Encoding"))
	}
}
