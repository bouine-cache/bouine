// payload.js — deterministic payload generation for data-integrity
// scenarios, mirroring the origin's /payload route (test/integration/
// origin/main.go and test/integration/driver/origin.go):
//
//   body = kb KiB of 8-byte blocks, block i = LE32(fnv32a(k)^i) || LE32(i)
//
// k6 receives the body as a latin1 string, so blocks are built as
// 8-char strings from String.fromCharCode on the little-endian bytes.

// fnv32a computes the standard 32-bit FNV-1a hash of s (>>> 0).
export function fnv32a(s) {
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619) >>> 0;
  }
  return h >>> 0;
}

// block returns the 8-char string for block index i under a key seed.
export function block(seed, i) {
  const a = (seed ^ i) >>> 0;
  const b = i >>> 0;
  return String.fromCharCode(
    a & 0xff, (a >>> 8) & 0xff, (a >>> 16) & 0xff, (a >>> 24) & 0xff,
    b & 0xff, (b >>> 8) & 0xff, (b >>> 16) & 0xff, (b >>> 24) & 0xff
  );
}

// expectedBody builds the full expected payload string for (k, kb).
// Only use for sampled full comparisons: constructing a 64 KiB string
// per request would dominate VU CPU at scenario rates.
export function expectedBody(k, kb) {
  const seed = fnv32a(k);
  const blocks = (kb * 1024) / 8;
  const parts = new Array(blocks);
  for (let i = 0; i < blocks; i++) {
    parts[i] = block(seed, i);
  }
  return parts.join('');
}

// verifyPayload performs the per-request integrity check: exact length,
// first and last 8-byte blocks, plus the block at a rotating offset so
// every block position is sampled across requests. Full string compares
// happen on top of this in the scenario (sampled).
export function verifyPayload(body, k, kb, rotation) {
  if (typeof body !== 'string' || body.length !== kb * 1024) {
    return `len=${body ? body.length : 'null'} want=${kb * 1024}`;
  }
  const seed = fnv32a(k);
  const last = (kb * 1024) / 8 - 1;
  if (body.slice(0, 8) !== block(seed, 0)) {
    return `first-block-mismatch k=${k}`;
  }
  if (body.slice(-8) !== block(seed, last)) {
    return `last-block-mismatch k=${k}`;
  }
  const probe = rotation % (last + 1);
  const off = probe * 8;
  if (body.slice(off, off + 8) !== block(seed, probe)) {
    return `block-${probe}-mismatch k=${k}`;
  }
  return '';
}
