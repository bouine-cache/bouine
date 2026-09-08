// §3.7 — Payload integrity under eviction churn.
//
// The data-integrity net the preprod corruption incident slipped past:
// every other scenario asserts status codes and latency, never payload
// bytes. 50k unique 64 KiB deterministic payloads (3 GiB working set,
// far above the 64 MiB hot budget) keep SIEVE eviction and origin
// refetches churning while every response is validated against the
// deterministic expected body (length + boundary blocks + a rotating
// probe block per request; every 100th request gets a full byte
// compare). A cache that serves an origin-client buffer reused by the
// next fetch, cross-object bytes, or a truncated body fails here —
// regardless of which serve path delivered it.
//
// Runs against every TUT: competitors (nginx, varnish, envoy) act as
// the control group — an integrity failure isolated to bouine is a
// bouine regression, a failure across all TUTs points at the origin.
import http from 'k6/http';
import { check } from 'k6';
import { Rate } from 'k6/metrics';
import { verifyPayload, expectedBody } from '../lib/payload.js';

const integrityOk = new Rate('payload_integrity_ok');
const base = __ENV.BASE_URL || 'http://bouine:8080';
const keys = parseInt(__ENV.KEYS || '50000');
const rate = parseInt(__ENV.RATE || '2000');
const duration = __ENV.DURATION || '60s';

export const options = {
  scenarios: {
    integrity: {
      executor: 'constant-arrival-rate',
      rate: rate,
      timeUnit: '1s',
      duration: duration,
      preAllocatedVUs: Math.min(rate, 1000),
      maxVUs: Math.min(rate * 4, 5000),
    },
  },
  thresholds: {
    // Zero tolerance: origin bodies are deterministic, so any single
    // corrupt byte is a real defect, not noise.
    payload_integrity_ok: ['rate==1'],
    http_req_failed: ['rate<0.01'],
  },
};

let rotation = 0;

export default function () {
  const id = Math.floor(Math.random() * keys);
  const k = `integrity-${id}`;
  const res = http.get(`${base}/payload?k=${k}&kb=64`, { timeout: '10s' });
  const ok = check(res, { '200': (r) => r.status === 200 });
  let why = ok ? '' : `status=${res.status}`;
  if (ok) {
    rotation++;
    why = verifyPayload(res.body, k, 64, rotation);
    if (why === '' && rotation % 100 === 0) {
      // Sampled full byte-compare: catches corruption in interior
      // blocks the per-request probes rotate through only slowly.
      if (res.body !== expectedBody(k, 64)) {
        why = `full-compare-mismatch k=${k}`;
      }
    }
  }
  integrityOk.add(why === '', why);
}
