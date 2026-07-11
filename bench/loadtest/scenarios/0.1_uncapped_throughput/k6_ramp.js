// Uncapped throughput ramp: increases RPS until errors or latency degrades.
// No rate cap — k6 ramps from 1k to 200k RPS over 5 minutes.
// The server that sustains the highest RPS at p95 < 5ms wins.
import http from 'k6/http';
import { check } from 'k6';
import { Rate, Trend } from 'k6/metrics';

const cacheHitRate = new Rate('cache_hit_rate');
const waitingTime  = new Trend('http_req_waiting');

const target = __ENV.TARGET || 'http://bouine:8080/hit';

export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-arrival-rate',
      startRate: 1000,
      timeUnit: '1s',
      preAllocatedVUs: 5000,
      maxVUs: 50000,
      stages: [
        { duration: '30s', target: 10000 },
        { duration: '30s', target: 25000 },
        { duration: '30s', target: 50000 },
        { duration: '30s', target: 100000 },
        { duration: '30s', target: 200000 },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.05'],
  },
};

export default function () {
  const res = http.get(target, { timeout: '10s' });
  check(res, { 'status 200': (r) => r.status === 200 });
  const xCache = res.headers['X-Cache'] || '';
  cacheHitRate.add(xCache === 'HIT');
}
