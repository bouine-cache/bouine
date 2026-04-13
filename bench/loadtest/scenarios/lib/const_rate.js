// k6 constant-rate executor — driven by TARGET, RATE, DURATION env vars
import http from 'k6/http';
import { check } from 'k6';
import { Rate } from 'k6/metrics';

const cacheHitRate = new Rate('cache_hit_rate');
const target  = __ENV.TARGET || 'http://bouine:8080/hit';
const rate    = parseInt(__ENV.RATE    || '1000');
const duration= __ENV.DURATION || '60s';

export const options = {
  scenarios: {
    const_rate: {
      executor: 'constant-arrival-rate',
      rate: rate,
      timeUnit: '1s',
      duration: duration,
      preAllocatedVUs: Math.min(rate * 2, 5000),
      maxVUs: Math.min(rate * 10, 20000),
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
  },
};

export default function () {
  const res = http.get(target, { timeout: '5s' });
  check(res, { 'status 200': (r) => r.status === 200 });
  const xCache = res.headers['X-Cache'] || '';
  cacheHitRate.add(xCache === 'HIT');
}
