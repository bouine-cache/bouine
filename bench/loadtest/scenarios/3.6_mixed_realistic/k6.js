// 60% hit / 15% miss / 10% stale / 5% reval / 5% bypass / 3% vary / 2% error
import http from 'k6/http';
import { check } from 'k6';
import { Rate } from 'k6/metrics';
const hitRate = new Rate('hit_rate');
const base = __ENV.BASE_URL || 'http://bouine:8080';
const PATHS = [
  ...Array(60).fill('/hit'),
  ...Array(15).fill('/miss'),
  ...Array(10).fill('/stale'),
  ...Array(5).fill('/revalidate'),
  ...Array(5).fill('/bypass'),
  ...Array(3).fill('/vary'),
  ...Array(2).fill('/error'),
];
export const options = {
  scenarios: { mixed: { executor:'constant-arrival-rate', rate:10000, timeUnit:'1s',
    duration:'300s', preAllocatedVUs:3000, maxVUs:15000 } },
  thresholds: { http_req_failed: ['rate<0.03'] },
};
export default function() {
  const path = PATHS[Math.floor(Math.random() * PATHS.length)];
  const res = http.get(`${base}${path}`, { timeout: '5s' });
  check(res, { '2xx': r => r.status < 300 || r.status === 503 });
  hitRate.add((res.headers['X-Cache']||'') === 'HIT');
}
