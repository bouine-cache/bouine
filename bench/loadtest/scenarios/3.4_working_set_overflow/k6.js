// 50k unique 64 KiB cacheable URLs — working set >> 64 MiB cache
import http from 'k6/http';
import { check } from 'k6';
import { Rate } from 'k6/metrics';
const hitRate = new Rate('hit_rate');
const base = __ENV.BASE_URL || 'http://bouine:8080';
export const options = {
  scenarios: { ws: { executor:'constant-arrival-rate', rate:5000, timeUnit:'1s',
    duration:'300s', preAllocatedVUs:2000, maxVUs:10000 } },
};
export default function() {
  const id = Math.floor(Math.random() * 50000);
  const res = http.get(`${base}/unique/${id}?kb=64`);
  check(res, { '200': r => r.status === 200 });
  hitRate.add((res.headers['X-Cache']||'') === 'HIT');
}
