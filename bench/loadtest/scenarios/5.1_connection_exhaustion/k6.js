import http from 'k6/http';
import { check } from 'k6';
const target = __ENV.TARGET;
const vus = parseInt(__ENV.VUS || '1000');
export const options = {
  vus: vus, duration: __ENV.DURATION || '60s',
  thresholds: { http_req_failed: ['rate<0.05'] },
};
// No keep-alive: each request opens+closes a connection
export default function() {
  const res = http.get(target, { timeout: '10s',
    headers: { Connection: 'close' } });
  check(res, { '2xx': r => r.status < 300 });
}
