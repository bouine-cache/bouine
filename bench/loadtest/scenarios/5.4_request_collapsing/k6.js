// All VUs hit the same uncached URL — collapse should reduce origin calls to 1
import http from 'k6/http';
import { check } from 'k6';
const target = __ENV.TARGET;
export const options = { vus: 10000, iterations: 10000 };
export default function() {
  const r = http.get(target, { timeout: '15s' });
  check(r, { '200': x => x.status === 200 });
}
