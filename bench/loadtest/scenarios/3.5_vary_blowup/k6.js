import http from 'k6/http';
const base = __ENV.BASE_URL || 'http://bouine:8080';
export const options = {
  scenarios: { s: { executor:'constant-arrival-rate', rate:1000, timeUnit:'1s',
    duration:'60s', preAllocatedVUs:500, maxVUs:2000 } },
};
export default function() {
  const variant = Math.floor(Math.random() * 1000);
  http.get(`${base}/vary`, { headers: {'X-Test': String(variant)} });
}
