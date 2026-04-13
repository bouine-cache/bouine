// k6 ramp scenario — driven by STAGES_JSON env var (JSON array of {duration,target})
import http from 'k6/http';
import { check } from 'k6';

const target = __ENV.TARGET || 'http://bouine:8080/hit';
const stages = JSON.parse(__ENV.STAGES_JSON || '[{"duration":"30s","target":1000}]');

export const options = {
  stages: stages,
  thresholds: { http_req_failed: ['rate<0.05'] },
};

export default function () {
  const res = http.get(target, { timeout: '10s' });
  check(res, { 'status 2xx': (r) => r.status >= 200 && r.status < 300 });
}
