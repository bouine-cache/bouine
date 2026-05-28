import http from 'k6/http';
export const options = { vus: 50, iterations: 10000 };
const target = __ENV.TARGET || 'http://bouine:8080';
export default function() { http.get(`${target}/hit`); }
