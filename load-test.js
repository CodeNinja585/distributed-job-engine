import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    flood: {
      executor: 'ramping-arrival-rate',   // ramps SUBMISSION rate, independent of response time
      startRate: 1, timeUnit: '1s',
      preAllocatedVUs: 50, maxVUs: 200,
      stages: [
        { target: 5,  duration: '20s' },  // warm up
        { target: 15, duration: '40s' },  // well past capacity
        { target: 0,  duration: '5s' },   // stop submitting
      ],
    },
  },
};

export default function () {
  const res = http.post('http://127.0.0.1:3000/jobs',
    JSON.stringify({ ms: 500 }),          // uniform jobs — the point is comparability
    { headers: { 'Content-Type': 'application/json' } });
  check(res, { 'accepted 202': (r) => r.status === 202 });
}