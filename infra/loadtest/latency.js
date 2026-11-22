// Enqueue → handled on an idle queue: one noop at a time, with a gap so every worker is
// asleep in LISTEN when the next job arrives.
//   docker compose --profile loadtest run --rm k6 run /scripts/latency.js
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://caddy:8080';
const handled = new Trend('enqueue_to_handled', true); // Postgres clock: created_at → finished_at
const observed = new Trend('client_round_trip', true); // POST sent → GET says succeeded

export const options = {
  scenarios: {
    idle: { executor: 'shared-iterations', vus: 1, iterations: Number(__ENV.JOBS || 200), maxDuration: '5m' },
  },
  summaryTrendStats: ['min', 'med', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    checks: ['rate==1'],
    enqueue_to_handled: [`p(95)<${__ENV.MAX_MS || 50}`],
  },
};

export default function () {
  sleep(0.15 + Math.random() * 0.2);
  const started = Date.now();
  const res = http.post(`${BASE}/api/jobs`, '{"kind":"noop"}');
  if (!check(res, { enqueued: (r) => r.status === 201 })) return;
  const url = `${BASE}/api/jobs/${res.json('id')}`;

  let job = {};
  while (Date.now() - started < 3000) {
    const got = http.get(url, { tags: { name: 'get job' } });
    if (got.status === 200) job = got.json();
    if (job.finished_at) break;
    sleep(0.002);
  }
  if (!check(job, { succeeded: (j) => j.status === 'succeeded' })) return;
  observed.add(Date.now() - started);
  handled.add(Date.parse(job.finished_at) - Date.parse(job.created_at));
}
