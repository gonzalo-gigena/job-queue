// A demo producer: enqueues realistic-looking traffic through the public TypeScript client only.
// docker compose --profile demo up -d example-app, then watch the dashboard.
import { createClient, QueueError } from '../client-ts/client.ts';

const qq = createClient(process.env.BASE_URL ?? 'http://caddy:8080');
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const pick = <T>(xs: T[]) => xs[Math.floor(Math.random() * xs.length)];

let tick = 0;
for (;;) {
  tick++;
  try {
    // A signup every few seconds: a welcome email (deduplicated per user) and the demo workflow.
    const user = Math.floor(Math.random() * 1000);
    const email = await qq.enqueue({ kind: 'sleep', payload: { ms: Math.round(200 + Math.random() * 800), user }, idempotency_key: `welcome:${user}` });
    // Some runs fail at a step, to show compensation.
    const run = await qq.startRun('demo', { n: user, ms: 100, fail: Math.random() < 0.1 ? pick(['b', 'c', 'e']) : undefined });
    // Background work, and now and then a job that keeps failing until it's dead.
    for (let i = 0; i < 1 + Math.floor(Math.random() * 4); i++) await qq.enqueue({ kind: 'noop', priority: pick([0, 0, 0, 10]) });
    if (tick % 7 === 0) await qq.enqueue({ kind: 'effect', payload: { fail: true }, max_attempts: 3 });
    if (tick % 10 === 0) console.log(JSON.stringify({ tick, email: email.id, duplicate: !!email.duplicate, run: run.id }));
  } catch (e) {
    console.error(e instanceof QueueError ? `api ${e.status}: ${e.message}` : e);
  }
  await sleep(1000 + Math.random() * 2000);
}
