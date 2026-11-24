// node --test apps/client-ts/client.test.ts
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createClient, QueueError } from './client.ts';

test('sends JSON and surfaces API errors', async () => {
  const seen: { url: string; init: RequestInit }[] = [];
  const fake = (async (url: string, init: RequestInit) => {
    seen.push({ url, init });
    if (url.endsWith('/api/jobs')) return new Response(JSON.stringify({ id: 7 }), { status: 201 });
    return new Response(JSON.stringify({ error: 'no such workflow' }), { status: 404 });
  }) as typeof fetch;
  const qq = createClient('http://q/', fake);

  assert.deepEqual(await qq.enqueue({ kind: 'noop', idempotency_key: 'k' }), { id: 7 });
  assert.equal(seen[0].url, 'http://q/api/jobs');
  assert.equal(seen[0].init.body, '{"kind":"noop","idempotency_key":"k"}');

  await assert.rejects(qq.startRun('a/b'), (e: unknown) => e instanceof QueueError && e.status === 404 && e.message === 'no such workflow');
  assert.equal(seen[1].url, 'http://q/api/workflows/a%2Fb/runs');
});
