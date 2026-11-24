// A small TypeScript client for the job queue's HTTP API. No dependencies: uses global fetch.
// Plain, erasable TypeScript, so Node 24 runs it directly and any bundler can take it.
//
//   import { createClient } from './client.ts';
//   const qq = createClient('http://localhost:8080');
//   await qq.enqueue({ kind: 'welcome_email', payload: { user: 42 }, idempotency_key: 'welcome:42' });

export type Job = {
  kind: string;
  payload?: unknown;
  queue?: string;
  priority?: number;
  run_at?: string; // RFC 3339
  max_attempts?: number;
  idempotency_key?: string; // 1 to 255 bytes; enqueueing the same key twice is a no-op
};

export type JobStatus = {
  id: number;
  queue: string;
  kind: string;
  status: 'queued' | 'running' | 'succeeded' | 'failed' | 'dead' | 'cancelled';
  attempts: number;
  worker_id: string | null;
  last_error: string | null;
  run_at: string;
  created_at: string;
  finished_at: string | null;
};

export type Run = {
  id: number;
  workflow: string;
  status: 'running' | 'succeeded' | 'failed';
  input: unknown;
  created_at: string;
  finished_at: string | null;
  steps: { step: string; depends_on: string[]; on_failure: boolean; status: string; output: unknown; job_id: number | null }[];
};

export class QueueError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export function createClient(baseUrl: string, fetchImpl: typeof fetch = fetch) {
  async function call<T>(method: string, path: string, body?: unknown): Promise<T> {
    const res = await fetchImpl(baseUrl.replace(/\/$/, '') + path, {
      method,
      headers: body === undefined ? undefined : { 'Content-Type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new QueueError(res.status, data.error ?? res.statusText);
    return data as T;
  }
  return {
    /** Enqueue a job. `duplicate` is true when the queue already had this idempotency_key. */
    enqueue: (job: Job) => call<{ id: number; duplicate?: boolean }>('POST', '/api/jobs', job),
    job: (id: number) => call<JobStatus>('GET', `/api/jobs/${id}`),
    /** Start a run of a workflow defined in the workers' Go code. */
    startRun: (workflow: string, input?: unknown) =>
      call<{ id: number }>('POST', `/api/workflows/${encodeURIComponent(workflow)}/runs`, { input }),
    run: (id: number) => call<Run>('GET', `/api/workflows/runs/${id}`),
  };
}
