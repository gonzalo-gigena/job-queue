# 02 — Job queue and workflow engine

A small, Postgres-backed version of Sidekiq or Temporal: retries, cron schedules,
multi-step workflows, a dead-letter queue, and a live dashboard. Follows the shared baseline
(`common/docs/baseline.md`).

**What it proves:** concurrency, failure handling, honest delivery
guarantees, Go, and Postgres internals.

## Stack

| Service | Tech | Scales? |
|---|---|---|
| `api` | Go 1.23, `net/http` + `pgx`: enqueue, query, admin actions | yes |
| `worker` | Go, the same binary run with `worker` subcommand | yes, the point of the project |
| `scheduler` | same binary, `scheduler` subcommand: cron + lease reaper | 2+ replicas, one leader |
| `dashboard` | React + Vite (or server-rendered HTML + htmx), live via SSE | static |
| `postgres` | Postgres 17 is the only stateful service (no Redis) | single primary |
| `example-app` | a demo producer that enqueues realistic jobs | — |

One Go binary with subcommands means one Dockerfile and one image.

## Steps

### Phase 0 — Scaffold
1. Baseline + `cmd/qq/main.go` with `api | worker | scheduler | migrate` subcommands.
- **Done when:** `docker compose up` starts all roles and `/readyz` is green.

### Phase 1 — Core queue
1. Table `jobs`: `id, queue, kind, payload jsonb, status (queued|running|succeeded|failed|dead|cancelled),
   priority, run_at, attempts, max_attempts, lease_until, worker_id,
   idempotency_key, last_error, created_at, finished_at`.
2. `UNIQUE (queue, idempotency_key)` so enqueueing the same job twice is a no-op (`ON CONFLICT DO NOTHING RETURNING`).
3. Dequeue in one statement:
   ```sql
   UPDATE jobs SET status='running', attempts=attempts+1,
          lease_until=now()+interval '30 s', worker_id=$1
   WHERE id IN (
     SELECT id FROM jobs
     WHERE queue = ANY($2) AND status='queued' AND run_at <= now()
     ORDER BY priority DESC, run_at
     FOR UPDATE SKIP LOCKED LIMIT $3)
   RETURNING *;
   ```
4. Partial index `ON jobs (queue, priority DESC, run_at) WHERE status='queued'`.
5. Workers poll, and also `LISTEN jobs_<queue>` so they wake as soon as a job
   arrives (`NOTIFY` in the enqueue transaction).
- **Done when:** enqueue → handled in under 50 ms on an idle system.

### Phase 2 — Failure handling
1. **Leases + heartbeat:** a running worker extends `lease_until` every 10 s
   with `WHERE id=$1 AND worker_id=$2`. If 0 rows update, the job was taken away,
   so the worker abandons it. This fences out "zombie" workers. Completing a job
   uses the same check. The reaper (in `scheduler`) puts jobs whose lease expired
   back to `queued`, or to `dead` once `attempts >= max_attempts`, so a job that
   keeps crashing its worker can't loop forever.
2. **Retries:** exponential backoff with full jitter, `run_at = now() + min(cap, base·2^attempt)·rand()`.
3. **Dead-letter queue:** after `max_attempts` → `dead`. The dashboard can retry or discard them.
4. **Graceful shutdown:** on SIGTERM stop dequeuing, finish in-flight jobs up to
   a deadline, then release their leases.
5. Be explicit in the README: delivery is **at-least-once**. Handlers must be
   idempotent. Show a pattern for it: an `processed(job_id)` table written in the same
   transaction as the job's side effect.
- **Done when:** the headline chaos test below passes.

### Phase 3 — Cron schedules
1. Table `schedules(id, cron_expr, timezone, queue, kind, payload, next_run_at, enabled)`.
2. Leader election: `pg_try_advisory_lock(<constant>)` held on a dedicated,
   direct connection (never through PgBouncer, see Phase 7). If the leader dies, its connection closes, the lock frees, and another
   replica takes over.
3. Each tick inserts due jobs with `idempotency_key = schedule_id || ':' || scheduled_time`,
   so even a brief two-leader overlap cannot create duplicate runs.
4. Handle DST correctly (use a cron library with timezone support, e.g. `robfig/cron/v3`).
- **Done when:** kill the leader scheduler; the other takes over within 5 s; no
  scheduled run is skipped or duplicated.

### Phase 4 — Workflows (DAGs)
1. Tables `workflow_runs`, `workflow_steps(run_id, step, depends_on text[], status, output jsonb)`.
2. When a step succeeds, mark it and enqueue steps whose dependencies are all done,
   **in the same transaction**. This keeps state consistent if a crash happens between the two.
   Lock the `workflow_runs` row first (`SELECT … FOR UPDATE`). Without that lock, two
   parent steps that finish at the same time each see the other as still running,
   and the child step is never enqueued.
3. A step can read its parents' outputs. If one step fails permanently, the
   workflow fails and `on_failure` steps run (compensation, the saga pattern).
4. Define workflows in Go code, e.g. `Workflow("signup").Step("create").Then("email", "crm")`.
- **Done when:** a 5-step DAG with fan-out/fan-in completes correctly while workers are being killed at random.

### Phase 5 — Concurrency and rate limits
1. Per-queue concurrency limit (e.g. at most 5 `emails` jobs running at once).
   Counting running jobs inside the dequeue query is not enough, because concurrent
   workers all see the same count. Lock a `queue_limits` row (`FOR UPDATE`) or take
   `pg_advisory_xact_lock(hashtext(queue))` in the dequeue transaction, then count.
2. Per-queue rate limit (N/sec) with a token bucket stored in Postgres.
- **Done when:** a load test shows the limit is never exceeded across 5 worker replicas.

### Phase 6 — Dashboard
1. Queues overview: depth, throughput/min, failure rate, oldest queued age.
2. Job search by status/queue/kind, job detail with payload, attempts, errors.
3. Live updates via Server-Sent Events from `api` (polling Postgres every 1 s is fine; say so in the README).
4. Actions: retry, cancel, pause queue. Protect them with basic auth or a token: an open admin panel is a security hole.
- **Done when:** a GIF of jobs flowing, failing and retrying live.

### Phase 7 — Scale and retention
1. Run `common/scripts/scale.sh` at 1, 2, 4 and 8 workers with no-op jobs
   and 50 ms jobs, and put a jobs/sec vs workers chart in the README.
2. Move finished jobs to a `jobs_archive` table partitioned by day, and drop old
   partitions. This keeps the hot table small and vacuum healthy. Keep `jobs` itself
   unpartitioned: a unique index on a partitioned table must include the partition
   key, which would break `UNIQUE (queue, idempotency_key)`. If keys must outlive the
   archive, keep them in a small `idempotency_keys` table with its own expiry.
   Document autovacuum settings.
3. PgBouncer in transaction mode for many workers. `LISTEN` and the scheduler's
   advisory lock are session-scoped, so they use direct connections: one per worker
   for LISTEN, one for the leader. Add a small random delay (0–50 ms) after a NOTIFY
   wake-up so idle workers don't all hit the dequeue query at the same instant.
- **Done when:** throughput scales close to linearly up to the point where Postgres
  saturates, and the README says where that point is.

### Phase 8 — Client SDKs
1. Go package `qq/client` (enqueue, enqueue in an existing transaction, define handlers).
2. A small TypeScript client for enqueueing over HTTP.
- **Done when:** `example-app` uses only the public SDK.

## Headline test
`chaos_test`: enqueue 10,000 jobs whose handler writes a row to `effects`
idempotently. While running, `docker compose kill -s SIGKILL worker` at
random intervals and restart replicas. Assert: every job ends in `succeeded`,
`effects` has exactly 10,000 rows, nothing is left `running`.

## Scaling notes
- Postgres is the ceiling (roughly thousands of jobs/sec on one node, depending on job size).
  The README should say that past this point you would shard queues across databases or
  switch to Kafka/NATS, and why you didn't need to.

## Beyond the plan

Build these after the phases above. Each starts from a problem real systems hit, and
each should add a row to the README's Decisions & tradeoffs table. If one wouldn't,
skip it. B1 is the one to build first.

### B1 — Durable waits: sleep, wait for an event, time out
**Why:** "Remind the user if they haven't verified their email in 3 days, and cancel
the reminder if they do." Without this, that's a cron job scanning a table.
1. Steps can call `Sleep(d)` and `WaitForEvent(key, timeout)`. A waiting step holds no
   worker: its job ends and it writes `step_waits(run_id, step, event_key, expires_at)`.
2. `POST /events {key, payload}` wakes every wait on that key. The scheduler's reaper
   resumes waits whose `expires_at` has passed, with a timeout result.
3. The event and the timeout can land at the same moment. Both paths resume with
   `UPDATE workflow_steps SET status='running' … WHERE status='waiting'` and act only
   if one row changed, so exactly one of them wins.
4. Dashboard: waiting steps with their key and deadline.
- **Done when:** 10,000 parked workflows use zero worker slots, and a race test that
  fires the event right at the deadline never resumes a step twice.

### B2 — Workflow versioning
**Why:** you deploy a change to `signup` while 2,000 runs are in progress. A run that
started on the old step graph can't safely continue on the new one.
1. Workflows register with a version, e.g. `Workflow("signup", 2)`. `workflow_runs.version`
   records the version a run started on, and the engine always advances a run with
   that version's graph.
2. Keep old versions registered until their last run finishes. The dashboard shows
   runs per version, so you know when one is safe to delete.
3. During a rolling deploy, a worker that gets a step for a version it doesn't have
   puts the job back with a short delay instead of failing it.
- **Done when:** deploying v2 mid-run lets every v1 run finish on v1 while new runs start on v2.

### B3 — Fairness between tenants in one queue
**Why:** one customer bulk-imports 1M jobs into `emails`, and every other customer's
password-reset email waits an hour behind them. Phase 5's per-queue limits don't fix
this: it's head-of-line blocking inside one queue.
1. Jobs get an optional `fairness_key` (usually the tenant).
2. Dequeue round-robins across keys that have queued work instead of taking the oldest
   jobs overall, without scanning the big tenant's rows. For example: a small
   `queue_keys(queue, fairness_key, last_served_at)` table, pick the least recently
   served keys, then claim from each through the partial index (add `fairness_key` to it).
3. Optional weights per key (a paid tier gets 3× the share).
- **Done when:** with one key holding 1M jobs and 50 keys holding 100 each, the small
  keys' p95 wait stays within 2× of a run without the flood. Chart both in the README.

### B4 — Debounce and batching
**Why:** 40 comment events in one minute should send one digest email, not 40.
1. `debounce_key` + window: enqueueing with a key that already has a `queued` job pushes
   that job's `run_at` forward instead of adding a new one. Only while it's still
   `queued`: once a worker has claimed it, the next event starts a new job.
2. `batch_key` + max size + max wait: events collect into one job whose handler gets all
   the payloads.
3. Both are upserts on a partial unique index `WHERE status='queued'`.
- **Done when:** 1,000 events with one debounce key over 10 s produce exactly one job,
  and an event that arrives while that job is running produces exactly one more.

### B5 — Trace context through the queue
**Why:** "this request was slow" often means "the job it enqueued was slow", and
without propagation the trace stops at the enqueue.
1. The SDK stores the W3C `traceparent` in job metadata at enqueue. The worker starts a
   span linked to it, one per attempt, so retries show up in the same trace.
2. Export with OpenTelemetry, off by default and turned on by `OTEL_EXPORTER_OTLP_ENDPOINT`.
   The observability project can receive it.
- **Done when:** one trace shows the HTTP request, the enqueue and every attempt of the job.
