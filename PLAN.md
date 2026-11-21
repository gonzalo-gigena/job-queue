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
