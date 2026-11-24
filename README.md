# Job queue and workflow engine

A Postgres-backed job queue with retries, cron schedules, multi-step workflows and a live dashboard.

The step-by-step build plan is in [PLAN.md](PLAN.md).

> **Status:** phase 4 of 8, workflows. Jobs run with leases, retries with backoff, a dead-letter state and
> graceful shutdown; cron schedules run on a leader-elected scheduler; multi-step workflows run as DAGs with
> compensation. Rate limits and the dashboard aren't built yet.
> The API, including the dead-letter actions, has no authentication yet: don't expose it publicly.

## Run it

```bash
git clone --recurse-submodules https://github.com/gonzalo-gigena/job-queue.git
cd job-queue
cp .env.example .env
docker compose up --watch
```

Open http://localhost:8080. Needs only Docker with Compose v2. Code changes reload
automatically (Compose Watch). On Windows, clone inside the WSL filesystem, not `/mnt/c`.

Cloned without `--recurse-submodules`? Run `git submodule update --init`.

## Use it

```bash
curl -X POST localhost:8080/api/jobs -d '{"kind":"sleep","payload":{"ms":200},"idempotency_key":"demo-1"}'
curl localhost:8080/api/jobs/1
```

`POST /api/jobs` returns `201 {"id":…}`, or `200 {"id":…,"duplicate":true}` when the queue already has that
`idempotency_key`.

| Field | Default | Notes |
|---|---|---|
| `kind` | required | Built in: `noop`, `sleep` with `{"ms":…}`, and `effect` (the idempotent pattern below; `{"fail":true}` always fails). An unknown kind fails the job. |
| `payload` | `{}` | Any JSON, up to 1 MB |
| `queue` | `default` | `[a-z0-9_]`, up to 50 characters. Workers serve the queues in `WORKER_QUEUES`. |
| `priority` | `0` | Higher runs first |
| `run_at` | now | RFC 3339 time. A future job starts within about 1 s of it (the poll interval). |
| `max_attempts` | `5` | Runs before the job is `dead`, counting crashes |
| `idempotency_key` | none | 1 to 255 bytes, unique per queue |

A worker runs one job at a time, for up to 5 minutes. Add workers with `--scale worker=N`.

### Cron schedules

```bash
curl -X PUT localhost:8080/api/schedules/nightly-report \
  -d '{"cron":"0 3 * * *","timezone":"Europe/Madrid","kind":"sleep","payload":{"ms":100}}'
curl localhost:8080/api/schedules
curl -X DELETE localhost:8080/api/schedules/nightly-report
```

`cron` is the standard 5 fields, an optional leading seconds field (`*/10 * * * * *`), or `@daily`-style
descriptors. Times are wall-clock in `timezone` (default `UTC`), so `0 9 * * *` stays at 09:00 local across
DST changes. `queue`, `kind` and `payload` are as for a job; `"enabled": false` pauses a schedule.

Two `scheduler` replicas run; the one holding `pg_try_advisory_lock` on a direct connection is the leader and
checks schedules every second. If it dies, Postgres closes its session, the lock frees, and the standby takes
over within about a second. Each run is enqueued with the idempotency key `schedule_id:time`, and runs missed
while no leader was up are enqueued on takeover (up to 100 per schedule per tick), so a failover neither skips
nor duplicates a run.

### Workflows

A workflow is a DAG of steps defined in Go (`apps/api/workflow.go`). Each step is an ordinary job of kind
`wf:<workflow>.<step>`, so it gets leases, retries and fencing, and reads its parents' outputs:

```go
newWorkflow("signup").
	step("create", createUser).
	step("email", sendWelcome, "create"). // runs once "create" succeeded
	step("crm", addToCRM, "create").
	step("done", markDone, "email", "crm"). // fan-in
	onFailure("cleanup", deleteUser)       // compensation: only if some step fails for good
```

```bash
curl -X POST localhost:8080/api/workflows/demo/runs -d '{"input":{"n":1}}'   # → {"id":…}
curl localhost:8080/api/workflows/runs/1   # run status, and each step's status, output and job
```

The built-in `demo` workflow is `a → b, c, d → e` (input `{"n":…, "ms":…, "fail":"<step>"}`).

When a step's job succeeds, its output, the run's state and the jobs for the steps it unblocks commit in the
same transaction as the job's completion, holding the run's row lock (`SELECT … FOR UPDATE`). When a step's job
goes `dead` (by error or by crashing its worker), the run fails, steps that hadn't started are `skipped`, and
the `onFailure` steps run with every succeeded step's output, so they know what to undo. Steps already running
finish. A dead step's job can't be retried on its own, since its run has already compensated.

### When things fail

Delivery is **at-least-once**: a job can run more than once, so handlers must be idempotent.

- **Handler error:** the job goes back to `queued` with a backoff, `run_at = now() + min(1 h, 1 s·2^attempt)·rand()`,
  and to `dead` after `max_attempts` runs. `last_error` keeps the latest error.
- **Worker crash:** a claim is a 30 s lease, and the worker extends it every 10 s. The `scheduler`'s reaper
  checks every 5 s and puts jobs whose lease expired back to `queued`, or to `dead` if that was their last
  attempt, so a job that crashes its worker can't loop forever. A crashed worker's job restarts 30–35 s later.
- **Zombie worker** (paused, or cut off from Postgres while its lease expired): every write it makes to the job
  checks `(id, worker_id, attempts)`. Once the job was reclaimed, those writes change 0 rows, the worker abandons
  the job, and its result is dropped.
- **Deploy (SIGTERM):** the worker stops claiming, gives the in-flight job up to 25 s to finish, then cancels it
  and hands it back to `queued` without using up an attempt.
- **Dead jobs:** `POST /api/jobs/{id}/retry` requeues one with fresh attempts; `POST /api/jobs/{id}/discard`
  marks it `cancelled`. The phase 6 dashboard calls these.

The idempotent pattern, as the `effect` kind does it: record the job in `processed` in the same transaction
as the side effect, and skip the side effect if it is already there.

```go
pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
	tag, err := tx.Exec(ctx, `INSERT INTO processed (job_id) VALUES ($1) ON CONFLICT DO NOTHING`, j.ID)
	if err != nil || tag.RowsAffected() == 0 {
		return err // an earlier run already did it
	}
	_, err = tx.Exec(ctx, `INSERT INTO effects (job_id) VALUES ($1)`, j.ID) // the side effect
	return err
})
```

This works when the side effect is in the same database. For an external call (email, payment), pass the job
id as the provider's idempotency key instead.

## Everyday commands

| What | Command |
|---|---|
| Seed demo data | `docker compose run --rm seed` |
| SQL shell | `docker compose exec db psql -U app app` |
| Scale the api | `docker compose up -d --scale api=3` |
| Scale the workers | `docker compose up -d --scale worker=3`, check with `sh infra/worker-smoke.sh 3` |
| Scheduler failover drill | `sh infra/scheduler-failover.sh` (SIGKILLs the leader) |
| Workflow chaos: kill workers mid-DAG | `sh infra/workflow-chaos.sh 4` (300 runs of `demo`) |
| Chaos test: kill workers mid-run | `sh infra/chaos-test.sh 4` (10,000 jobs, about 1–4 minutes) |
| Lint migrations | `sh common/scripts/check-migrations.sh` |
| Measure enqueue → handled | `docker compose --profile loadtest run --rm k6 run /scripts/latency.js` |
| Prove scaling works | `sh common/scripts/smoke.sh 3` |
| The api replica check from CI | `sh common/scripts/ci-stack.sh 3` (ends with `down -v`) |
| Load test at 1, 2, 4 replicas | `sh common/scripts/scale.sh` |
| Metrics (Prometheus) | `docker compose --profile observability up -d`, then http://localhost:9090 |
| Tests, against real Postgres | `docker compose run --rm test` |
| New migration | add `infra/db/migrations/00000N_name.up.sql` + `.down.sql` (start with `SET lock_timeout`; `CONCURRENTLY` alone in its file), restart |
| Update shared code | `git submodule update --remote common` |
| Reset everything | `docker compose down -v` |

## Architecture

```mermaid
flowchart LR
  browser["Browser<br/>dashboard"]
  app["Your app<br/>Go / TypeScript SDK"]
  subgraph stack["docker compose"]
    caddy["Caddy :8080"]
    api["api ×N<br/>enqueue, admin, live updates"]
    worker["worker ×N"]
    sched["scheduler ×2<br/>lease reaper, cron leader"]
    db[("Postgres<br/>jobs, schedules, workflows")]
  end
  browser --> caddy
  app -.->|"enqueue"| caddy
  caddy --> api
  api --> db
  worker -->|"claim: FOR UPDATE SKIP LOCKED<br/>wake: LISTEN/NOTIFY"| db
  sched -->|"requeue expired leases<br/>cron: advisory-lock leader"| db
  class app planned
  classDef planned stroke-dasharray: 5 5
```

Solid boxes and arrows run today. Dashed ones are planned in [PLAN.md](PLAN.md).

Postgres is the only stateful service. Workers claim jobs with `FOR UPDATE SKIP LOCKED`, so any number of them pull from one table without waiting on each other.

## Stack

- **api:** Go 1.27 (`net/http` + `pgx` + Prometheus client), distroless runtime image. Stateless, so it scales with `--scale api=N`.
- **worker:** the same binary run as `api worker`. It has one direct connection for `LISTEN` (`DIRECT_DATABASE_URL`,
  which defaults to `DATABASE_URL`) and a pool for everything else.
- **scheduler:** the same binary run as `api scheduler`, two replicas. Both run the lease reaper (safe
  concurrently); the advisory-lock leader also runs cron. Timezones are embedded (`time/tzdata`), since the
  distroless image has no zoneinfo.
- **Caddy:** the only service with a host port. It load-balances `/api/*` across every
  replica (`import lb api 8080`, from common) and serves `apps/web`.
- **Postgres 18:** migrations run once before the api starts (`golang-migrate`).
- **common/:** git submodule with the shared fakes, scripts, Caddy snippet and Prometheus.
  See [portfolio-common](common/README.md).

## Deploy (one VM)

```bash
docker compose -f compose.yaml -f compose.prod.yaml up -d
```

The server's `.env` needs `SITE_ADDRESS`, `IMAGE_REPO`, `TAG`, a strong password, and no `fakes`
profile. Caddy gets TLS automatically. CI pushes multi-arch images to GHCR on every merge to `main`.

## Decisions & tradeoffs

| Decision | Chosen | Rejected | Why, and what it costs |
|---|---|---|---|
| Broker | Postgres only | Redis (Sidekiq-style), RabbitMQ, Kafka | A job can be enqueued in the same transaction as the app's own write, so it exists exactly when the data does. One stateful service to run and back up. Cost: one Postgres node caps throughput, roughly thousands of jobs/s depending on job size. |
| Claiming jobs | One `UPDATE … WHERE id = ANY(ARRAY(SELECT … FOR UPDATE SKIP LOCKED LIMIT n))` | `SELECT` then `UPDATE`, an advisory lock per job, or the common `id IN (SELECT …)` | One round trip, and concurrent workers skip rows another worker holds instead of queueing behind them. `IN` looks the same, but Postgres may re-scan it and lock more rows each time: on 18 it claimed 5 jobs with `LIMIT 1`. `ARRAY(…)` is evaluated once. |
| Dequeue index | `queue = $1` when a worker serves one queue | Always `queue = ANY($1)` | With `ANY`, Postgres sorts every queued row on each claim (about 12 ms at 50,000 queued) instead of reading the partial index in order (under 1 ms). Cost: a worker with several queues still pays for the sort. |
| Waking workers | `LISTEN/NOTIFY` in the enqueue transaction, plus a 1 s poll | Polling only | Jobs start within milliseconds, and polling catches a missed notification. Cost: `LISTEN` needs a direct connection per worker, not PgBouncer. |
| Delivery guarantee | At-least-once, with leases and a heartbeat | Claiming exactly-once | Exactly-once isn't possible once a worker can die mid-job. Leases fence out zombie workers, and handlers must be idempotent: write a `processed(job_id)` row in the same transaction as the side effect. The chaos test kills every worker 8 times during 10,000 jobs and still gets exactly 10,000 effects. |
| Fencing token | `attempts`, checked with `worker_id` on every heartbeat and completion | `worker_id` alone | A worker's id is its hostname, and a restarted container keeps it. With `worker_id` alone, a zombie could complete the job its own restarted replica had just reclaimed. `attempts` goes up on every claim, so it is never reused. |
| Lease length | 30 s lease, 10 s heartbeat, reaper every 5 s | A short lease without heartbeats | A heartbeat lets a job run longer than its lease, and two missed beats are tolerated. Cost: a crashed worker's job waits 30–35 s before it runs again. |
| Retries | Exponential backoff with full jitter, `min(1 h, 1 s·2^attempt)·rand()`, then `dead` | A fixed delay | Jitter stops failed jobs from retrying in lockstep against a struggling dependency. A crash uses an attempt too, so a job that kills its worker ends in `dead`. |
| Shutdown | Stop claiming, 25 s to finish, then cancel and release the job, giving the attempt back | Waiting for the job, or dropping it | Inside compose's 35 s grace period, so the job is handed back rather than left for the reaper. A deploy doesn't push a job toward `dead`. |
| Reaper | One `UPDATE … SKIP LOCKED` on a partial index of running jobs, on every scheduler replica | Only the leader reaps | Concurrent reapers each take different rows, so it needs no leader. The index only holds running jobs, so it stays tiny however large `jobs` grows. |
| Scheduler leader | `pg_try_advisory_lock` on a direct connection, standbys retry every 1 s | etcd or Raft, or one scheduler | If the leader dies, its connection closes and the lock frees: the CI drill SIGKILLs the leader container and the standby leads 1 s later. Each run's idempotency key (`schedule_id:time`) and `FOR UPDATE SKIP LOCKED` on the schedule row make a brief two-leader overlap harmless. Cost: a leader cut off by a network partition (not killed) holds the lock until TCP notices. |
| Missed cron runs | Enqueue every missed run on takeover, 100 per schedule per tick | Skip to the next future run | The failover test demands no skipped run. Cost: after a long outage a minutely schedule fires a burst of catch-up jobs; disable it first if that's wrong for the job. |
| Cron library | `robfig/cron/v3` evaluated in the schedule's `time.Location` | Computing in UTC | Wall-clock schedules keep their local hour across DST (tested on New York's 2026 spring-forward). |
| Workflow fan-in | Complete the step's job, save its output and enqueue its children in one transaction, holding the run's row lock | Separate updates, or no lock | A crash can't land between "step done" and "children enqueued". Without the lock, two parents finishing at once each see the other still running and the child never starts: removing `FOR UPDATE` makes `TestWorkflowFanOutFanIn` hang every time. Cost: steps of one run complete one at a time. |
| Workflow steps | Ordinary jobs, kind `wf:<workflow>.<step>`, idempotency key `wf:<run>:<step>` | A separate step executor | Steps inherit leases, retries, fencing and the dead-letter state. The key means a step is enqueued at most once per run even if its parents' completion is replayed. The chaos drill SIGKILLs every worker 4 times during 300 runs: all succeed with exactly 1,500 step jobs. |
| Workflow failure | The run fails, pending steps are skipped, `onFailure` steps run (saga) | Distributed transactions | Steps call external systems, which can't join a transaction. Compensation steps get every succeeded step's output. Cost: compensations must be idempotent too. |
| Packaging | One Go binary with `api`, `worker` and `scheduler` subcommands | A separate image per role | One Dockerfile, one image, and the same version everywhere. |
| Load balancing | Caddy `dynamic a` upstreams (the `lb` snippet in `common/`) | `reverse_proxy api:8080`, Traefik | Caddy re-reads Docker DNS every 5 s, so `--scale api=N` needs no config change and no Docker socket mount. A plain `reverse_proxy api:8080` is a single upstream to Caddy and doesn't reliably spread load. |
| Migrations | A one-shot `migrate` service that finishes before the api starts | Each replica migrating on boot | Replicas starting together would race on the same migration. |

Rows for parts that aren't built yet record the plan's choice. Each one gets confirmed or revised when that phase is built.

## Scaling

### Measured

**Enqueue → handled, idle queue.** `infra/loadtest/latency.js` sends 200 `noop` jobs, one at a time, 150–350 ms
apart, so every worker is asleep in `LISTEN` when a job arrives. *Handled* is `finished_at - created_at` on the
Postgres clock. *Round trip* is what a client sees through Caddy, from sending the POST to a polled GET that
says `succeeded`.

**On an idle system a job is handled 1 ms after enqueue at p50 and 2 ms at p95**, against a target of under
50 ms. That's [one CI run](https://github.com/gonzalo-gigena/job-queue/actions/runs/44981277382) at commit `14b00b3`, with 3 workers on a GitHub-hosted `ubuntu-26.04` amd64
runner that ran nothing else:

| | p50 | p95 | p99 | max |
|---|---|---|---|---|
| handled | 1 ms | 2 ms | 4 ms | 7 ms |
| round trip | 4 ms | 5 ms | 9 ms | 10 ms |

The script compares whole milliseconds, so each value is ±1 ms.

For comparison, under load on an Apple M1 laptop (8 CPUs, 8 GB for Docker). **This machine was not idle:** other projects' stacks shared
the Docker VM, one of them using up to 4 cores, and the host's load average was 12–21. All eight runs:

| workers | run | handled p50 | handled p95 | handled p99 | round trip p50 | round trip p95 |
|---|---|---|---|---|---|---|
| 1 | 1 | 3 ms | 22 ms | 54 ms | 8 ms | 40 ms |
| 1 | 2 | 3 ms | 12 ms | 26 ms | 8 ms | 22 ms |
| 1 | 3 | 10 ms | 59 ms | 121 ms | 23 ms | 127 ms |
| 1 | 4 | 7 ms | 27 ms | 40 ms | 17 ms | 51 ms |
| 3 | 1 | 8 ms | 24 ms | 33 ms | 16 ms | 47 ms |
| 3 | 2 | 24 ms | 88 ms | 103 ms | 38 ms | 100 ms |
| 3 | 3 | 12 ms | 75 ms | 110 ms | 23 ms | 91 ms |
| 3 | 4 | 7 ms | 47 ms | 111 ms | 17 ms | 81 ms |

The median stayed under 50 ms in every run. The p95 went over in three of the eight, so under heavy CPU
contention the target doesn't hold at p95. CI runs the same script on every PR and fails above 50 ms at p95.

**HTTP, from the skeleton:** k6 with 20 virtual users for 20 s against `GET /api/hello`, which reads one row from Postgres through Caddy. Run on an Apple M1 laptop with 8 CPUs and 8 GB given to Docker, with the dev override left out:

```bash
COMPOSE_FILE=compose.yaml sh common/scripts/scale.sh
```

| api replicas | req/s | p95 latency | failed |
|---|---|---|---|
| 1 | 13,526 | 3.09 ms | 0.00% |
| 2 | 14,438 | 2.96 ms | 0.00% |
| 4 | 14,468 | 3.06 ms | 0.00% |

4 replicas served 1.1× the requests of 1. One Go process already spreads across every core, and the run shares 8 CPUs with Caddy, Postgres and k6, so on one machine extra api replicas buy redundancy, not throughput. Replica counts matter for `worker`, which phase 7 measures.

**Chaos test.** `infra/chaos-test.sh 4` inserts 10,000 `effect` jobs (each sleeps 10 ms after its side effect)
for 4 workers, then 8 times waits 2–6 s, SIGKILLs every worker and restarts them 0–2 s later. In
[one CI run](https://github.com/gonzalo-gigena/job-queue/actions/runs/45093265380) at commit `b52ac04`: all 10,000
succeeded in 86 s, `effects` had exactly 10,000 rows, nothing was left `running`, and 27 jobs ran twice, all 27
requeued by the reaper after their lease expired. On the shared M1 laptop the same test took 211 s, with 18 jobs
run twice. CI runs it on every PR and fails on any duplicate or lost job.

### How each part scales

| Part | Scales by | Limit |
|---|---|---|
| `api` | `--scale api=N` (stateless) | Postgres connections |
| `worker` | `--scale worker=N`, one job at a time each | Throughput grows until Postgres saturates. Phase 5 adds per-queue concurrency and rate limits that hold across replicas. |
| `scheduler` | 2 replicas, one leader | The reaper runs on every replica. For cron, only the leader works and the standby takes over in about 1 s. |
| Postgres | One primary | The ceiling for the whole system |

### Where it breaks next

- **The dequeue query on a busy Postgres.** First: PgBouncer for workers (transaction mode), a 0–50 ms random delay after each NOTIFY so idle workers don't all query at once, and archiving finished jobs to a day-partitioned table so the hot table stays small and vacuum keeps up.
- **Past one Postgres node.** Shard queues across databases, or move to Kafka or NATS. Portfolio-scale load never gets there.

Phase 7 adds the real test: jobs/s at 1, 2, 4 and 8 workers with no-op jobs and with 50 ms jobs (`REPLICAS="1 2 4 8" SERVICE=worker`).
