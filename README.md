# Job queue and workflow engine

A Postgres-backed job queue with retries, cron schedules, multi-step workflows and a live dashboard.

The step-by-step build plan is in [PLAN.md](PLAN.md).

> **Status:** skeleton. The stack runs, scales and passes CI. The product itself isn't built yet.

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

## Everyday commands

| What | Command |
|---|---|
| Seed demo data | `docker compose run --rm seed` |
| SQL shell | `docker compose exec db psql -U app app` |
| Scale the api | `docker compose up -d --scale api=3` |
| Prove scaling works | `sh common/scripts/smoke.sh 3` |
| Run exactly what CI runs | `sh common/scripts/ci-stack.sh 3` |
| Load test at 1, 2, 4 replicas | `sh common/scripts/scale.sh` |
| Metrics (Prometheus) | `docker compose --profile observability up -d`, then http://localhost:9090 |
| Unit tests | `docker run --rm -v "$PWD/apps/api:/src" -w /src golang:1.27.4-bookworm sh -c "go vet ./... && go test ./..."` |
| New migration | add `infra/db/migrations/00000N_name.up.sql` + `.down.sql`, restart |
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
    sched["scheduler ×2<br/>cron + lease reaper, one leader"]
    db[("Postgres<br/>jobs, schedules, workflows")]
  end
  browser --> caddy
  app -.->|"enqueue"| caddy
  caddy --> api
  api --> db
  worker -.->|"claim: FOR UPDATE SKIP LOCKED<br/>wake: LISTEN/NOTIFY"| db
  sched -.->|"leader: advisory lock"| db
  class app,worker,sched planned
  classDef planned stroke-dasharray: 5 5
```

Solid boxes and arrows run today. Dashed ones are planned in [PLAN.md](PLAN.md).

Postgres is the only stateful service. Workers claim jobs with `FOR UPDATE SKIP LOCKED`, so any number of them pull from one table without waiting on each other.

## Stack

- **api:** Go 1.27 (`net/http` + `pgx` + Prometheus client), distroless runtime image. Stateless, so it scales with `--scale api=N`.
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
| Claiming jobs | One `UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED)` | `SELECT` then `UPDATE`, or an advisory lock per job | One round trip, and concurrent workers skip rows another worker holds instead of queueing behind them. |
| Waking workers | `LISTEN/NOTIFY`, plus polling as a fallback | Polling only | Jobs start within milliseconds, and polling catches a missed notification. Cost: `LISTEN` needs a direct connection per worker, not PgBouncer. |
| Delivery guarantee | At-least-once, with leases and a heartbeat | Claiming exactly-once | Exactly-once isn't possible once a worker can die mid-job. Leases fence out zombie workers, and handlers must be idempotent: write a `processed(job_id)` row in the same transaction as the side effect. |
| Retries | Exponential backoff with full jitter, then a dead-letter state | A fixed delay | Jitter stops failed jobs from retrying in lockstep against a struggling dependency. |
| Scheduler leader | `pg_try_advisory_lock` on a direct connection | etcd or Raft, or one scheduler | If the leader dies, its connection closes and the lock frees. Each run's idempotency key (`schedule_id:time`) makes a brief two-leader overlap harmless. |
| Workflow fan-in | Mark the step done and enqueue its children in one transaction, holding the run's row lock | Separate updates | Without the lock, two parents finishing at once each see the other still running, and the child never starts. |
| Packaging | One Go binary with `api`, `worker` and `scheduler` subcommands | A separate image per role | One Dockerfile, one image, and the same version everywhere. |
| Load balancing | Caddy `dynamic a` upstreams (the `lb` snippet in `common/`) | `reverse_proxy api:8080`, Traefik | Caddy re-reads Docker DNS every 5 s, so `--scale api=N` needs no config change and no Docker socket mount. A plain `reverse_proxy api:8080` is a single upstream to Caddy and doesn't reliably spread load. |
| Migrations | A one-shot `migrate` service that finishes before the api starts | Each replica migrating on boot | Replicas starting together would race on the same migration. |

Rows for parts that aren't built yet record the plan's choice. Each one gets confirmed or revised when that phase is built.

## Scaling

### Measured

The skeleton only, not the product yet: k6 with 20 virtual users for 20 s against `GET /api/hello`, which reads one row from Postgres through Caddy. Run on an Apple M1 laptop with 8 CPUs and 8 GB given to Docker, with the dev override left out:

```bash
COMPOSE_FILE=compose.yaml sh common/scripts/scale.sh
```

| api replicas | req/s | p95 latency | failed |
|---|---|---|---|
| 1 | 13,526 | 3.09 ms | 0.00% |
| 2 | 14,438 | 2.96 ms | 0.00% |
| 4 | 14,468 | 3.06 ms | 0.00% |

4 replicas served 1.1× the requests of 1. One Go process already spreads across every core, and the run shares 8 CPUs with Caddy, Postgres and k6, so on one machine extra api replicas buy redundancy, not throughput. Replica counts matter for `worker`, which phase 7 measures.

### How each part scales

| Part | Scales by | Limit |
|---|---|---|
| `api` | `--scale api=N` (stateless) | Postgres connections |
| `worker` | `--scale worker=N` | Throughput grows until Postgres saturates. Per-queue concurrency and rate limits hold across replicas. |
| `scheduler` | 2 replicas, one leader | Only the leader works. The standby takes over within ~5 s. |
| Postgres | One primary | The ceiling for the whole system |

### Where it breaks next

- **The dequeue query on a busy Postgres.** First: PgBouncer for workers (transaction mode), a 0–50 ms random delay after each NOTIFY so idle workers don't all query at once, and archiving finished jobs to a day-partitioned table so the hot table stays small and vacuum keeps up.
- **Past one Postgres node.** Shard queues across databases, or move to Kafka or NATS. Portfolio-scale load never gets there.

Phase 7 adds the real test: jobs/s at 1, 2, 4 and 8 workers with no-op jobs and with 50 ms jobs (`REPLICAS="1 2 4 8" SERVICE=worker`).
