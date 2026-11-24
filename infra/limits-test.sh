#!/bin/sh
# Phase 5 check: with 5 worker replicas, a queue's concurrency limit and rate limit are never
# exceeded. Overlap and rate are computed from started_at/finished_at, on Postgres's clock.
# Usage: sh infra/limits-test.sh [workers]
set -eu
workers=${1:-5}
dc="docker compose -f compose.yaml"
base=${BASE_URL:-http://localhost:${HTTP_PORT:-8080}}
run="limits-$$-$(date +%s)"
sql() {
  # shellcheck disable=SC2016 # the variables expand inside the container
  $dc exec -T db sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -tAc "$1"' sh "$1"
}
limits() { curl -fsS -o /dev/null -X PUT -H 'Content-Type: application/json' -d "$1" "$base/api/queues/default/limits"; }
wait_done() {
  for _ in $(seq 1 120); do
    [ "$(sql "SELECT count(*) FROM jobs WHERE payload->>'run' = '$1' AND status <> 'succeeded'")" -eq 0 ] && return
    sleep 1
  done
  echo "FAIL: $1 jobs didn't finish" >&2; exit 1
}
trap 'curl -fsS -o /dev/null -X DELETE "$base/api/queues/default/limits" || true' EXIT

$dc up -d --wait --build --scale worker="$workers"

# Concurrency: at most 2 of 60 100 ms jobs at once, across every replica.
limits '{"max_running":2}'
sql "INSERT INTO jobs (queue, kind, payload) SELECT 'default', 'sleep', jsonb_build_object('ms', 100, 'run', '$run-c') FROM generate_series(1, 60)" >/dev/null
wait_done "$run-c"
set -- $(sql "WITH j AS (SELECT started_at s, finished_at f, worker_id w FROM jobs WHERE payload->>'run' = '$run-c')
              SELECT max((SELECT count(*) FROM j b WHERE b.s <= a.s AND b.f > a.s)) || ' ' || count(DISTINCT w) FROM j a")
echo "concurrency: peak $1 running at once with max_running 2, $2 workers took jobs"
[ "$1" -le 2 ] && [ "$1" -eq 2 ] || { echo "FAIL: peak $1, want exactly 2" >&2; exit 1; }
[ "$2" -gt 1 ] || { echo "FAIL: one worker took every job, so nothing ran concurrently" >&2; exit 1; }

# Rate: at most 20 starts per second (burst 1) for 200 no-op jobs.
limits '{"rate_per_sec":20,"burst":1}'
sql "INSERT INTO jobs (queue, kind, payload) SELECT 'default', 'noop', jsonb_build_object('run', '$run-r') FROM generate_series(1, 200)" >/dev/null
wait_done "$run-r"
set -- $(sql "WITH j AS (SELECT started_at s FROM jobs WHERE payload->>'run' = '$run-r')
              SELECT max((SELECT count(*) FROM j b WHERE b.s >= a.s AND b.s < a.s + interval '1 s')) || ' ' ||
                     round(199 / extract(epoch FROM max(s) - min(s)), 1) FROM j a")
echo "rate: at most $1 starts in any 1 s window, $2/s overall, with rate_per_sec 20, burst 1"
[ "$1" -le 21 ] || { echo "FAIL: $1 starts in one second" >&2; exit 1; }
echo "OK: limits held across $workers workers"
