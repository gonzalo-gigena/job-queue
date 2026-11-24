#!/bin/sh
# Phase 7: jobs/s at several worker counts, for no-op jobs and 50 ms jobs.
# jobs/s = jobs / (last finished_at - first started_at), on Postgres's clock.
# Usage: sh infra/throughput.sh   (REPLICAS="1 2 4 8" NOOP_JOBS=5000 SLOW_PER_WORKER=200)
set -eu
dc="docker compose -f compose.yaml"
sql() {
  # shellcheck disable=SC2016 # the variables expand inside the container
  $dc exec -T db sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -tAc "$1"' sh "$1"
}
bench() { # workers kind ms jobs
  run="tp-$$-$1-$2-$(date +%s)"
  sql "INSERT INTO jobs (queue, kind, payload) SELECT 'default', '$2', jsonb_build_object('ms', $3, 'run', '$run') FROM generate_series(1, $4)" >/dev/null
  while [ "$(sql "SELECT count(*) FROM jobs WHERE payload->>'run' = '$run' AND status <> 'succeeded'")" -gt 0 ]; do sleep 1; done
  sql "SELECT round($4 / extract(epoch FROM max(finished_at) - min(started_at))) FROM jobs WHERE payload->>'run' = '$run'"
}
echo "workers | no-op jobs/s | 50 ms jobs/s"
for n in ${REPLICAS:-1 2 4 8}; do
  $dc up -d --wait --scale worker="$n" >/dev/null 2>&1
  sleep 2
  noop=$(bench "$n" noop 0 "${NOOP_JOBS:-5000}")
  slow=$(bench "$n" sleep 50 $((n * ${SLOW_PER_WORKER:-200})))
  echo "$n | $noop | $slow"
done
