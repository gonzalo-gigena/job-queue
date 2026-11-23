#!/bin/sh
# Headline test (PLAN.md): 10,000 jobs whose handler writes a row to `effects` idempotently,
# while every worker is SIGKILLed at random intervals and restarted. Passes when every job
# succeeded, `effects` has exactly one row per job, and nothing is left running.
# Usage: sh infra/chaos-test.sh [workers]   (JOBS=10000 ROUNDS=8 JOB_MS=10)
set -eu
workers=${1:-4}
jobs=${JOBS:-10000}
rounds=${ROUNDS:-8}
dc="docker compose -f compose.yaml"
run="chaos-$$-$(date +%s)"

sql() {
  # shellcheck disable=SC2016 # the variables expand inside the container
  $dc exec -T db sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -tAc "$1"' sh "$1"
}
rand() { # a decimal in [$1, $2)
  awk -v a="$1" -v b="$2" -v s="$(od -An -N4 -tu4 /dev/urandom)" 'BEGIN { srand(s); printf "%.1f", a + rand() * (b - a) }'
}

$dc up -d --wait --build --scale worker="$workers"
start=$(date +%s)
sql "INSERT INTO jobs (queue, kind, payload) SELECT 'default', 'effect', jsonb_build_object('ms', ${JOB_MS:-10}, 'run', '$run') FROM generate_series(1, $jobs)" >/dev/null
echo "enqueued $jobs jobs, $workers workers"

state() {
  sql "SELECT count(*) FILTER (WHERE status = 'succeeded') || ' ' || count(*) FILTER (WHERE status = 'running')
              || ' ' || count(*) FILTER (WHERE status IN ('dead', 'failed', 'cancelled'))
       FROM jobs WHERE payload->>'run' = '$run'"
}

i=0
while [ "$i" -lt "$rounds" ]; do
  i=$((i + 1))
  sleep "$(rand 2 6)"
  $dc kill -s SIGKILL worker >/dev/null 2>&1
  echo "round $i: killed every worker (succeeded running dead: $(state))"
  sleep "$(rand 0 2)"
  $dc start worker >/dev/null 2>&1
done

# Jobs the kills orphaned come back when their 30 s lease expires and the reaper requeues them.
deadline=$(($(date +%s) + ${TIMEOUT:-300}))
while :; do
  set -- $(state)
  [ "$1" -eq "$jobs" ] && break
  if [ "$3" -gt 0 ] || [ "$(date +%s)" -ge "$deadline" ]; then
    echo "FAIL: succeeded $1, running $2, dead/failed $3 of $jobs" >&2
    exit 1
  fi
  sleep 2
done

set -- $(sql "SELECT count(*) || ' ' || count(DISTINCT e.job_id) FROM effects e JOIN jobs j ON j.id = e.job_id WHERE j.payload->>'run' = '$run'")
effects=$1 distinct=$2
set -- $(sql "SELECT count(*) FILTER (WHERE status = 'running') || ' ' || count(*) FILTER (WHERE attempts > 1)
              || ' ' || count(*) FILTER (WHERE attempts > 1 AND last_error LIKE 'lease expired%')
       FROM jobs WHERE payload->>'run' = '$run'")
running=$1 rerun=$2 reaped=$3
echo "succeeded $jobs/$jobs in $(($(date +%s) - start)) s; effects $effects ($distinct distinct); running $running;" \
  "$rerun jobs ran more than once, $reaped of them requeued by the reaper"
[ "$effects" -eq "$jobs" ] && [ "$distinct" -eq "$jobs" ] || { echo "FAIL: effects $effects, want $jobs" >&2; exit 1; }
[ "$running" -eq 0 ] || { echo "FAIL: $running jobs left running" >&2; exit 1; }
[ "$reaped" -gt 0 ] || { echo "FAIL: no job was requeued by the reaper, so the kills tested nothing" >&2; exit 1; }
echo "OK: chaos test passed"
