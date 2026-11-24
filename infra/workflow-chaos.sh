#!/bin/sh
# Phase 4 check: RUNS runs of the 5-step `demo` DAG (a → b, c, d → e) while every worker is
# SIGKILLed at random. Passes when every run succeeded with the right fan-in result
# (e = 3·(n+1)), each run has exactly 5 step jobs, and nothing is left running.
# Usage: sh infra/workflow-chaos.sh [workers]   (RUNS=300 ROUNDS=4 STEP_MS=30)
set -eu
workers=${1:-4}
runs=${RUNS:-300}
rounds=${ROUNDS:-4}
dc="docker compose -f compose.yaml"
base=${BASE_URL:-http://localhost:${HTTP_PORT:-8080}}
tag="wfchaos-$$-$(date +%s)"
sql() {
  # shellcheck disable=SC2016 # the variables expand inside the container
  $dc exec -T db sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -tAc "$1"' sh "$1"
}
rand() { awk -v a="$1" -v b="$2" -v s="$(od -An -N4 -tu4 /dev/urandom)" 'BEGIN { srand(s); printf "%.1f", a + rand() * (b - a) }'; }
where="workflow = 'demo' AND input->>'tag' = '$tag'"

$dc up -d --wait --build --scale worker="$workers"
start=$(date +%s)
i=0
while [ "$i" -lt "$runs" ]; do
  i=$((i + 1))
  curl -fsS -o /dev/null -H 'Content-Type: application/json' \
    -d "{\"input\":{\"n\":$i,\"ms\":${STEP_MS:-30},\"tag\":\"$tag\"}}" "$base/api/workflows/demo/runs"
done
echo "started $runs runs, $workers workers"

i=0
while [ "$i" -lt "$rounds" ]; do
  i=$((i + 1))
  sleep "$(rand 1 4)"
  $dc kill -s SIGKILL worker >/dev/null 2>&1
  echo "round $i: killed every worker ($(sql "SELECT count(*) FILTER (WHERE status = 'succeeded') FROM workflow_runs WHERE $where") of $runs runs done)"
  sleep "$(rand 0 2)"
  $dc start worker >/dev/null 2>&1
done

deadline=$(($(date +%s) + ${TIMEOUT:-240}))
while :; do
  set -- $(sql "SELECT count(*) FILTER (WHERE status = 'succeeded') || ' ' || count(*) FILTER (WHERE status = 'failed') FROM workflow_runs WHERE $where")
  [ "$1" -eq "$runs" ] && break
  if [ "$2" -gt 0 ] || [ "$(date +%s)" -ge "$deadline" ]; then
    echo "FAIL: $1 succeeded, $2 failed of $runs runs" >&2
    exit 1
  fi
  sleep 2
done

set -- $(sql "SELECT count(*) FILTER (WHERE (s.output->>'n')::int = 3 * ((r.input->>'n')::int + 1)) || ' ' ||
                     (SELECT count(*) FROM jobs j JOIN workflow_runs r2 ON j.idempotency_key LIKE 'wf:' || r2.id || ':%' WHERE r2.$where) || ' ' ||
                     (SELECT count(*) FROM workflow_steps s2 JOIN workflow_runs r3 ON r3.id = s2.run_id WHERE r3.$where AND NOT s2.on_failure AND s2.status <> 'succeeded')
              FROM workflow_runs r JOIN workflow_steps s ON s.run_id = r.id AND s.step = 'e' WHERE r.$where")
right=$1 stepjobs=$2 unfinished=$3
set -- $(sql "SELECT count(*) FROM jobs j JOIN workflow_runs r ON j.idempotency_key LIKE 'wf:' || r.id || ':%'
              WHERE r.$where AND j.attempts > 1")
echo "succeeded $runs/$runs runs in $(($(date +%s) - start)) s; fan-in correct in $right; $stepjobs step jobs; $1 step jobs ran more than once"
[ "$right" -eq "$runs" ] || { echo "FAIL: wrong fan-in result in $((runs - right)) runs" >&2; exit 1; }
[ "$stepjobs" -eq $((runs * 5)) ] || { echo "FAIL: $stepjobs step jobs, want $((runs * 5))" >&2; exit 1; }
[ "$unfinished" -eq 0 ] || { echo "FAIL: $unfinished steps not succeeded" >&2; exit 1; }
[ "$1" -gt 0 ] || { echo "FAIL: no step was re-run, so the kills tested nothing" >&2; exit 1; }
echo "OK: workflow chaos test passed"
