#!/bin/sh
# Scaling check: every one of N worker replicas takes jobs, and each job runs once.
# Usage: docker compose up -d --wait --scale worker=3 && sh infra/worker-smoke.sh 3
set -eu
workers=${1:-1}
jobs=$((workers * 20))
base=${BASE_URL:-http://localhost:${HTTP_PORT:-8080}}
run="smoke-$$-$(date +%s)"

i=0
while [ "$i" -lt "$jobs" ]; do
  i=$((i + 1))
  curl -fsS -o /dev/null -H 'Content-Type: application/json' \
    -d "{\"kind\":\"sleep\",\"payload\":{\"ms\":200,\"run\":\"$run\"}}" "$base/api/jobs"
done

query="SELECT count(*) FILTER (WHERE status = 'succeeded' AND attempts = 1) || ' ' || count(DISTINCT worker_id)
       FROM jobs WHERE payload->>'run' = '$run'"
for _ in $(seq 1 60); do
  # shellcheck disable=SC2016 # the variables expand inside the container
  result=$(docker compose -f compose.yaml exec -T db sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "$1"' sh "$query")
  ok=${result% *}
  seen=${result#* }
  if [ "$ok" -eq "$jobs" ]; then
    if [ "$seen" -eq "$workers" ]; then
      echo "OK: $jobs jobs succeeded on the first attempt, shared by $seen workers"
      exit 0
    fi
    echo "FAIL: $seen workers took jobs, wanted $workers" >&2
    exit 1
  fi
  sleep 1
done
echo "FAIL: $ok of $jobs jobs succeeded" >&2
exit 1
