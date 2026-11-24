#!/bin/sh
# Phase 8 check: the example app, which only uses the TypeScript client, gets jobs and
# workflow runs through to succeeded.
set -eu
dc="docker compose -f compose.yaml"
sql() {
  # shellcheck disable=SC2016 # the variables expand inside the container
  $dc exec -T db sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -tAc "$1"' sh "$1"
}
since=$(sql "SELECT now()")
$dc --profile demo up -d --wait --force-recreate example-app
for _ in $(seq 1 40); do
  set -- $(sql "SELECT (SELECT count(*) FROM jobs WHERE kind = 'sleep' AND payload ? 'user' AND status = 'succeeded' AND created_at > '$since') || ' ' ||
                       (SELECT count(*) FROM workflow_runs WHERE created_at > '$since' AND status <> 'running')")
  if [ "$1" -gt 2 ] && [ "$2" -gt 2 ]; then
    $dc --profile demo stop example-app >/dev/null 2>&1
    echo "OK: example app's jobs ($1 welcome emails) and workflow runs ($2 finished) went through"
    exit 0
  fi
  sleep 1
done
$dc --profile demo logs example-app | tail -20
echo "FAIL: $1 jobs and $2 runs from the example app finished" >&2
exit 1
