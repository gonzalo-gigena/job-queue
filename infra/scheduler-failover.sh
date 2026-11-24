#!/bin/sh
# Phase 3 check: SIGKILL the scheduler leader; a standby must take over within 5 s, and an
# every-second schedule must have no skipped and no duplicated run across the failover.
# Usage: sh infra/scheduler-failover.sh   (needs the stack up with 2 scheduler replicas)
set -eu
dc="docker compose -f compose.yaml"
base=${BASE_URL:-http://localhost:${HTTP_PORT:-8080}}
id="failover-$$"
sql() {
  # shellcheck disable=SC2016 # the variables expand inside the container
  $dc exec -T db sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -tAc "$1"' sh "$1"
}
leader_ip() { sql "SELECT a.client_addr FROM pg_locks l JOIN pg_stat_activity a USING (pid) WHERE l.locktype = 'advisory' AND l.objid = 7171003 AND l.granted"; }

curl -fsS -o /dev/null -X PUT -H 'Content-Type: application/json' \
  -d '{"cron":"* * * * * *","kind":"noop","queue":"default"}' "$base/api/schedules/$id"
sleep 4

ip=$(leader_ip)
[ -n "$ip" ] || { echo "FAIL: no leader" >&2; exit 1; }
victim=$(for c in $($dc ps -q scheduler); do
  docker inspect -f '{{.Name}} {{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$c"
done | awk -v ip="$ip" '$2 == ip { sub("^/", "", $1); print $1 }')
[ -n "$victim" ] || { echo "FAIL: no scheduler container has the leader's address $ip" >&2; exit 1; }

docker kill -s SIGKILL "$victim" >/dev/null
killed=$(date +%s)
echo "killed leader $victim ($ip)"
while :; do
  new=$(leader_ip)
  [ -n "$new" ] && [ "$new" != "$ip" ] && break
  [ $(($(date +%s) - killed)) -le 5 ] || { echo "FAIL: no new leader within 5 s" >&2; exit 1; }
  sleep 0.2
done
took=$(($(date +%s) - killed))
echo "new leader at $new after ${took}s"
sleep 4
docker start "$victim" >/dev/null
curl -fsS -o /dev/null -X DELETE "$base/api/schedules/$id"

# A run per second, consecutive, each once (the unique key alone would hide duplicates, so count distinct too).
set -- $(sql "SELECT count(*) || ' ' || count(DISTINCT run_at) || ' ' || (extract(epoch FROM max(run_at) - min(run_at)) + 1)::int
              FROM jobs WHERE idempotency_key LIKE '$id:%'")
echo "$1 runs, $2 distinct, spanning $3 s"
[ "$1" -eq "$2" ] && [ "$1" -eq "$3" ] || { echo "FAIL: a run was skipped or duplicated" >&2; exit 1; }
echo "OK: failover in ${took}s, no run skipped or duplicated"
