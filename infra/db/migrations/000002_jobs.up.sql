SET lock_timeout = '3s';

CREATE TABLE jobs (
  id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  queue           text NOT NULL CHECK (queue ~ '^[a-z0-9_]{1,50}$'), -- also names the NOTIFY channel jobs_<queue>
  kind            text NOT NULL,
  payload         jsonb NOT NULL DEFAULT '{}',
  status          text NOT NULL DEFAULT 'queued'
                  CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'dead', 'cancelled')),
  priority        int NOT NULL DEFAULT 0,
  run_at          timestamptz NOT NULL DEFAULT now(),
  attempts        int NOT NULL DEFAULT 0,
  max_attempts    int NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
  lease_until     timestamptz,
  worker_id       text,
  idempotency_key text,
  last_error      text,
  created_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
  finished_at     timestamptz,
  -- NULL keys never collide, so jobs without a key are never deduplicated.
  UNIQUE (queue, idempotency_key)
);

-- The table is new and empty, so a plain CREATE INDEX holds its lock for no time.
-- Later indexes on jobs go in their own migration with CREATE INDEX CONCURRENTLY.
CREATE INDEX jobs_dequeue ON jobs (queue, priority DESC, run_at) WHERE status = 'queued';
