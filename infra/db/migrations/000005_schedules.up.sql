SET lock_timeout = '3s';

-- Cron schedules. The scheduler leader enqueues each due run with the idempotency key
-- id:run_time and moves next_run_at forward, in one transaction.
CREATE TABLE schedules (
  id          text PRIMARY KEY CHECK (id ~ '^[a-z0-9_-]{1,100}$'),
  cron_expr   text NOT NULL,
  timezone    text NOT NULL DEFAULT 'UTC',
  queue       text NOT NULL CHECK (queue ~ '^[a-z0-9_]{1,50}$'),
  kind        text NOT NULL,
  payload     jsonb NOT NULL DEFAULT '{}',
  next_run_at timestamptz NOT NULL,
  enabled     boolean NOT NULL DEFAULT true
);

CREATE INDEX schedules_due ON schedules (next_run_at) WHERE enabled;
