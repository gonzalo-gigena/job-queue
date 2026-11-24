SET lock_timeout = '3s';

-- Per-queue limits, enforced in the claim transaction under this row's lock. A queue with
-- no row is unlimited. NULL max_running or rate_per_sec means no limit of that kind.
CREATE TABLE queue_limits (
  queue        text PRIMARY KEY CHECK (queue ~ '^[a-z0-9_]{1,50}$'),
  max_running  int CHECK (max_running > 0),
  rate_per_sec double precision CHECK (rate_per_sec > 0),
  burst        double precision NOT NULL DEFAULT 1 CHECK (burst >= 1),
  tokens       double precision NOT NULL DEFAULT 1, -- token bucket, refilled lazily at claim time
  refilled_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
  paused       boolean NOT NULL DEFAULT false
);

-- When the latest attempt started: the dashboard shows it, and the limits test checks overlap with it.
-- Nullable with no default, so adding it doesn't rewrite the table.
ALTER TABLE jobs ADD COLUMN started_at timestamptz;
