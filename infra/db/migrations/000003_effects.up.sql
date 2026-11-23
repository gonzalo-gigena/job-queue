SET lock_timeout = '3s';

-- The idempotent-handler pattern from the README, used by the `effect` job kind and the chaos test.
-- A handler inserts processed(job_id) and does its side effect in one transaction, so a job that
-- runs twice (at-least-once delivery) does the side effect once.
CREATE TABLE processed (
  job_id       bigint PRIMARY KEY,
  processed_at timestamptz NOT NULL DEFAULT now()
);

-- Deliberately no unique key: a duplicate run that skipped the processed check would show up here.
CREATE TABLE effects (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  job_id     bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
