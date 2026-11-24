SET lock_timeout = '3s';

CREATE TABLE workflow_runs (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  workflow    text NOT NULL,
  input       jsonb NOT NULL DEFAULT '{}',
  status      text NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'succeeded', 'failed')),
  created_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
  finished_at timestamptz
);

-- One row per step per run. A step's job (kind wf:<workflow>.<step>) is enqueued with the
-- idempotency key wf:<run_id>:<step>, so it exists at most once.
CREATE TABLE workflow_steps (
  run_id     bigint NOT NULL REFERENCES workflow_runs ON DELETE CASCADE,
  step       text NOT NULL,
  depends_on text[] NOT NULL DEFAULT '{}',
  on_failure boolean NOT NULL DEFAULT false, -- compensation: runs only if the run fails
  status     text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'queued', 'succeeded', 'failed', 'skipped')),
  output     jsonb,
  job_id     bigint,
  PRIMARY KEY (run_id, step)
);
