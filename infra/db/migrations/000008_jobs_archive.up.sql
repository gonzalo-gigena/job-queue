SET lock_timeout = '3s';

-- Finished jobs move here after ARCHIVE_AFTER, so the hot jobs table stays small and vacuum keeps up.
-- Partitioned by day on finished_at: retention drops whole partitions instead of deleting rows.
-- jobs itself stays unpartitioned: a unique index on a partitioned table must include the
-- partition key, which would break UNIQUE (queue, idempotency_key).
-- Same columns in the same order as jobs (the archiver does INSERT … SELECT *): a column
-- added to jobs must be added here too, in the same migration.
CREATE TABLE jobs_archive (LIKE jobs) PARTITION BY RANGE (finished_at);
-- The scheduler leader creates each day's partition ahead of time and drops expired ones.

-- jobs is a queue: every row is updated a few times and then deleted by the archiver. The
-- default thresholds (20% of the table dead before a vacuum) let a busy jobs table bloat
-- between vacuums; 1% keeps the dequeue index scans short.
ALTER TABLE jobs SET (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_vacuum_insert_scale_factor = 0.01,
  autovacuum_analyze_scale_factor = 0.02
);
