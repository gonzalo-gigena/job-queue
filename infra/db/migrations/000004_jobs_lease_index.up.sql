-- The reaper's lookup: running jobs by lease. Few rows match, so the index stays tiny.
CREATE INDEX CONCURRENTLY jobs_lease ON jobs (lease_until) WHERE status = 'running';
