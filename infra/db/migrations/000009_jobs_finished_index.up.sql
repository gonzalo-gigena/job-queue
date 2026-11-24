-- What the archiver moves, and what the dashboard counts as done per minute.
CREATE INDEX CONCURRENTLY jobs_finished ON jobs (finished_at) WHERE status IN ('succeeded', 'cancelled');
