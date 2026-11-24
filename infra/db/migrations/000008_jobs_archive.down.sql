ALTER TABLE jobs RESET (autovacuum_vacuum_scale_factor, autovacuum_vacuum_insert_scale_factor, autovacuum_analyze_scale_factor);
DROP TABLE jobs_archive;
