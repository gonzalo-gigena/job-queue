package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Succeeded and cancelled jobs move to jobs_archive after archiveAfter; partitions older than
// archiveRetention are dropped. Dead jobs stay in jobs until someone retries or discards them.
// An archived job's idempotency key is free again: keys dedupe for archiveAfter, no longer.
var (
	archiveAfter     = time.Hour
	archiveRetention = 7 * 24 * time.Hour
)

const archiveBatch = 5000

// archive moves up to archiveBatch finished jobs in one statement. Call it until it returns < archiveBatch.
func archive(ctx context.Context, db *pgxpool.Pool) (int64, error) {
	tag, err := db.Exec(ctx, `
		WITH moved AS (
		  DELETE FROM jobs WHERE id = ANY(ARRAY(
		    SELECT id FROM jobs WHERE status IN ('succeeded', 'cancelled') AND finished_at < now() - $1 * interval '1 ms'
		    ORDER BY finished_at LIMIT $2 FOR UPDATE SKIP LOCKED))
		  RETURNING *)
		INSERT INTO jobs_archive SELECT * FROM moved`, archiveAfter.Milliseconds(), archiveBatch)
	return tag.RowsAffected(), err
}

// partitions creates jobs_archive's partitions for yesterday through two days ahead, and drops
// those wholly older than archiveRetention. It also prunes processed markers that old: an
// archived job can't be delivered again, so its marker has nothing left to guard.
func partitions(ctx context.Context, db *pgxpool.Pool, now time.Time) error {
	day := func(t time.Time) time.Time { return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC) }
	return pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '3s'`); err != nil {
			return err
		}
		today := day(now.UTC())
		for d := -1; d <= 2; d++ {
			from := today.AddDate(0, 0, d)
			name := pgx.Identifier{"jobs_archive_" + from.Format("20060102")}.Sanitize()
			if _, err := tx.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s PARTITION OF jobs_archive FOR VALUES FROM ('%s') TO ('%s')`,
				name, from.Format(time.DateOnly), from.AddDate(0, 0, 1).Format(time.DateOnly))); err != nil {
				return err
			}
		}
		cutoff := day(now.UTC().Add(-archiveRetention)).Format("20060102")
		rows, err := tx.Query(ctx, `
			SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
			WHERE i.inhparent = 'jobs_archive'::regclass AND c.relname < 'jobs_archive_' || $1`, cutoff)
		if err != nil {
			return err
		}
		old, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		for _, p := range old {
			if _, err := tx.Exec(ctx, `DROP TABLE `+pgx.Identifier{p}.Sanitize()); err != nil {
				return err
			}
			slog.Info("dropped archive partition", "partition", p)
		}
		_, err = tx.Exec(ctx, `DELETE FROM processed WHERE processed_at < now() - $1 * interval '1 ms'`, archiveRetention.Milliseconds())
		return err
	})
}

// archiveOnce runs on the scheduler leader once a minute.
func archiveOnce(ctx context.Context, db *pgxpool.Pool) {
	if err := partitions(ctx, db, time.Now()); err != nil {
		slog.Error("archive partitions failed", "err", err)
		return
	}
	var total int64
	for {
		n, err := archive(ctx, db)
		total += n
		if err != nil {
			slog.Error("archive failed", "err", err)
			break
		}
		if n < archiveBatch {
			break
		}
	}
	if total > 0 {
		slog.Info("archived jobs", "jobs", total)
	}
}
