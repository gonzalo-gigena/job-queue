package main

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Queue names double as the NOTIFY channel suffix, so keep them to safe identifiers.
// The jobs table has the same CHECK.
var queueName = regexp.MustCompile(`^[a-z0-9_]{1,50}$`)

type newJob struct {
	Queue          string          `json:"queue"`
	Kind           string          `json:"kind"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int32           `json:"priority"` // int32 like the column, so overflow is a 400 at decode
	RunAt          *time.Time      `json:"run_at"`
	MaxAttempts    int32           `json:"max_attempts"`
	IdempotencyKey *string         `json:"idempotency_key"`
}

type job struct {
	ID       int64
	Kind     string
	Payload  json.RawMessage
	Attempts int
}

func (j *newJob) validate() error {
	if j.Queue == "" {
		j.Queue = "default"
	}
	if !queueName.MatchString(j.Queue) {
		return errors.New("queue must match " + queueName.String())
	}
	if j.Kind == "" {
		return errors.New("kind is required")
	}
	if len(j.Payload) == 0 {
		j.Payload = json.RawMessage("{}")
	}
	if j.MaxAttempts < 0 {
		return errors.New("max_attempts must be positive")
	}
	if j.MaxAttempts == 0 {
		j.MaxAttempts = 5
	}
	// The key is in a btree, which rejects entries over ~2700 bytes.
	if k := j.IdempotencyKey; k != nil && (*k == "" || len(*k) > 255) {
		return errors.New("idempotency_key must be 1 to 255 bytes")
	}
	return nil
}

// enqueue inserts a job and NOTIFYs jobs_<queue> in the same transaction, so listeners
// hear it only once the row is committed. Enqueueing a (queue, idempotency_key) that
// already exists is a no-op that returns the existing id.
func enqueue(ctx context.Context, db *pgxpool.Pool, j newJob) (id int64, created bool, err error) {
	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			INSERT INTO jobs (queue, kind, payload, priority, run_at, max_attempts, idempotency_key)
			VALUES ($1, $2, $3, $4, coalesce($5, now()), $6, $7)
			ON CONFLICT (queue, idempotency_key) DO NOTHING
			RETURNING id`,
			j.Queue, j.Kind, j.Payload, j.Priority, j.RunAt, j.MaxAttempts, j.IdempotencyKey).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) { // conflict: hand back the job that already has this key
			return tx.QueryRow(ctx, `SELECT id FROM jobs WHERE queue = $1 AND idempotency_key = $2`,
				j.Queue, j.IdempotencyKey).Scan(&id)
		}
		if err != nil {
			return err
		}
		created = true
		_, err = tx.Exec(ctx, `SELECT pg_notify($1, '')`, "jobs_"+j.Queue)
		return err
	})
	return id, created, err
}

// dequeue claims up to limit due jobs in one statement. SKIP LOCKED lets concurrent
// workers pass over rows another worker is claiming instead of waiting on them.
//
// Two changes from the textbook statement, each with a regression test:
//   - id = ANY(ARRAY(...)), not id IN (...): the planner may re-scan an IN subquery,
//     and each re-scan locks more rows, so LIMIT 1 can claim the whole queue.
//     ARRAY(...) is an InitPlan, evaluated exactly once.
//   - queue = $2 for a single queue: with queue = ANY($2) Postgres can't read the partial
//     index in priority order and sorts every queued row on each claim.
//
// ponytail: several queues in one worker still sort their backlog on every claim.
// Run a worker per queue, or claim per queue with LATERAL, if those backlogs get large.
func dequeue(ctx context.Context, db *pgxpool.Pool, workerID string, queues []string, limit int) ([]job, error) {
	match, arg := "queue = ANY($2)", any(queues)
	if len(queues) == 1 {
		match, arg = "queue = $2", queues[0]
	}
	rows, err := db.Query(ctx, `
		UPDATE jobs SET status = 'running', attempts = attempts + 1,
		       lease_until = now() + interval '1 ms' * $4, worker_id = $1
		WHERE id = ANY(ARRAY(
		  SELECT id FROM jobs
		  WHERE `+match+` AND status = 'queued' AND run_at <= now()
		  ORDER BY priority DESC, run_at
		  FOR UPDATE SKIP LOCKED LIMIT $3))
		RETURNING id, kind, payload, attempts`, workerID, arg, limit, leaseDuration.Milliseconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (job, error) {
		var j job
		err := r.Scan(&j.ID, &j.Kind, &j.Payload, &j.Attempts)
		return j, err
	})
}

// A job's lease. The worker extends it every heartbeatInterval; the reaper requeues jobs
// whose lease ran out, which means their worker died or lost touch with Postgres.
const (
	leaseDuration = 30 * time.Second
	backoffBase   = time.Second
	backoffCap    = time.Hour
)

var heartbeatInterval = 10 * time.Second // a var so tests can shorten it

// backoff is exponential with full jitter: uniform in [0, min(cap, base·2^attempt)).
// Jitter spreads retries so jobs that failed together don't retry in lockstep.
func backoff(attempt int) time.Duration {
	ceiling := backoffCap
	if attempt < 32 && backoffBase<<attempt < backoffCap { // the shift overflows past ~2^33 s
		ceiling = backoffBase << attempt
	}
	return rand.N(ceiling)
}

// Fencing: every write a worker makes to its job matches (id, worker_id, attempts).
// attempts goes up on each claim, so it is a fencing token: a worker that lost its lease
// (paused, partitioned) can't touch the job after someone else claimed it, even when the
// new claimer has the same worker_id because it is the same container restarted.
const fence = `id = $1 AND worker_id = $2 AND attempts = $3 AND status = 'running'`

// complete records the outcome if this worker still holds the job. A failed job goes back
// to queued with a backoff, or to dead once it has used max_attempts. It returns false
// when the lease was lost, in which case the outcome is dropped: the job runs again.
func complete(ctx context.Context, db *pgxpool.Pool, j job, workerID string, jobErr error) (bool, error) {
	var lastErr *string
	if jobErr != nil {
		msg := jobErr.Error()
		lastErr = &msg
	}
	tag, err := db.Exec(ctx, `
		UPDATE jobs SET
		  status = CASE WHEN $4::text IS NULL THEN 'succeeded' WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
		  run_at = CASE WHEN $4::text IS NOT NULL AND attempts < max_attempts THEN now() + $5 * interval '1 ms' ELSE run_at END,
		  finished_at = CASE WHEN $4::text IS NULL OR attempts >= max_attempts THEN clock_timestamp() END,
		  last_error = coalesce($4, last_error), lease_until = NULL
		WHERE `+fence, j.ID, workerID, j.Attempts, lastErr, backoff(j.Attempts).Milliseconds())
	return tag.RowsAffected() == 1, err
}

// heartbeat extends the lease. false means the job was taken away: abandon it.
func heartbeat(ctx context.Context, db *pgxpool.Pool, j job, workerID string) (bool, error) {
	tag, err := db.Exec(ctx, `UPDATE jobs SET lease_until = now() + $4 * interval '1 ms' WHERE `+fence,
		j.ID, workerID, j.Attempts, leaseDuration.Milliseconds())
	return tag.RowsAffected() == 1, err
}

// release hands a job back on graceful shutdown. The attempt is given back too: a deploy
// isn't the job's fault, and shouldn't push it toward dead.
func release(ctx context.Context, db *pgxpool.Pool, j job, workerID string) error {
	_, err := db.Exec(ctx, `
		UPDATE jobs SET status = 'queued', attempts = attempts - 1, run_at = now(), lease_until = NULL
		WHERE `+fence, j.ID, workerID, j.Attempts)
	return err
}

// reap requeues running jobs whose lease expired, or marks them dead once they have used
// max_attempts, so a job that keeps crashing its worker can't loop forever. Safe to run from
// several replicas at once: SKIP LOCKED gives each expired job to one of them.
// ponytail: requeued jobs aren't NOTIFYed; workers find them on their next 1 s poll.
func reap(ctx context.Context, db *pgxpool.Pool) (requeued, dead int, err error) {
	err = db.QueryRow(ctx, `
		WITH r AS (
		  UPDATE jobs SET
		    status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
		    finished_at = CASE WHEN attempts >= max_attempts THEN clock_timestamp() END,
		    last_error = 'lease expired: worker died or lost its connection',
		    run_at = now(), lease_until = NULL
		  WHERE id = ANY(ARRAY(
		    SELECT id FROM jobs WHERE status = 'running' AND lease_until < now()
		    FOR UPDATE SKIP LOCKED LIMIT 1000))
		  RETURNING status)
		SELECT count(*) FILTER (WHERE status = 'queued'), count(*) FILTER (WHERE status = 'dead') FROM r`).
		Scan(&requeued, &dead)
	return requeued, dead, err
}

// retryDead puts a dead job back in its queue with a fresh set of attempts.
func retryDead(ctx context.Context, db *pgxpool.Pool, id int64) (bool, error) {
	var ok bool
	err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		var queue string
		err := tx.QueryRow(ctx, `
			UPDATE jobs SET status = 'queued', attempts = 0, run_at = now(), finished_at = NULL
			WHERE id = $1 AND status = 'dead' RETURNING queue`, id).Scan(&queue)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		ok = true
		_, err = tx.Exec(ctx, `SELECT pg_notify($1, '')`, "jobs_"+queue)
		return err
	})
	return ok, err
}

// discardDead gives up on a dead job. It stays in the table as cancelled, for the record.
func discardDead(ctx context.Context, db *pgxpool.Pool, id int64) (bool, error) {
	tag, err := db.Exec(ctx, `UPDATE jobs SET status = 'cancelled' WHERE id = $1 AND status = 'dead'`, id)
	return tag.RowsAffected() == 1, err
}
