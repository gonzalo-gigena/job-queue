package main

import (
	"context"
	"encoding/json"
	"errors"
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
		       lease_until = now() + interval '30 s', worker_id = $1
		WHERE id = ANY(ARRAY(
		  SELECT id FROM jobs
		  WHERE `+match+` AND status = 'queued' AND run_at <= now()
		  ORDER BY priority DESC, run_at
		  FOR UPDATE SKIP LOCKED LIMIT $3))
		RETURNING id, kind, payload, attempts`, workerID, arg, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (job, error) {
		var j job
		err := r.Scan(&j.ID, &j.Kind, &j.Payload, &j.Attempts)
		return j, err
	})
}

// complete records the outcome, but only if this worker still holds the job.
// ponytail: a failed job stays failed; retries with backoff and the dead state come in phase 2.
func complete(ctx context.Context, db *pgxpool.Pool, id int64, workerID string, jobErr error) error {
	status, lastErr := "succeeded", (*string)(nil)
	if jobErr != nil {
		msg := jobErr.Error()
		status, lastErr = "failed", &msg
	}
	_, err := db.Exec(ctx, `
		UPDATE jobs SET status = $3, last_error = $4, finished_at = clock_timestamp(), lease_until = NULL
		WHERE id = $1 AND worker_id = $2 AND status = 'running'`, id, workerID, status, lastErr)
	return err
}
