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
	if isStep(j.Kind) {
		return errors.New(`kinds starting with "wf:" are workflow steps; start a run instead`)
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
		id, created, err = enqueueTx(ctx, tx, j)
		return err
	})
	return id, created, err
}

// enqueueTx is enqueue inside the caller's transaction: the job exists exactly when the
// caller's own writes commit.
func enqueueTx(ctx context.Context, tx pgx.Tx, j newJob) (id int64, created bool, err error) {
	err = tx.QueryRow(ctx, `
		INSERT INTO jobs (queue, kind, payload, priority, run_at, max_attempts, idempotency_key)
		VALUES ($1, $2, $3, $4, coalesce($5, now()), $6, $7)
		ON CONFLICT (queue, idempotency_key) DO NOTHING
		RETURNING id`,
		j.Queue, j.Kind, j.Payload, j.Priority, j.RunAt, j.MaxAttempts, j.IdempotencyKey).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) { // conflict: hand back the job that already has this key
		return id, false, tx.QueryRow(ctx, `SELECT id FROM jobs WHERE queue = $1 AND idempotency_key = $2`,
			j.Queue, j.IdempotencyKey).Scan(&id)
	}
	if err != nil {
		return 0, false, err
	}
	_, err = tx.Exec(ctx, `SELECT pg_notify($1, '')`, "jobs_"+j.Queue)
	return id, true, err
}

// dequeue claims up to limit due jobs, trying queues in the order given. When a rate limit
// held jobs back, retryIn says when the next token is due (0 otherwise).
func dequeue(ctx context.Context, db *pgxpool.Pool, workerID string, queues []string, limit int) (out []job, retryIn time.Duration, err error) {
	for _, q := range queues {
		if len(out) >= limit {
			break
		}
		js, wait, err := claim(ctx, db, workerID, q, limit-len(out))
		if err != nil {
			return out, 0, err
		}
		if wait > 0 && (retryIn == 0 || wait < retryIn) {
			retryIn = wait
		}
		out = append(out, js...)
	}
	return out, retryIn, nil
}

// claim takes up to limit due jobs from one queue, within its queue_limits row if it has one.
//
// The claim itself is one statement. SKIP LOCKED lets concurrent workers pass over rows
// another worker is claiming instead of waiting on them. Two changes from the textbook
// statement, each with a regression test:
//   - id = ANY(ARRAY(...)), not id IN (...): the planner may re-scan an IN subquery,
//     and each re-scan locks more rows, so LIMIT 1 can claim the whole queue.
//     ARRAY(...) is an InitPlan, evaluated exactly once.
//   - one queue per statement (queue = $2): with queue = ANY($2) Postgres can't read the
//     partial index in priority order and sorts every queued row on each claim.
//
// Limits: lock the queue's limits row first, then count running jobs in a later statement.
// Counting in the claim statement alone isn't enough, because concurrent workers all see the
// same count. Counting in the locking statement isn't either: under READ COMMITTED a
// statement's snapshot is taken before it waits for the lock, so it would miss the claims
// the previous lock holder just committed.
func claim(ctx context.Context, db *pgxpool.Pool, workerID, queue string, limit int) (jobs []job, retryIn time.Duration, err error) {
	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		var maxRunning *int32
		var rate, tokens *float64
		var paused bool
		err := tx.QueryRow(ctx, `
			SELECT max_running, rate_per_sec, paused,
			       least(burst, tokens + rate_per_sec * extract(epoch FROM clock_timestamp() - refilled_at))
			FROM queue_limits WHERE queue = $1 FOR UPDATE`, queue).Scan(&maxRunning, &rate, &paused, &tokens)
		limited := err == nil
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if paused {
			return nil
		}
		if tokens != nil && int(*tokens) < limit {
			limit = int(*tokens)
			if limit == 0 {
				retryIn = time.Duration((1 - *tokens) / *rate * float64(time.Second))
			}
		}
		if limit <= 0 {
			return nil
		}
		running, args := "$3", []any{workerID, queue, limit, leaseDuration.Milliseconds()}
		if maxRunning != nil {
			running, args = "least($3, $5 - (SELECT count(*) FROM jobs WHERE queue = $2 AND status = 'running'))", append(args, *maxRunning)
		}
		rows, err := tx.Query(ctx, `
			UPDATE jobs SET status = 'running', attempts = attempts + 1, started_at = clock_timestamp(),
			       lease_until = now() + interval '1 ms' * $4, worker_id = $1
			WHERE id = ANY(ARRAY(
			  SELECT id FROM jobs
			  WHERE queue = $2 AND status = 'queued' AND run_at <= now()
			  ORDER BY priority DESC, run_at
			  FOR UPDATE SKIP LOCKED LIMIT greatest(0, `+running+`)))
			RETURNING id, kind, payload, attempts`, args...)
		if err != nil {
			return err
		}
		if jobs, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (j job, err error) {
			err = r.Scan(&j.ID, &j.Kind, &j.Payload, &j.Attempts)
			return j, err
		}); err != nil || !limited || rate == nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE queue_limits SET tokens = $2, refilled_at = clock_timestamp() WHERE queue = $1`,
			queue, *tokens-float64(len(jobs)))
		return err
	})
	return jobs, retryIn, err
}

type queueLimits struct {
	MaxRunning *int32   `json:"max_running"`  // at most this many running at once, across every worker
	RatePerSec *float64 `json:"rate_per_sec"` // at most this many starts per second (token bucket)
	Burst      *float64 `json:"burst"`        // bucket size, default max(1, rate_per_sec)
}

func (l *queueLimits) validate() error {
	if l.MaxRunning != nil && *l.MaxRunning <= 0 {
		return errors.New("max_running must be positive")
	}
	if l.RatePerSec != nil && !(*l.RatePerSec > 0 && *l.RatePerSec <= 1e6) {
		return errors.New("rate_per_sec must be in (0, 1e6]")
	}
	if l.Burst == nil {
		b := 1.0
		if l.RatePerSec != nil && *l.RatePerSec > b {
			b = *l.RatePerSec
		}
		l.Burst = &b
	}
	if !(*l.Burst >= 1 && *l.Burst <= 1e6) {
		return errors.New("burst must be in [1, 1e6]")
	}
	return nil
}

// setLimits replaces a queue's limits; null fields mean unlimited. The bucket starts full.
func setLimits(ctx context.Context, db *pgxpool.Pool, queue string, l queueLimits) error {
	_, err := db.Exec(ctx, `
		INSERT INTO queue_limits (queue, max_running, rate_per_sec, burst, tokens) VALUES ($1, $2, $3, $4, $4)
		ON CONFLICT (queue) DO UPDATE SET max_running = $2, rate_per_sec = $3, burst = $4, tokens = $4, refilled_at = clock_timestamp()`,
		queue, l.MaxRunning, l.RatePerSec, l.Burst)
	return err
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
// after, if set, runs in the same transaction with the job's new status, so a workflow
// step's result and the steps it unblocks commit together with the job.
func complete(ctx context.Context, db *pgxpool.Pool, j job, workerID string, jobErr error, after func(ctx context.Context, tx pgx.Tx, status string) error) (held bool, err error) {
	var lastErr *string
	if jobErr != nil {
		msg := jobErr.Error()
		lastErr = &msg
	}
	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		var status string
		err := tx.QueryRow(ctx, `
			UPDATE jobs SET
			  status = CASE WHEN $4::text IS NULL THEN 'succeeded' WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
			  run_at = CASE WHEN $4::text IS NOT NULL AND attempts < max_attempts THEN now() + $5 * interval '1 ms' ELSE run_at END,
			  finished_at = CASE WHEN $4::text IS NULL OR attempts >= max_attempts THEN clock_timestamp() END,
			  last_error = coalesce($4, last_error), lease_until = NULL
			WHERE `+fence+` RETURNING status`, j.ID, workerID, j.Attempts, lastErr, backoff(j.Attempts).Milliseconds()).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		held = true
		if after != nil {
			return after(ctx, tx, status)
		}
		return nil
	})
	return held, err
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
// several replicas at once: SKIP LOCKED gives each expired job to one of them. A workflow
// step that dies here fails its run in the same transaction.
// ponytail: requeued jobs aren't NOTIFYed; workers find them on their next 1 s poll.
func reap(ctx context.Context, db *pgxpool.Pool) (requeued, dead int, err error) {
	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			UPDATE jobs SET
			  status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
			  finished_at = CASE WHEN attempts >= max_attempts THEN clock_timestamp() END,
			  last_error = 'lease expired: worker died or lost its connection',
			  run_at = now(), lease_until = NULL
			WHERE id = ANY(ARRAY(
			  SELECT id FROM jobs WHERE status = 'running' AND lease_until < now()
			  FOR UPDATE SKIP LOCKED LIMIT 1000))
			RETURNING kind, payload, status`)
		if err != nil {
			return err
		}
		type reaped struct {
			kind, status string
			payload      []byte
		}
		rs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (x reaped, err error) {
			err = r.Scan(&x.kind, &x.payload, &x.status)
			return x, err
		})
		if err != nil {
			return err
		}
		requeued, dead = 0, 0
		for _, r := range rs {
			if r.status == "queued" {
				requeued++
				continue
			}
			dead++
			if isStep(r.kind) {
				if err := stepDied(ctx, tx, r.payload, "lease expired: worker died or lost its connection"); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return requeued, dead, err
}

// retryDead puts a dead job back in its queue with a fresh set of attempts. Not a workflow
// step: its run already failed and ran its compensation.
func retryDead(ctx context.Context, db *pgxpool.Pool, id int64) (bool, error) {
	var ok bool
	err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		var queue string
		err := tx.QueryRow(ctx, `
			UPDATE jobs SET status = 'queued', attempts = 0, run_at = now(), finished_at = NULL
			WHERE id = $1 AND status = 'dead' AND kind NOT LIKE 'wf:%' RETURNING queue`, id).Scan(&queue)
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
