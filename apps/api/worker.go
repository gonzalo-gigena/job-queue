package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var jobsDone = promauto.NewCounterVec(prometheus.CounterOpts{Name: "jobs_done_total", Help: "Jobs a worker finished."}, []string{"kind", "status"})

// handlers maps a job kind to its code. Handlers must be idempotent: delivery is at-least-once.
// They should return soon after ctx is cancelled (lease lost, shutdown deadline, timeout).
var handlers = map[string]func(ctx context.Context, db *pgxpool.Pool, j job) error{
	"noop": func(context.Context, *pgxpool.Pool, job) error { return nil },
	"sleep": func(ctx context.Context, _ *pgxpool.Pool, j job) error {
		var a struct{ MS int }
		if err := json.Unmarshal(j.Payload, &a); err != nil {
			return err
		}
		return sleep(ctx, time.Duration(a.MS)*time.Millisecond)
	},
	// effect is the idempotent-handler pattern: the side effect (a row in effects) and the
	// processed(job_id) marker commit together, so a second run finds the marker and skips.
	// {"ms":…} sleeps after the commit, widening the window where a crash forces a re-run.
	// {"fail":true} always errors, to exercise retries and the dead state.
	"effect": func(ctx context.Context, db *pgxpool.Pool, j job) error {
		var a struct {
			MS   int
			Fail bool
		}
		if err := json.Unmarshal(j.Payload, &a); err != nil {
			return err
		}
		if a.Fail {
			return errors.New("asked to fail")
		}
		err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
			tag, err := tx.Exec(ctx, `INSERT INTO processed (job_id) VALUES ($1) ON CONFLICT DO NOTHING`, j.ID)
			if err != nil || tag.RowsAffected() == 0 {
				return err // already done by an earlier run
			}
			_, err = tx.Exec(ctx, `INSERT INTO effects (job_id) VALUES ($1)`, j.ID)
			return err
		})
		if err != nil {
			return err
		}
		return sleep(ctx, time.Duration(a.MS)*time.Millisecond)
	},
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// After SIGTERM, how long an in-flight job may keep running. A var so tests can shorten it.
var drainTimeout = 25 * time.Second

var (
	errLeaseLost = errors.New("lease lost: the job was taken away")
	errShutdown  = errors.New("shutdown deadline: job released")
)

const (
	pollInterval = time.Second // fallback: a run_at came due, or a NOTIFY was missed
	reapInterval = 5 * time.Second
	// claimTimeout + drainTimeout + dbTimeout stay inside compose's 35 s stop_grace_period.
	claimTimeout   = 5 * time.Second
	dbTimeout      = 4 * time.Second // complete, release, heartbeat
	handlerTimeout = 5 * time.Minute // heartbeats keep the lease, so this is only a hang guard
)

// work claims and runs jobs until ctx is cancelled. It wakes on NOTIFY jobs_<queue>
// and polls every pollInterval in case a notification was missed or a run_at came due.
// ponytail: one job at a time per process; scale with `--scale worker=N`.
// In-process concurrency comes later if a replica turns out to be idle on I/O.
func work(ctx context.Context, db *pgxpool.Pool, listenURL, workerID string, queues []string) error {
	// LISTEN is session state, so it needs its own direct connection, never a
	// transaction-mode pooler (PgBouncer would hand the session to someone else).
	conn, err := pgx.Connect(ctx, listenURL)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	for _, q := range queues {
		if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{"jobs_" + q}.Sanitize()); err != nil {
			return err
		}
	}

	// Read notifications even while a job runs, and fold a burst into one wake-up: left
	// unread they would cost one empty dequeue each once the backlog is drained.
	listenCtx, stopListening := context.WithCancel(ctx)
	wake, listening := make(chan struct{}, 1), make(chan struct{})
	var listenErr error // read only after listening is closed
	go func() {
		defer close(listening)
		for {
			if _, listenErr = conn.WaitForNotification(listenCtx); listenErr != nil {
				return
			}
			select {
			case wake <- struct{}{}:
			default:
			}
		}
	}()
	defer func() { stopListening(); <-listening }() // before conn.Close: a pgx.Conn isn't concurrency-safe
	slog.Info("worker started", "queues", queues)

	for ctx.Err() == nil {
		// Not cancelled by shutdown: a claim cancelled after Postgres committed it would
		// leave the job running with nobody working on it.
		claimCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), claimTimeout)
		jobs, err := dequeue(claimCtx, db, workerID, queues, 1)
		cancel()
		if err != nil {
			slog.Error("dequeue failed", "err", err)
			time.Sleep(pollInterval)
			continue
		}
		if len(jobs) == 0 {
			select {
			case <-ctx.Done():
			case <-wake:
			case <-time.After(pollInterval):
			case <-listening:
				if ctx.Err() == nil {
					return fmt.Errorf("listen connection: %w", listenErr) // exit; compose restarts us
				}
			}
			continue
		}
		for _, j := range jobs {
			runJob(ctx, db, workerID, j)
		}
	}
	return nil
}

// runJob executes one job and records the outcome. While the handler runs, a heartbeat
// extends the lease and abandons the job if it was taken away. On shutdown the job gets
// drainTimeout to finish, then it is cancelled and its lease released.
func runJob(shutdown context.Context, db *pgxpool.Pool, workerID string, j job) {
	ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
	defer cancel()
	ctx, abandon := context.WithCancelCause(ctx)
	defer abandon(nil)
	watching := make(chan struct{})
	go func() {
		defer close(watching)
		t := time.NewTicker(heartbeatInterval)
		defer t.Stop()
		var drain <-chan time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case <-shutdown.Done():
				drain, shutdown = time.After(drainTimeout), context.Background() // fire once
			case <-drain:
				abandon(errShutdown)
				return
			case <-t.C:
				hbCtx, c := context.WithTimeout(ctx, dbTimeout)
				held, err := heartbeat(hbCtx, db, j, workerID)
				c()
				if err != nil {
					slog.Warn("heartbeat failed", "job", j.ID, "err", err) // retry next tick; the lease has 20 s left
				} else if !held {
					abandon(errLeaseLost)
					return
				}
			}
		}
	}()

	label := j.Kind
	h, ok := handlers[j.Kind]
	var err error
	if !ok {
		err = fmt.Errorf("no handler for kind %q", j.Kind)
		label = "unknown" // kind is caller input: keep the metric's label set fixed
	} else {
		err = h(ctx, db, j)
	}
	cause := context.Cause(ctx)
	abandon(nil) // stop the heartbeat before writing the outcome
	<-watching

	// Their own deadline: a handler that timed out must still get its outcome recorded.
	saveCtx, cancelSave := context.WithTimeout(context.Background(), dbTimeout)
	defer cancelSave()
	switch {
	case errors.Is(cause, errLeaseLost):
		slog.Warn("job abandoned", "job", j.ID, "err", cause)
		return
	case errors.Is(cause, errShutdown):
		if rerr := release(saveCtx, db, j, workerID); rerr != nil {
			slog.Error("release failed", "job", j.ID, "err", rerr) // the reaper gets it when the lease runs out
		}
		slog.Warn("job released", "job", j.ID)
		return
	}
	held, cerr := complete(saveCtx, db, j, workerID, err)
	if cerr != nil {
		slog.Error("complete failed", "job", j.ID, "err", cerr) // the lease runs out and the job runs again
		return
	}
	if !held {
		slog.Warn("job finished after its lease was lost; outcome dropped", "job", j.ID)
		return
	}
	status := "succeeded"
	if err != nil {
		status = "failed"
		slog.Warn("job failed", "job", j.ID, "kind", j.Kind, "attempt", j.Attempts, "err", err)
	}
	jobsDone.WithLabelValues(label, status).Inc()
}

// reapLoop runs the reaper every reapInterval until ctx is cancelled.
// ponytail: every scheduler replica reaps; that's safe (SKIP LOCKED). Phase 3 adds a leader for cron.
func reapLoop(ctx context.Context, db *pgxpool.Pool) {
	slog.Info("reaper started")
	for {
		rctx, cancel := context.WithTimeout(ctx, dbTimeout)
		requeued, dead, err := reap(rctx, db)
		cancel()
		if err != nil && ctx.Err() == nil {
			slog.Error("reap failed", "err", err)
		} else if requeued+dead > 0 {
			slog.Warn("reaped expired leases", "requeued", requeued, "dead", dead)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(reapInterval):
		}
	}
}

func splitQueues(s string) []string {
	var qs []string
	for _, q := range strings.Split(s, ",") {
		if q = strings.TrimSpace(q); q != "" {
			qs = append(qs, q)
		}
	}
	return qs
}
