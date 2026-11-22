package main

import (
	"context"
	"encoding/json"
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
var handlers = map[string]func(ctx context.Context, payload json.RawMessage) error{
	"noop": func(context.Context, json.RawMessage) error { return nil },
	"sleep": func(ctx context.Context, p json.RawMessage) error {
		var a struct{ MS int }
		if err := json.Unmarshal(p, &a); err != nil {
			return err
		}
		select {
		case <-time.After(time.Duration(a.MS) * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	},
}

const (
	pollInterval = time.Second // fallback: a run_at came due, or a NOTIFY was missed
	// Together these stay inside compose's stop_grace_period, and handler + complete inside the 30 s lease.
	claimTimeout    = 5 * time.Second
	handlerTimeout  = 24 * time.Second
	completeTimeout = 4 * time.Second
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
			runJob(db, workerID, j)
		}
	}
	return nil
}

// runJob executes one job and records the outcome. It ignores the shutdown signal so an
// in-flight job finishes instead of being cut off halfway.
func runJob(db *pgxpool.Pool, workerID string, j job) {
	ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
	defer cancel()
	label := j.Kind
	h, ok := handlers[j.Kind]
	var err error
	if !ok {
		err = fmt.Errorf("no handler for kind %q", j.Kind)
		label = "unknown" // kind is caller input: keep the metric's label set fixed
	} else {
		err = h(ctx, j.Payload)
	}
	// Its own deadline: a handler that timed out must still get marked failed.
	saveCtx, cancelSave := context.WithTimeout(context.Background(), completeTimeout)
	defer cancelSave()
	if cerr := complete(saveCtx, db, j.ID, workerID, err); cerr != nil {
		slog.Error("complete failed", "job", j.ID, "err", cerr)
		return
	}
	status := "succeeded"
	if err != nil {
		status = "failed"
		slog.Warn("job failed", "job", j.ID, "kind", j.Kind, "err", err)
	}
	jobsDone.WithLabelValues(label, status).Inc()
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
