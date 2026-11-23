package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"
	_ "time/tzdata" // the distroless image has no zoneinfo

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/robfig/cron/v3"
)

var isLeader = promauto.NewGauge(prometheus.GaugeOpts{Name: "scheduler_leader", Help: "1 while this replica holds the scheduler lock."})

// Standard 5-field cron, plus an optional leading seconds field and @daily-style descriptors.
var cronParser = cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

var scheduleID = regexp.MustCompile(`^[a-z0-9_-]{1,100}$`)

type schedule struct {
	ID       string          `json:"id"`
	CronExpr string          `json:"cron"`
	Timezone string          `json:"timezone"`
	Queue    string          `json:"queue"`
	Kind     string          `json:"kind"`
	Payload  json.RawMessage `json:"payload"`
	Enabled  *bool           `json:"enabled"`
	NextRun  time.Time       `json:"next_run_at"`
}

// nextRun parses the schedule and returns its first run after t, in its timezone.
// robfig/cron computes in wall-clock time, so "0 9 * * *" stays at 09:00 local across DST.
func (s *schedule) nextRun(t time.Time) (time.Time, error) {
	if s.Timezone == "" {
		s.Timezone = "UTC"
	}
	loc, err := time.LoadLocation(s.Timezone)
	if err != nil {
		return time.Time{}, fmt.Errorf("timezone: %w", err)
	}
	c, err := cronParser.Parse(s.CronExpr)
	if err != nil {
		return time.Time{}, fmt.Errorf("cron: %w", err)
	}
	next := c.Next(t.In(loc))
	if next.IsZero() {
		return next, errors.New("cron: never fires")
	}
	return next, nil
}

func (s *schedule) validate() error {
	if !scheduleID.MatchString(s.ID) {
		return errors.New("id must match " + scheduleID.String())
	}
	j := newJob{Queue: s.Queue, Kind: s.Kind, Payload: s.Payload}
	if err := j.validate(); err != nil {
		return err
	}
	s.Queue, s.Payload = j.Queue, j.Payload
	if s.Enabled == nil {
		t := true
		s.Enabled = &t
	}
	var err error
	s.NextRun, err = s.nextRun(time.Now())
	return err
}

func putSchedule(ctx context.Context, db *pgxpool.Pool, s schedule) error {
	_, err := db.Exec(ctx, `
		INSERT INTO schedules (id, cron_expr, timezone, queue, kind, payload, enabled, next_run_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO UPDATE SET cron_expr = $2, timezone = $3, queue = $4, kind = $5, payload = $6, enabled = $7, next_run_at = $8`,
		s.ID, s.CronExpr, s.Timezone, s.Queue, s.Kind, s.Payload, *s.Enabled, s.NextRun)
	return err
}

func listSchedules(ctx context.Context, db *pgxpool.Pool) ([]schedule, error) {
	rows, err := db.Query(ctx, `SELECT id, cron_expr, timezone, queue, kind, payload, enabled, next_run_at FROM schedules ORDER BY id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (s schedule, err error) {
		err = r.Scan(&s.ID, &s.CronExpr, &s.Timezone, &s.Queue, &s.Kind, &s.Payload, &s.Enabled, &s.NextRun)
		return s, err
	})
}

// maxCatchUp caps how many missed runs one schedule enqueues per tick; the rest come next tick.
const maxCatchUp = 100

// tick enqueues every due run of every due schedule and moves next_run_at past now.
// Missed runs (the leader was down) are enqueued too, so failover skips nothing. Each run's
// idempotency key is schedule_id:time, so a brief two-leader overlap can't duplicate one.
func tick(ctx context.Context, db *pgxpool.Pool) (enqueued int, err error) {
	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, cron_expr, timezone, queue, kind, payload, next_run_at FROM schedules
			WHERE enabled AND next_run_at <= now() FOR UPDATE SKIP LOCKED LIMIT 100`)
		if err != nil {
			return err
		}
		due, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (s schedule, err error) {
			err = r.Scan(&s.ID, &s.CronExpr, &s.Timezone, &s.Queue, &s.Kind, &s.Payload, &s.NextRun)
			return s, err
		})
		if err != nil {
			return err
		}
		now := time.Now()
		for _, s := range due {
			t := s.NextRun
			for n := 0; !t.After(now) && n < maxCatchUp; n++ {
				tag, err := tx.Exec(ctx, `
					INSERT INTO jobs (queue, kind, payload, run_at, idempotency_key) VALUES ($1, $2, $3, $4, $5)
					ON CONFLICT (queue, idempotency_key) DO NOTHING`,
					s.Queue, s.Kind, s.Payload, t, s.ID+":"+t.UTC().Format(time.RFC3339))
				if err != nil {
					return err
				}
				enqueued += int(tag.RowsAffected())
				if t, err = s.nextRun(t); err != nil {
					return err
				}
			}
			if _, err := tx.Exec(ctx, `UPDATE schedules SET next_run_at = $2 WHERE id = $1`, s.ID, t); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `SELECT pg_notify($1, '')`, "jobs_"+s.Queue); err != nil {
				return err
			}
		}
		return nil
	})
	return enqueued, err
}

// Any constant works; it only has to be the same in every replica.
const leaderLockID = 7_171_003

var leaderRetry = time.Second // how often a standby tries the lock: failover takes about this long

// leadLoop runs cron on whichever scheduler replica holds an advisory lock. The lock lives on
// a direct session (never PgBouncer): if the leader dies, Postgres closes its session and the
// lock frees, and a standby takes it on its next try.
func leadLoop(ctx context.Context, db *pgxpool.Pool, directURL string) {
	for ctx.Err() == nil {
		if err := lead(ctx, db, directURL); err != nil && ctx.Err() == nil {
			slog.Error("leader session ended", "err", err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(leaderRetry):
		}
	}
}

// lead returns when it can't get the lock, or when it had it and lost it.
func lead(ctx context.Context, db *pgxpool.Pool, directURL string) error {
	conn, err := pgx.Connect(ctx, directURL)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background()) // closing the session releases the lock
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, leaderLockID).Scan(&got); err != nil || !got {
		return err
	}
	slog.Info("became scheduler leader")
	isLeader.Set(1)
	defer isLeader.Set(0)
	for {
		// Still holding the session (and so the lock)? Stop leading as soon as we can't tell.
		pctx, cancel := context.WithTimeout(ctx, dbTimeout)
		err := conn.Ping(pctx)
		cancel()
		if err != nil {
			return fmt.Errorf("lost leader session: %w", err)
		}
		tctx, cancel := context.WithTimeout(ctx, dbTimeout)
		n, err := tick(tctx, db)
		cancel()
		if err != nil && ctx.Err() == nil {
			slog.Error("cron tick failed", "err", err)
		} else if n > 0 {
			slog.Info("cron enqueued", "jobs", n)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
		}
	}
}
