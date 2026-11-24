package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// requireAdmin guards /api/admin/*: HTTP basic auth with ADMIN_TOKEN as the password (any
// user name), or "Authorization: Bearer <ADMIN_TOKEN>". Basic auth lets the browser prompt
// once and send it with every fetch and EventSource, with no login page to build.
func requireAdmin(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token == "" {
			http.Error(w, `{"error":"set ADMIN_TOKEN to use the admin API"}`, http.StatusServiceUnavailable)
			return
		}
		_, got, ok := r.BasicAuth()
		if !ok {
			got, _ = strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		}
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="job queue admin"`)
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type queueStats struct {
	Queue        string       `json:"queue"`
	Ready        int          `json:"ready"`     // queued and due
	Scheduled    int          `json:"scheduled"` // queued with a future run_at (cron, backoff)
	Running      int          `json:"running"`
	Dead         int          `json:"dead"`
	DonePerMin   int          `json:"done_per_min"`       // succeeded in the last minute
	FailureRate  *float64     `json:"failure_rate_5m"`    // dead / (succeeded + dead) over 5 min
	OldestReadyS *float64     `json:"oldest_ready_age_s"` // how long the oldest due job has waited
	Paused       bool         `json:"paused"`
	Limits       *queueLimits `json:"limits"`
}

// queueOverview is one pass over jobs. ponytail: a seq scan per call; archiving (phase 7)
// keeps jobs small. Add an index on (status, finished_at) if it shows up in pg_stat_statements.
func queueOverview(ctx context.Context, db *pgxpool.Pool) ([]queueStats, error) {
	rows, err := db.Query(ctx, `
		WITH s AS (
		  SELECT queue,
		    count(*) FILTER (WHERE status = 'queued' AND run_at <= now()) AS ready,
		    count(*) FILTER (WHERE status = 'queued' AND run_at > now()) AS scheduled,
		    count(*) FILTER (WHERE status = 'running') AS running,
		    count(*) FILTER (WHERE status = 'dead') AS dead,
		    count(*) FILTER (WHERE status = 'succeeded' AND finished_at > now() - interval '1 min') AS done_1m,
		    count(*) FILTER (WHERE status = 'succeeded' AND finished_at > now() - interval '5 min') AS ok_5m,
		    count(*) FILTER (WHERE status = 'dead' AND finished_at > now() - interval '5 min') AS dead_5m,
		    extract(epoch FROM now() - min(run_at) FILTER (WHERE status = 'queued' AND run_at <= now()))::float8 AS oldest
		  FROM jobs GROUP BY queue)
		SELECT coalesce(s.queue, l.queue), coalesce(ready, 0), coalesce(scheduled, 0), coalesce(running, 0), coalesce(dead, 0),
		       coalesce(done_1m, 0), CASE WHEN ok_5m + dead_5m > 0 THEN dead_5m::float8 / (ok_5m + dead_5m) END, oldest,
		       coalesce(l.paused, false), l.max_running, l.rate_per_sec, l.burst
		FROM s FULL JOIN queue_limits l ON l.queue = s.queue ORDER BY 1`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (q queueStats, err error) {
		var l queueLimits
		err = r.Scan(&q.Queue, &q.Ready, &q.Scheduled, &q.Running, &q.Dead, &q.DonePerMin, &q.FailureRate, &q.OldestReadyS,
			&q.Paused, &l.MaxRunning, &l.RatePerSec, &l.Burst)
		if l.MaxRunning != nil || l.RatePerSec != nil {
			q.Limits = &l
		}
		return q, err
	})
}

type jobRow struct {
	ID             int64           `json:"id"`
	Queue          string          `json:"queue"`
	Kind           string          `json:"kind"`
	Status         string          `json:"status"`
	Priority       int             `json:"priority"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAt          time.Time       `json:"run_at"`
	CreatedAt      time.Time       `json:"created_at"`
	StartedAt      *time.Time      `json:"started_at"`
	FinishedAt     *time.Time      `json:"finished_at"`
	LastError      *string         `json:"last_error"`
	WorkerID       *string         `json:"worker_id,omitempty"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	Payload        json.RawMessage `json:"payload,omitempty"`
}

const jobCols = `id, queue, kind, status, priority, attempts, max_attempts, run_at, created_at, started_at, finished_at`

func scanJob(r pgx.CollectableRow) (j jobRow, err error) {
	err = r.Scan(&j.ID, &j.Queue, &j.Kind, &j.Status, &j.Priority, &j.Attempts, &j.MaxAttempts, &j.RunAt, &j.CreatedAt, &j.StartedAt, &j.FinishedAt, &j.LastError)
	return j, err
}

// searchJobs lists the newest jobs matching the filters (empty = any), newest first.
func searchJobs(ctx context.Context, db *pgxpool.Pool, status, queue, kind string, beforeID int64) ([]jobRow, error) {
	if beforeID <= 0 {
		beforeID = 1 << 62
	}
	rows, err := db.Query(ctx, `
		SELECT `+jobCols+`, left(last_error, 300) FROM jobs
		WHERE ($1 = '' OR status = $1) AND ($2 = '' OR queue = $2) AND ($3 = '' OR kind = $3) AND id < $4
		ORDER BY id DESC LIMIT 50`, status, queue, kind, beforeID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanJob)
}

var errNoJob = errors.New("no such job")

func jobDetail(ctx context.Context, db *pgxpool.Pool, id int64) (j jobRow, err error) {
	err = db.QueryRow(ctx, `SELECT `+jobCols+`, last_error, worker_id, idempotency_key, payload FROM jobs WHERE id = $1`, id).
		Scan(&j.ID, &j.Queue, &j.Kind, &j.Status, &j.Priority, &j.Attempts, &j.MaxAttempts, &j.RunAt, &j.CreatedAt, &j.StartedAt, &j.FinishedAt,
			&j.LastError, &j.WorkerID, &j.IdempotencyKey, &j.Payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return j, errNoJob
	}
	return j, err
}

// cancelJob gives up on a queued or running job. A running job's worker finds out at its
// next heartbeat (within 10 s): the fence requires status = 'running', so the heartbeat
// changes 0 rows and the worker abandons the job. Not workflow steps: their run would hang.
func cancelJob(ctx context.Context, db *pgxpool.Pool, id int64) (bool, error) {
	tag, err := db.Exec(ctx, `
		UPDATE jobs SET status = 'cancelled', finished_at = clock_timestamp(), lease_until = NULL
		WHERE id = $1 AND status IN ('queued', 'running') AND kind NOT LIKE 'wf:%'`, id)
	return tag.RowsAffected() == 1, err
}

// pauseQueue stops (or resumes) claims from a queue. Running jobs finish.
func pauseQueue(ctx context.Context, db *pgxpool.Pool, queue string, paused bool) error {
	return pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO queue_limits (queue, paused) VALUES ($1, $2)
			ON CONFLICT (queue) DO UPDATE SET paused = $2`, queue, paused); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `SELECT pg_notify($1, '')`, "jobs_"+queue)
		return err
	})
}

// adminRoutes registers the dashboard's API. Every route here is behind requireAdmin.
func adminRoutes(route func(string, handler), mux *http.ServeMux, token string, db *pgxpool.Pool, shutdown context.Context) {
	idOf := func(r *http.Request) (int64, error) { return strconv.ParseInt(r.PathValue("id"), 10, 64) }
	fail := func(what string, err error) (int, any) {
		slog.Error(what+" failed", "err", err)
		return 500, map[string]string{"error": "internal error"}
	}
	decode := func(r *http.Request, v any) error {
		dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
		dec.DisallowUnknownFields()
		if err := dec.Decode(v); err != nil {
			return fmt.Errorf("invalid JSON: %w", err)
		}
		return nil
	}

	route("GET /api/admin/queues", func(r *http.Request) (int, any) {
		qs, err := queueOverview(r.Context(), db)
		if err != nil {
			return fail("queue overview", err)
		}
		return 200, qs
	})
	route("GET /api/admin/jobs", func(r *http.Request) (int, any) {
		q := r.URL.Query()
		before, _ := strconv.ParseInt(q.Get("before_id"), 10, 64)
		js, err := searchJobs(r.Context(), db, q.Get("status"), q.Get("queue"), q.Get("kind"), before)
		if err != nil {
			return fail("search jobs", err)
		}
		return 200, js
	})
	route("GET /api/admin/jobs/{id}", func(r *http.Request) (int, any) {
		id, err := idOf(r)
		if err != nil {
			return 400, map[string]string{"error": "id must be a number"}
		}
		j, err := jobDetail(r.Context(), db, id)
		if errors.Is(err, errNoJob) {
			return 404, map[string]string{"error": "not found"}
		}
		if err != nil {
			return fail("job detail", err)
		}
		return 200, j
	})
	for action, fn := range map[string]func(context.Context, *pgxpool.Pool, int64) (bool, error){"retry": retryDead, "discard": discardDead, "cancel": cancelJob} {
		route("POST /api/admin/jobs/{id}/"+action, func(r *http.Request) (int, any) {
			id, err := idOf(r)
			if err != nil {
				return 400, map[string]string{"error": "id must be a number"}
			}
			ok, err := fn(r.Context(), db, id)
			if err != nil {
				return fail(action, err)
			}
			if !ok {
				return 409, map[string]string{"error": "the job isn't in a state that allows " + action + " (retry and discard need dead, cancel needs queued or running; workflow steps can't be retried or cancelled alone)"}
			}
			return 200, map[string]any{"id": id}
		})
	}
	for action, paused := range map[string]bool{"pause": true, "resume": false} {
		route("POST /api/admin/queues/{queue}/"+action, func(r *http.Request) (int, any) {
			q := r.PathValue("queue")
			if !queueName.MatchString(q) {
				return 400, map[string]string{"error": "queue must match " + queueName.String()}
			}
			if err := pauseQueue(r.Context(), db, q, paused); err != nil {
				return fail(action, err)
			}
			return 200, map[string]any{"queue": q, "paused": paused}
		})
	}
	route("PUT /api/admin/queues/{queue}/limits", func(r *http.Request) (int, any) {
		var l queueLimits
		if err := decode(r, &l); err != nil {
			return 400, map[string]string{"error": err.Error()}
		}
		q := r.PathValue("queue")
		if !queueName.MatchString(q) {
			return 400, map[string]string{"error": "queue must match " + queueName.String()}
		}
		if err := l.validate(); err != nil {
			return 400, map[string]string{"error": err.Error()}
		}
		if err := setLimits(r.Context(), db, q, l); err != nil {
			return fail("set limits", err)
		}
		return 200, l
	})
	route("DELETE /api/admin/queues/{queue}/limits", func(r *http.Request) (int, any) {
		// Keeps the row's paused flag: only the limits go.
		if _, err := db.Exec(r.Context(), `UPDATE queue_limits SET max_running = NULL, rate_per_sec = NULL WHERE queue = $1`, r.PathValue("queue")); err != nil {
			return fail("delete limits", err)
		}
		return 200, map[string]bool{"ok": true}
	})
	route("PUT /api/admin/schedules/{id}", func(r *http.Request) (int, any) {
		var s schedule
		if err := decode(r, &s); err != nil {
			return 400, map[string]string{"error": err.Error()}
		}
		s.ID = r.PathValue("id")
		if err := s.validate(); err != nil {
			return 400, map[string]string{"error": err.Error()}
		}
		if err := putSchedule(r.Context(), db, s); err != nil {
			return fail("put schedule", err)
		}
		return 200, s
	})
	route("GET /api/admin/schedules", func(r *http.Request) (int, any) {
		s, err := listSchedules(r.Context(), db)
		if err != nil {
			return fail("list schedules", err)
		}
		return 200, s
	})
	route("DELETE /api/admin/schedules/{id}", func(r *http.Request) (int, any) {
		tag, err := db.Exec(r.Context(), `DELETE FROM schedules WHERE id = $1`, r.PathValue("id"))
		if err != nil {
			return fail("delete schedule", err)
		}
		if tag.RowsAffected() == 0 {
			return 404, map[string]string{"error": "not found"}
		}
		return 200, map[string]bool{"ok": true}
	})

	// Live updates: every second, the queue overview and the newest jobs matching the
	// filters in the query string. ponytail: each viewer polls Postgres once a second; fan out
	// one poll per replica if there are ever more than a handful of viewers.
	mux.Handle("GET /api/admin/stream", requireAdmin(token, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", 500)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-store")
		q := r.URL.Query()
		send := func() bool {
			ctx, cancel := context.WithTimeout(r.Context(), dbTimeout)
			defer cancel()
			qs, err := queueOverview(ctx, db)
			if err != nil {
				slog.Warn("stream: queue overview failed", "err", err)
				return r.Context().Err() == nil
			}
			js, err := searchJobs(ctx, db, q.Get("status"), q.Get("queue"), q.Get("kind"), 0)
			if err != nil {
				slog.Warn("stream: search failed", "err", err)
				return r.Context().Err() == nil
			}
			body, _ := json.Marshal(map[string]any{"queues": qs, "jobs": js, "at": time.Now()})
			if _, err := fmt.Fprintf(w, "data: %s\n\n", body); err != nil {
				return false
			}
			flusher.Flush()
			return true
		}
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for send() {
			select {
			case <-r.Context().Done():
				return
			case <-shutdown.Done(): // srv.Shutdown waits for idle connections, and a stream never is
				return
			case <-t.C:
			}
		}
	})))
}
