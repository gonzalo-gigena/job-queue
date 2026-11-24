// One binary, several roles:
//
//	api              stateless HTTP service: enqueue and query jobs
//	api worker       claims and runs jobs (WORKER_QUEUES, default "default")
//	api scheduler    requeues jobs whose lease expired; the leader replica runs cron
//	api healthcheck  lets the distroless image check itself
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	requests  = promauto.NewCounterVec(prometheus.CounterOpts{Name: "http_requests_total", Help: "HTTP requests."}, []string{"route", "status"})
	durations = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "http_request_duration_seconds", Help: "HTTP request latency.", Buckets: prometheus.DefBuckets}, []string{"route"})
)

type handler func(*http.Request) (int, any)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}
	role := "api"
	if len(os.Args) > 1 {
		role = os.Args[1]
	}
	if role != "api" && role != "worker" && role != "scheduler" {
		fmt.Fprintln(os.Stderr, "usage: api [worker|scheduler|healthcheck]")
		os.Exit(2)
	}
	if err := run(role); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func port() string {
	if p := os.Getenv("PORT"); p != "" {
		return p
	}
	return "8080"
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func healthcheck() int {
	c := http.Client{Timeout: 2 * time.Second}
	res, err := c.Get("http://localhost:" + port() + "/readyz")
	if err != nil || res.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func run(role string) error {
	replica, _ := os.Hostname()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("replica", replica))

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return errors.New("missing env var DATABASE_URL")
	}
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	var draining atomic.Bool
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	errc := make(chan error, 2)
	workDone := make(chan struct{})
	mux := http.NewServeMux()
	route := func(pattern string, h handler) { mux.Handle(pattern, instrument(pattern, replica, h)) }

	route("GET /healthz", func(*http.Request) (int, any) { return 200, map[string]bool{"ok": true} })
	route("GET /readyz", func(r *http.Request) (int, any) {
		if draining.Load() {
			return 503, map[string]any{"ok": false, "reason": "draining"}
		}
		if err := pool.Ping(r.Context()); err != nil {
			return 503, map[string]any{"ok": false, "reason": "db unreachable"}
		}
		return 200, map[string]bool{"ok": true}
	})
	switch role {
	case "scheduler":
		// The advisory lock is session state: like LISTEN, it needs a direct connection.
		directURL := envOr("DIRECT_DATABASE_URL", dbURL)
		go func() {
			defer close(workDone)
			go reapLoop(ctx, pool)
			leadLoop(ctx, pool, directURL)
		}()
	case "worker":
		queues := splitQueues(envOr("WORKER_QUEUES", "default"))
		if len(queues) == 0 {
			return errors.New("WORKER_QUEUES lists no queue")
		}
		for _, q := range queues {
			if !queueName.MatchString(q) {
				return fmt.Errorf("WORKER_QUEUES: %q must match %s", q, queueName)
			}
		}
		// LISTEN must bypass any pooler; today DATABASE_URL is already direct.
		listenURL := envOr("DIRECT_DATABASE_URL", dbURL)
		go func() {
			defer close(workDone)
			if err := work(ctx, pool, listenURL, replica, queues); err != nil && ctx.Err() == nil {
				errc <- err
			}
		}()
	default:
		close(workDone)
		route("POST /api/jobs", func(r *http.Request) (int, any) {
			var j newJob
			dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&j); err != nil {
				return 400, map[string]string{"error": "invalid JSON: " + err.Error()}
			}
			if err := j.validate(); err != nil {
				return 400, map[string]string{"error": err.Error()}
			}
			id, created, err := enqueue(r.Context(), pool, j)
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "22") {
				// data exception, e.g. a NUL character in a string or an out-of-range run_at
				return 400, map[string]string{"error": "a field has a value Postgres can't store"}
			}
			if err != nil {
				slog.Error("enqueue failed", "err", err)
				return 500, map[string]string{"error": "internal error"}
			}
			if !created {
				return 200, map[string]any{"id": id, "duplicate": true}
			}
			return 201, map[string]any{"id": id}
		})
		// Dead-letter actions for the dashboard. ponytail: unauthenticated like the rest of the API; phase 6 adds a token.
		for action, fn := range map[string]func(context.Context, *pgxpool.Pool, int64) (bool, error){"retry": retryDead, "discard": discardDead} {
			route("POST /api/jobs/{id}/"+action, func(r *http.Request) (int, any) {
				id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
				if err != nil {
					return 400, map[string]string{"error": "id must be a number"}
				}
				ok, err := fn(r.Context(), pool, id)
				if err != nil {
					slog.Error(action+" failed", "err", err)
					return 500, map[string]string{"error": "internal error"}
				}
				if !ok {
					return 409, map[string]string{"error": "no dead job with that id"}
				}
				return 200, map[string]any{"id": id}
			})
		}
		route("PUT /api/schedules/{id}", func(r *http.Request) (int, any) {
			var s schedule
			dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&s); err != nil {
				return 400, map[string]string{"error": "invalid JSON: " + err.Error()}
			}
			s.ID = r.PathValue("id")
			if err := s.validate(); err != nil {
				return 400, map[string]string{"error": err.Error()}
			}
			if err := putSchedule(r.Context(), pool, s); err != nil {
				slog.Error("put schedule failed", "err", err)
				return 500, map[string]string{"error": "internal error"}
			}
			return 200, s
		})
		route("GET /api/schedules", func(r *http.Request) (int, any) {
			s, err := listSchedules(r.Context(), pool)
			if err != nil {
				slog.Error("list schedules failed", "err", err)
				return 500, map[string]string{"error": "internal error"}
			}
			return 200, s
		})
		route("DELETE /api/schedules/{id}", func(r *http.Request) (int, any) {
			tag, err := pool.Exec(r.Context(), `DELETE FROM schedules WHERE id = $1`, r.PathValue("id"))
			if err != nil {
				slog.Error("delete schedule failed", "err", err)
				return 500, map[string]string{"error": "internal error"}
			}
			if tag.RowsAffected() == 0 {
				return 404, map[string]string{"error": "not found"}
			}
			return 200, map[string]bool{"ok": true}
		})
		route("POST /api/workflows/{name}/runs", func(r *http.Request) (int, any) {
			var body struct {
				Input json.RawMessage `json:"input"`
			}
			dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&body); err != nil {
				return 400, map[string]string{"error": "invalid JSON: " + err.Error()}
			}
			if _, ok := workflows[r.PathValue("name")]; !ok {
				return 404, map[string]string{"error": "no such workflow"}
			}
			id, err := startRun(r.Context(), pool, r.PathValue("name"), body.Input)
			if err != nil {
				slog.Error("start run failed", "err", err)
				return 500, map[string]string{"error": "internal error"}
			}
			return 201, map[string]any{"id": id}
		})
		route("GET /api/workflows/runs/{id}", func(r *http.Request) (int, any) {
			id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
			if err != nil {
				return 400, map[string]string{"error": "id must be a number"}
			}
			run, err := getRun(r.Context(), pool, id)
			if errors.Is(err, errNoRun) {
				return 404, map[string]string{"error": "not found"}
			}
			if err != nil {
				slog.Error("get run failed", "err", err)
				return 500, map[string]string{"error": "internal error"}
			}
			return 200, run
		})
		route("PUT /api/queues/{queue}/limits", func(r *http.Request) (int, any) {
			var l queueLimits
			dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&l); err != nil {
				return 400, map[string]string{"error": "invalid JSON: " + err.Error()}
			}
			q := r.PathValue("queue")
			if !queueName.MatchString(q) {
				return 400, map[string]string{"error": "queue must match " + queueName.String()}
			}
			if err := l.validate(); err != nil {
				return 400, map[string]string{"error": err.Error()}
			}
			if err := setLimits(r.Context(), pool, q, l); err != nil {
				slog.Error("set limits failed", "err", err)
				return 500, map[string]string{"error": "internal error"}
			}
			return 200, l
		})
		route("DELETE /api/queues/{queue}/limits", func(r *http.Request) (int, any) {
			if _, err := pool.Exec(r.Context(), `DELETE FROM queue_limits WHERE queue = $1`, r.PathValue("queue")); err != nil {
				slog.Error("delete limits failed", "err", err)
				return 500, map[string]string{"error": "internal error"}
			}
			return 200, map[string]bool{"ok": true}
		})
		route("GET /api/jobs/{id}", func(r *http.Request) (int, any) {
			var out struct {
				ID         int64      `json:"id"`
				Queue      string     `json:"queue"`
				Kind       string     `json:"kind"`
				Status     string     `json:"status"`
				Attempts   int        `json:"attempts"`
				WorkerID   *string    `json:"worker_id"`
				LastError  *string    `json:"last_error"`
				RunAt      time.Time  `json:"run_at"`
				CreatedAt  time.Time  `json:"created_at"`
				FinishedAt *time.Time `json:"finished_at"`
			}
			id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
			if err != nil {
				return 400, map[string]string{"error": "id must be a number"}
			}
			err = pool.QueryRow(r.Context(), `SELECT id, queue, kind, status, attempts, worker_id, last_error, run_at, created_at, finished_at
				FROM jobs WHERE id = $1`, id).
				Scan(&out.ID, &out.Queue, &out.Kind, &out.Status, &out.Attempts, &out.WorkerID, &out.LastError, &out.RunAt, &out.CreatedAt, &out.FinishedAt)
			if errors.Is(err, pgx.ErrNoRows) {
				return 404, map[string]string{"error": "not found"}
			}
			if err != nil {
				slog.Error("get job failed", "err", err)
				return 500, map[string]string{"error": "internal error"}
			}
			return 200, out
		})
	}
	route("GET /api/hello", func(r *http.Request) (int, any) {
		var msg *string
		err := pool.QueryRow(r.Context(), "SELECT message FROM greetings ORDER BY id LIMIT 1").Scan(&msg)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			slog.Error("query failed", "err", err)
			return 500, map[string]string{"error": "internal error"}
		}
		return 200, map[string]any{"message": msg, "replica": replica}
	})
	route("/", func(*http.Request) (int, any) { return 404, map[string]string{"error": "not found"} })
	mux.Handle("GET /metrics", promhttp.Handler())

	srv := &http.Server{Addr: ":" + port(), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		slog.Info("listening on :" + port())
		errc <- srv.ListenAndServe()
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	draining.Store(true)
	slog.Info("SIGTERM received, draining")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // below compose stop_grace_period
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	<-workDone // the worker finishes its in-flight job first
	return err
}

// instrument adds replica/request-id headers, JSON encoding, metrics and a request log line.
func instrument(pattern, replica string, h handler) http.Handler {
	label := pattern
	if pattern == "/" {
		label = "unmatched" // fixed label set, no cardinality blow-up
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		id := r.Header.Get("X-Request-Id")
		if id == "" {
			id = rand.Text()
		}
		w.Header().Set("X-Replica", replica)
		w.Header().Set("X-Request-Id", id)
		w.Header().Set("Content-Type", "application/json")

		status, body := h(r)
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)

		elapsed := time.Since(start)
		requests.WithLabelValues(label, strconv.Itoa(status)).Inc()
		durations.WithLabelValues(label).Observe(elapsed.Seconds())
		if !strings.HasSuffix(r.URL.Path, "z") {
			slog.Info("request", "requestId", id, "method", r.Method, "path", r.URL.Path, "status", status, "ms", elapsed.Milliseconds())
		}
	})
}
