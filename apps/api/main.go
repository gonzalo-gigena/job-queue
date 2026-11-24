// One binary, several roles:
//
//	api              stateless HTTP service: enqueue and query jobs, the admin API and dashboard stream
//	api worker       claims and runs jobs (WORKER_QUEUES, default "default")
//	api scheduler    requeues jobs whose lease expired; the leader replica runs cron and archiving
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

	"github.com/gonzalo-gigena/job-queue/apps/api/qq"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	requests  = promauto.NewCounterVec(prometheus.CounterOpts{Name: "http_requests_total", Help: "HTTP requests."}, []string{"route", "status"})
	durations = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "http_request_duration_seconds", Help: "HTTP request latency.", Buckets: prometheus.DefBuckets}, []string{"route"})
)

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
	route := func(pattern string, h qq.Handler) { mux.Handle(pattern, instrument(pattern, replica, h)) }

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
			go qq.ReapLoop(ctx, pool)
			qq.LeadLoop(ctx, pool, directURL)
		}()
	case "worker":
		queues := qq.SplitQueues(envOr("WORKER_QUEUES", "default"))
		if len(queues) == 0 {
			return errors.New("WORKER_QUEUES lists no queue")
		}
		for _, q := range queues {
			if !qq.QueueName.MatchString(q) {
				return fmt.Errorf("WORKER_QUEUES: %q must match %s", q, qq.QueueName)
			}
		}
		// LISTEN is session state: it must bypass PgBouncer.
		listenURL := envOr("DIRECT_DATABASE_URL", dbURL)
		go func() {
			defer close(workDone)
			if err := qq.Work(ctx, pool, listenURL, replica, queues); err != nil && ctx.Err() == nil {
				errc <- err
			}
		}()
	default:
		close(workDone)
		qq.Routes(route, pool)
		// The dashboard and every admin action: behind ADMIN_TOKEN.
		token := os.Getenv("ADMIN_TOKEN")
		adminRoute := func(pattern string, h qq.Handler) {
			mux.Handle(pattern, qq.RequireAdmin(token, instrument(pattern, replica, h)))
		}
		qq.AdminRoutes(adminRoute, mux, token, pool, ctx)
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
func instrument(pattern, replica string, h qq.Handler) http.Handler {
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
