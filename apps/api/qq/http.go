package qq

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Handler is an HTTP handler that returns a status and a JSON body; the server's
// instrument wrapper encodes it and records metrics.
type Handler func(*http.Request) (int, any)

// Routes registers the app-facing API: enqueue a job, read its status, start and read a
// workflow run. No auth: keep these on a private network (the admin API is AdminRoutes).
func Routes(route func(string, Handler), db *pgxpool.Pool) {
	route("POST /api/jobs", func(r *http.Request) (int, any) {
		var j Job
		dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&j); err != nil {
			return 400, map[string]string{"error": "invalid JSON: " + err.Error()}
		}
		if err := j.validate(); err != nil {
			return 400, map[string]string{"error": err.Error()}
		}
		id, created, err := Enqueue(r.Context(), db, j)
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
		id, err := StartRun(r.Context(), db, r.PathValue("name"), body.Input)
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
		run, err := getRun(r.Context(), db, id)
		if errors.Is(err, errNoRun) {
			return 404, map[string]string{"error": "not found"}
		}
		if err != nil {
			slog.Error("get run failed", "err", err)
			return 500, map[string]string{"error": "internal error"}
		}
		return 200, run
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
		err = db.QueryRow(r.Context(), `SELECT id, queue, kind, status, attempts, worker_id, last_error, run_at, created_at, finished_at
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
