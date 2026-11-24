package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// A workflow is a DAG of steps defined in Go. Each step runs as an ordinary job of kind
// "wf:<workflow>.<step>", so it gets leases, retries and fencing for free. When a step's job
// succeeds, the step's output and the jobs for the steps it unblocks commit in the same
// transaction as the job's completion, under a lock on the run's row.
//
//	newWorkflow("signup").
//		step("create", create).
//		step("email", email, "create").
//		step("crm", crm, "create").
//		onFailure("cleanup", cleanup)

type stepInput struct {
	RunID   int64
	Input   json.RawMessage
	Parents map[string]json.RawMessage // outputs of the step's dependencies (on_failure steps: of every succeeded step)
}

type stepFunc func(ctx context.Context, db *pgxpool.Pool, in stepInput) (output any, err error)

type stepDef struct {
	deps      []string
	onFailure bool
	fn        stepFunc
}

type workflow struct {
	name, queue string
	maxAttempts int32    // per step job
	order       []string // definition order, so runs list their steps predictably
	steps       map[string]stepDef
}

var workflows = map[string]*workflow{}

func newWorkflow(name string) *workflow {
	w := &workflow{name: name, queue: "default", maxAttempts: 5, steps: map[string]stepDef{}}
	workflows[name] = w
	return w
}

// step adds a step that runs once every step in deps has succeeded. A dependency must be
// defined first, which also rules out cycles.
func (w *workflow) step(name string, fn stepFunc, deps ...string) *workflow {
	for _, d := range deps {
		if s, ok := w.steps[d]; !ok || s.onFailure {
			panic(fmt.Sprintf("workflow %s: step %s depends on undefined step %s", w.name, name, d))
		}
	}
	return w.add(name, stepDef{deps: deps, fn: fn})
}

// onFailure adds a compensation step: it runs only if some step fails for good.
func (w *workflow) onFailure(name string, fn stepFunc) *workflow {
	return w.add(name, stepDef{onFailure: true, fn: fn})
}

func (w *workflow) add(name string, s stepDef) *workflow {
	if _, dup := w.steps[name]; dup || strings.ContainsAny(name, ".:") {
		panic(fmt.Sprintf("workflow %s: bad or duplicate step name %q", w.name, name))
	}
	w.steps[name] = s
	w.order = append(w.order, name)
	return w
}

func isStep(kind string) bool { return strings.HasPrefix(kind, "wf:") }

type stepRef struct {
	RunID int64  `json:"run_id"`
	Step  string `json:"step"`
}

// enqueueStep inserts a step's job, keyed so a step can never be enqueued twice for one run.
func enqueueStep(ctx context.Context, tx pgx.Tx, w *workflow, runID int64, step string) error {
	payload, _ := json.Marshal(stepRef{runID, step})
	key := fmt.Sprintf("wf:%d:%s", runID, step)
	id, _, err := enqueueTx(ctx, tx, newJob{Queue: w.queue, Kind: "wf:" + w.name + "." + step, Payload: payload, MaxAttempts: w.maxAttempts, IdempotencyKey: &key})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE workflow_steps SET status = 'queued', job_id = $3 WHERE run_id = $1 AND step = $2`, runID, step, id)
	return err
}

// startRun creates a run and its steps and enqueues the steps with no dependencies.
func startRun(ctx context.Context, db *pgxpool.Pool, name string, input json.RawMessage) (runID int64, err error) {
	w, ok := workflows[name]
	if !ok {
		return 0, fmt.Errorf("no workflow %q", name)
	}
	if len(input) == 0 {
		input = json.RawMessage("{}")
	}
	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `INSERT INTO workflow_runs (workflow, input) VALUES ($1, $2) RETURNING id`, name, input).Scan(&runID); err != nil {
			return err
		}
		for _, s := range w.order {
			d := w.steps[s]
			if _, err := tx.Exec(ctx, `INSERT INTO workflow_steps (run_id, step, depends_on, on_failure) VALUES ($1, $2, $3, $4)`,
				runID, s, append([]string{}, d.deps...), d.onFailure); err != nil {
				return err
			}
		}
		for _, s := range w.order {
			if d := w.steps[s]; len(d.deps) == 0 && !d.onFailure {
				if err := enqueueStep(ctx, tx, w, runID, s); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return runID, err
}

// runStep executes a step's job and returns the completion hook that records its outcome.
func runStep(ctx context.Context, db *pgxpool.Pool, j job) (func(context.Context, pgx.Tx, string) error, error) {
	var ref stepRef
	if err := json.Unmarshal(j.Payload, &ref); err != nil {
		return nil, err
	}
	in := stepInput{RunID: ref.RunID, Parents: map[string]json.RawMessage{}}
	var name string
	if err := db.QueryRow(ctx, `SELECT workflow, input FROM workflow_runs WHERE id = $1`, ref.RunID).Scan(&name, &in.Input); err != nil {
		return nil, err
	}
	w, ok := workflows[name]
	if !ok {
		return nil, fmt.Errorf("no workflow %q in this worker", name)
	}
	def, ok := w.steps[ref.Step]
	if !ok {
		return nil, fmt.Errorf("workflow %s has no step %q in this worker", name, ref.Step)
	}
	rows, err := db.Query(ctx, `
		SELECT step, output FROM workflow_steps
		WHERE run_id = $1 AND status = 'succeeded' AND ($2 OR step = ANY($3))`, ref.RunID, def.onFailure, def.deps)
	if err != nil {
		return nil, err
	}
	var step string
	var out json.RawMessage
	if _, err := pgx.ForEachRow(rows, []any{&step, &out}, func() error { in.Parents[step] = out; return nil }); err != nil {
		return nil, err
	}

	result, err := def.fn(ctx, db, in)
	var output []byte
	if err == nil {
		if output, err = json.Marshal(result); err != nil {
			return nil, err
		}
	}
	return func(ctx context.Context, tx pgx.Tx, status string) error {
		switch status {
		case "succeeded":
			return stepSucceeded(ctx, tx, w, ref, output)
		case "dead":
			return stepFailed(ctx, tx, w, ref, err.Error())
		}
		return nil // queued for a retry: the step stays queued
	}, err
}

// lockRun serializes everything that advances one run. Without it, two parents finishing at
// the same moment would each see the other still running, and their child would never start.
func lockRun(ctx context.Context, tx pgx.Tx, runID int64) (status string, err error) {
	err = tx.QueryRow(ctx, `SELECT status FROM workflow_runs WHERE id = $1 FOR UPDATE`, runID).Scan(&status)
	return status, err
}

func stepSucceeded(ctx context.Context, tx pgx.Tx, w *workflow, ref stepRef, output []byte) error {
	runStatus, err := lockRun(ctx, tx, ref.RunID)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE workflow_steps SET status = 'succeeded', output = $3 WHERE run_id = $1 AND step = $2`,
		ref.RunID, ref.Step, output); err != nil {
		return err
	}
	if runStatus != "running" {
		return nil // the run already failed; a sibling or compensation step finishing changes nothing else
	}
	rows, err := tx.Query(ctx, `
		SELECT s.step FROM workflow_steps s
		WHERE s.run_id = $1 AND s.status = 'pending' AND NOT s.on_failure
		  AND NOT EXISTS (SELECT 1 FROM workflow_steps p
		                  WHERE p.run_id = s.run_id AND p.step = ANY(s.depends_on) AND p.status <> 'succeeded')`, ref.RunID)
	if err != nil {
		return err
	}
	ready, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	for _, s := range ready {
		if err := enqueueStep(ctx, tx, w, ref.RunID, s); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `
		UPDATE workflow_runs SET status = 'succeeded', finished_at = clock_timestamp()
		WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM workflow_steps WHERE run_id = $1 AND NOT on_failure AND status <> 'succeeded')`, ref.RunID)
	return err
}

// stepFailed fails the run once a step's job is dead: steps that haven't started are skipped
// and the on_failure steps are enqueued (the saga pattern). Steps already running finish.
func stepFailed(ctx context.Context, tx pgx.Tx, w *workflow, ref stepRef, msg string) error {
	runStatus, err := lockRun(ctx, tx, ref.RunID)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE workflow_steps SET status = 'failed', output = jsonb_build_object('error', $3::text) WHERE run_id = $1 AND step = $2`,
		ref.RunID, ref.Step, msg); err != nil {
		return err
	}
	if runStatus != "running" {
		return nil
	}
	if _, err := tx.Exec(ctx, `UPDATE workflow_runs SET status = 'failed', finished_at = clock_timestamp() WHERE id = $1`, ref.RunID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE workflow_steps SET status = 'skipped' WHERE run_id = $1 AND status = 'pending' AND NOT on_failure`, ref.RunID); err != nil {
		return err
	}
	for _, s := range w.order {
		if w.steps[s].onFailure {
			if err := enqueueStep(ctx, tx, w, ref.RunID, s); err != nil {
				return err
			}
		}
	}
	return nil
}

// stepDied is stepFailed for a step whose job the reaper marked dead (it kept crashing its worker).
func stepDied(ctx context.Context, tx pgx.Tx, payload []byte, msg string) error {
	var ref stepRef
	if err := json.Unmarshal(payload, &ref); err != nil {
		return err
	}
	var name string
	if err := tx.QueryRow(ctx, `SELECT workflow FROM workflow_runs WHERE id = $1`, ref.RunID).Scan(&name); err != nil {
		return err
	}
	w, ok := workflows[name]
	if !ok {
		w = &workflow{name: name} // the scheduler may not know this workflow: fail the run, skip compensation
	}
	return stepFailed(ctx, tx, w, ref, msg)
}

type runView struct {
	ID         int64           `json:"id"`
	Workflow   string          `json:"workflow"`
	Status     string          `json:"status"`
	Input      json.RawMessage `json:"input"`
	CreatedAt  time.Time       `json:"created_at"`
	FinishedAt *time.Time      `json:"finished_at"`
	Steps      []stepView      `json:"steps"`
}

type stepView struct {
	Step      string          `json:"step"`
	DependsOn []string        `json:"depends_on"`
	OnFailure bool            `json:"on_failure"`
	Status    string          `json:"status"`
	Output    json.RawMessage `json:"output"`
	JobID     *int64          `json:"job_id"`
}

var errNoRun = errors.New("no such run")

func getRun(ctx context.Context, db *pgxpool.Pool, id int64) (runView, error) {
	r := runView{ID: id}
	err := db.QueryRow(ctx, `SELECT workflow, status, input, created_at, finished_at FROM workflow_runs WHERE id = $1`, id).
		Scan(&r.Workflow, &r.Status, &r.Input, &r.CreatedAt, &r.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, errNoRun
	}
	if err != nil {
		return r, err
	}
	rows, err := db.Query(ctx, `SELECT step, depends_on, on_failure, status, output, job_id FROM workflow_steps WHERE run_id = $1 ORDER BY on_failure, step`, id)
	if err != nil {
		return r, err
	}
	r.Steps, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (s stepView, err error) {
		err = row.Scan(&s.Step, &s.DependsOn, &s.OnFailure, &s.Status, &s.Output, &s.JobID)
		return s, err
	})
	return r, err
}

// demo is the 5-step fan-out/fan-in DAG the workflow chaos test runs: a → b, c, d → e.
// Input {"n": 1, "ms": 20, "fail": "c"}: each step sleeps ms; the step named in fail always errors.
func init() {
	type in struct {
		N    int
		MS   int
		Fail string
	}
	step := func(name string, f func(in, map[string]json.RawMessage) (any, error)) stepFunc {
		return func(ctx context.Context, _ *pgxpool.Pool, s stepInput) (any, error) {
			var a in
			if err := json.Unmarshal(s.Input, &a); err != nil {
				return nil, err
			}
			if err := sleep(ctx, time.Duration(a.MS)*time.Millisecond); err != nil {
				return nil, err
			}
			if a.Fail == name {
				return nil, errors.New("asked to fail")
			}
			return f(a, s.Parents)
		}
	}
	n := func(raw json.RawMessage) int {
		var o struct{ N int }
		json.Unmarshal(raw, &o)
		return o.N
	}
	plusOne := func(_ in, p map[string]json.RawMessage) (any, error) { return map[string]int{"n": n(p["a"]) + 1}, nil }
	newWorkflow("demo").
		step("a", step("a", func(a in, _ map[string]json.RawMessage) (any, error) { return map[string]int{"n": a.N}, nil })).
		step("b", step("b", plusOne), "a").
		step("c", step("c", plusOne), "a").
		step("d", step("d", plusOne), "a").
		step("e", step("e", func(_ in, p map[string]json.RawMessage) (any, error) {
			return map[string]int{"n": n(p["b"]) + n(p["c"]) + n(p["d"])}, nil
		}), "b", "c", "d").
		onFailure("undo", step("undo", func(_ in, p map[string]json.RawMessage) (any, error) {
			done := []string{}
			for s := range p {
				done = append(done, s)
			}
			return map[string]any{"undid": done}, nil
		}))
}
