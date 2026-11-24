package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testWorkflow is a → b, c, d → e on its own queue, plus an "undo" compensation step.
// Every step returns its parents' n values summed, plus one; the step named in fail errors.
func testWorkflow(q, fail string) *workflow {
	f := func(name string) stepFunc {
		return func(_ context.Context, _ *pgxpool.Pool, in stepInput) (any, error) {
			if name == fail {
				return nil, errors.New("asked to fail")
			}
			sum := 1
			for _, out := range in.Parents {
				var o struct{ N int }
				json.Unmarshal(out, &o)
				sum += o.N
			}
			return map[string]int{"n": sum}, nil
		}
	}
	w := newWorkflow(q).
		step("a", f("a")).
		step("b", f("b"), "a").step("c", f("c"), "a").step("d", f("d"), "a").
		step("e", f("e"), "b", "c", "d").
		onFailure("undo", f("undo"))
	w.queue, w.maxAttempts = q, 1
	return w
}

func runWorkers(t *testing.T, db *pgxpool.Pool, q string, n int) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { work(ctx, db, os.Getenv("DATABASE_URL"), "w"+string(rune('0'+i)), []string{q}) })
	}
	t.Cleanup(func() { cancel(); wg.Wait() })
}

func waitRuns(t *testing.T, db *pgxpool.Pool, ids []int64) []runView {
	var runs []runView
	for range 200 {
		runs = runs[:0]
		done := true
		for _, id := range ids {
			r, err := getRun(context.Background(), db, id)
			if err != nil {
				t.Fatal(err)
			}
			done = done && r.Status != "running"
			runs = append(runs, r)
		}
		if done {
			return runs
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("runs still running after 10 s: %+v", runs)
	return nil
}

func TestWorkflowFanOutFanIn(t *testing.T) {
	db, q := testDB(t)
	testWorkflow(q, "")
	runWorkers(t, db, q, 4) // b, c and d finish at the same moment: the fan-in race the run lock closes
	var ids []int64
	for range 20 {
		id, err := startRun(context.Background(), db, q, nil)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	for _, r := range waitRuns(t, db, ids) {
		if r.Status != "succeeded" {
			t.Fatalf("run %d: %s, %+v", r.ID, r.Status, r.Steps)
		}
		for _, s := range r.Steps {
			want := map[string]string{"a": `{"n": 1}`, "b": `{"n": 2}`, "c": `{"n": 2}`, "d": `{"n": 2}`, "e": `{"n": 7}`, "undo": ""}[s.Step]
			got := string(s.Output)
			if s.Step == "undo" && s.Status == "pending" {
				continue
			}
			if s.Status != "succeeded" || got != want {
				t.Fatalf("run %d step %s: %s %s, want succeeded %s", r.ID, s.Step, s.Status, got, want)
			}
		}
	}
	var jobs, distinct int
	db.QueryRow(context.Background(), `SELECT count(*), count(DISTINCT idempotency_key) FROM jobs WHERE queue = $1`, q).Scan(&jobs, &distinct)
	if jobs != 5*len(ids) || distinct != jobs {
		t.Fatalf("%d step jobs (%d distinct), want %d", jobs, distinct, 5*len(ids))
	}
}

func TestWorkflowFailureRunsCompensation(t *testing.T) {
	db, q := testDB(t)
	testWorkflow(q, "c")
	runWorkers(t, db, q, 2)
	id, err := startRun(context.Background(), db, q, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := waitRuns(t, db, []int64{id})[0]
	time.Sleep(300 * time.Millisecond) // let the compensation step finish
	r, _ = getRun(context.Background(), db, id)
	got := map[string]string{}
	for _, s := range r.Steps {
		got[s.Step] = s.Status
	}
	if r.Status != "failed" || got["c"] != "failed" || got["e"] != "skipped" || got["undo"] != "succeeded" || got["a"] != "succeeded" {
		t.Fatalf("run %s, steps %v", r.Status, got)
	}
	// The dead step's job can't be retried alone: its run already failed and compensated.
	var jobID int64
	db.QueryRow(context.Background(), `SELECT job_id FROM workflow_steps WHERE run_id = $1 AND step = 'c'`, id).Scan(&jobID)
	if ok, err := retryDead(context.Background(), db, jobID); ok || err != nil {
		t.Fatalf("retryDead of a workflow step: %v %v", ok, err)
	}
}

func TestReaperFailsCrashingStep(t *testing.T) {
	db, q := testDB(t)
	ctx := context.Background()
	testWorkflow(q, "")
	id, err := startRun(ctx, db, q, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Step a's worker claims it and dies: with max_attempts 1 the reaper marks it dead.
	if _, _, err := dequeue(ctx, db, "w", []string{q}, 1); err != nil {
		t.Fatal(err)
	}
	db.Exec(ctx, `UPDATE jobs SET lease_until = now() - interval '1 s' WHERE queue = $1`, q)
	if _, _, err := reap(ctx, db); err != nil {
		t.Fatal(err)
	}
	r, _ := getRun(ctx, db, id)
	if r.Status != "failed" {
		t.Fatalf("run %s after its step's job died", r.Status)
	}
}
