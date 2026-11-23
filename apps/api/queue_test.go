package main

// Integration tests against real Postgres with the migrations applied.
// Run: docker compose -f compose.yaml run --rm test (skipped without DATABASE_URL).

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testDB(t *testing.T) (*pgxpool.Pool, string) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set")
	}
	db, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	queue := "t_" + strings.ToLower(rand.Text()[:10]) // own queue, so tests never see each other's jobs
	return db, queue
}

func TestEnqueueIsIdempotent(t *testing.T) {
	db, q := testDB(t)
	ctx := context.Background()
	key := "signup:42"

	// 20 concurrent enqueues of the same key: one creates it, all get the same id.
	var wg sync.WaitGroup
	ids, created := make([]int64, 20), make([]bool, 20)
	for i := range ids {
		wg.Go(func() {
			var err error
			ids[i], created[i], err = enqueue(ctx, db, newJob{Queue: q, Kind: "noop", Payload: []byte("{}"), MaxAttempts: 5, IdempotencyKey: &key})
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()

	nCreated := 0
	for i := range ids {
		if ids[i] != ids[0] {
			t.Fatalf("different ids for one key: %v", ids)
		}
		if created[i] {
			nCreated++
		}
	}
	if nCreated != 1 {
		t.Fatalf("created %d times, want 1", nCreated)
	}
	var rows int
	db.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE queue = $1", q).Scan(&rows)
	if rows != 1 {
		t.Fatalf("%d rows, want 1", rows)
	}

	// No key means no deduplication.
	for range 2 {
		if _, _, err := enqueue(ctx, db, newJob{Queue: q, Kind: "noop", Payload: []byte("{}"), MaxAttempts: 5}); err != nil {
			t.Fatal(err)
		}
	}
	db.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE queue = $1", q).Scan(&rows)
	if rows != 3 {
		t.Fatalf("%d rows, want 3", rows)
	}
}

func TestNoJobGoesToTwoWorkers(t *testing.T) {
	db, q := testDB(t)
	ctx := context.Background()
	const nJobs, nWorkers = 2000, 16
	// The default pool is max(4, NumCPU) connections, which would serialise the claims on CI.
	cfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = nWorkers
	db, err = pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)

	_, err = db.Exec(ctx, `INSERT INTO jobs (queue, kind, priority) SELECT $1, 'noop', g % 3 FROM generate_series(1, $2) g`, q, nJobs)
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	claimedBy := map[int64]string{}
	var wg sync.WaitGroup
	for w := range nWorkers {
		wg.Go(func() {
			id := fmt.Sprint("w", w)
			for {
				jobs, err := dequeue(ctx, db, id, []string{q}, 3)
				if err != nil {
					t.Error(err)
					return
				}
				if len(jobs) == 0 {
					return
				}
				mu.Lock()
				for _, j := range jobs {
					if prev, dup := claimedBy[j.ID]; dup {
						t.Errorf("job %d claimed by %s and %s", j.ID, prev, id)
					}
					claimedBy[j.ID] = id
				}
				mu.Unlock()
				for _, j := range jobs {
					if held, err := complete(ctx, db, j, id, nil); err != nil || !held {
						t.Error("complete:", held, err)
					}
				}
			}
		})
	}
	wg.Wait()

	workers := map[string]bool{}
	for _, w := range claimedBy {
		workers[w] = true
	}
	if len(workers) < 2 {
		t.Fatalf("only %d worker got jobs, so nothing was tested concurrently", len(workers))
	}
	if len(claimedBy) != nJobs {
		t.Fatalf("claimed %d jobs, want %d", len(claimedBy), nJobs)
	}
	var left, attempts int
	db.QueryRow(ctx, "SELECT count(*) FILTER (WHERE status <> 'succeeded'), max(attempts) FROM jobs WHERE queue = $1", q).Scan(&left, &attempts)
	if left != 0 || attempts != 1 {
		t.Fatalf("%d jobs not succeeded, max attempts %d; want 0 and 1", left, attempts)
	}
}

func TestDequeueOrderAndRunAt(t *testing.T) {
	db, q := testDB(t)
	ctx := context.Background()
	later := time.Now().Add(time.Hour)
	for _, j := range []newJob{
		{Kind: "low", Priority: 0},
		{Kind: "high", Priority: 10},
		{Kind: "future", Priority: 100, RunAt: &later},
	} {
		j.Queue, j.Payload, j.MaxAttempts = q, []byte("{}"), 5
		if _, _, err := enqueue(ctx, db, j); err != nil {
			t.Fatal(err)
		}
	}
	// One at a time: RETURNING doesn't promise an order within a batch.
	var got []string
	for {
		jobs, err := dequeue(ctx, db, "w", []string{q}, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(jobs) == 0 {
			break
		}
		got = append(got, jobs[0].Kind)
	}
	if fmt.Sprint(got) != "[high low]" {
		t.Fatalf("got %v, want high then low, and no future job", got)
	}
}

func TestDequeueNeverExceedsLimit(t *testing.T) {
	_, q := testDB(t)
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	// Leave the planner only the nested loop that makes `id IN (... LIMIT)` re-scan.
	// The seq scan is what the planner picks anyway while the table is small.
	for _, o := range []string{"hashagg", "hashjoin", "mergejoin", "sort", "material", "indexscan", "indexonlyscan", "bitmapscan"} {
		cfg.ConnConfig.RuntimeParams["enable_"+o] = "off"
	}
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if _, err := db.Exec(ctx, `INSERT INTO jobs (queue, kind) SELECT $1, 'noop' FROM generate_series(1, 5)`, q); err != nil {
		t.Fatal(err)
	}
	for _, queues := range [][]string{{q}, {q, "other"}} {
		jobs, err := dequeue(ctx, db, "w", queues, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(jobs) != 1 {
			t.Fatalf("LIMIT 1 claimed %d jobs from %v", len(jobs), queues)
		}
	}
}

func TestDequeueReadsIndexInOrder(t *testing.T) {
	_, q := testDB(t)
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1 // table stats are per backend until flushed
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)

	const backlog, claims = 50000, 20 // enough claims to reach the cached generic plan too
	if _, err := db.Exec(ctx, `INSERT INTO jobs (queue, kind, priority) SELECT $1, 'noop', g % 3 FROM generate_series(1, $2) g`, q, backlog); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec(ctx, `DELETE FROM jobs WHERE queue = $1`, q) })
	if _, err := db.Exec(ctx, "ANALYZE jobs"); err != nil {
		t.Fatal(err)
	}
	rowsRead := func() (n int64) {
		if _, err := db.Exec(ctx, "SELECT pg_stat_force_next_flush()"); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(ctx, "SELECT seq_tup_read + coalesce(idx_tup_fetch, 0) FROM pg_stat_user_tables WHERE relname = 'jobs'").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	before := rowsRead()
	for range claims {
		if _, err := dequeue(ctx, db, "w", []string{q}, 1); err != nil {
			t.Fatal(err)
		}
	}
	// Rows, not milliseconds, so a busy machine can't fail it. Sorting reads the whole backlog per claim.
	each := (rowsRead() - before) / claims
	if each > 1000 {
		t.Fatalf("%d rows read per claim with %d queued: is it sorting the backlog?", each, backlog)
	}
	t.Logf("%d rows read per claim with %d queued", each, backlog)
}

func TestValidate(t *testing.T) {
	long, empty := strings.Repeat("k", 256), ""
	for name, j := range map[string]newJob{
		"no kind":          {},
		"bad queue":        {Kind: "noop", Queue: `x"; DROP`},
		"negative attempt": {Kind: "noop", MaxAttempts: -1},
		"long key":         {Kind: "noop", IdempotencyKey: &long},
		"empty key":        {Kind: "noop", IdempotencyKey: &empty},
	} {
		if j.validate() == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	ok := newJob{Kind: "noop"}
	if err := ok.validate(); err != nil || ok.Queue != "default" || ok.MaxAttempts != 5 || string(ok.Payload) != "{}" {
		t.Errorf("defaults: %+v, %v", ok, err)
	}
}

func TestEnqueueNotifiesAfterCommit(t *testing.T) {
	db, q := testDB(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "LISTEN jobs_"+q); err != nil {
		t.Fatal(err)
	}
	if _, _, err := enqueue(ctx, db, newJob{Queue: q, Kind: "noop", Payload: []byte("{}"), MaxAttempts: 5}); err != nil {
		t.Fatal(err)
	}
	wait, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	n, err := conn.WaitForNotification(wait)
	if err != nil {
		t.Fatal("no NOTIFY after enqueue:", err)
	}
	if n.Channel != "jobs_"+q {
		t.Fatalf("channel %q", n.Channel)
	}
}

func TestWorkerHandlesJob(t *testing.T) {
	db, q := testDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- work(ctx, db, os.Getenv("DATABASE_URL"), "w1", []string{q}) }()
	defer func() { cancel(); <-done }()

	time.Sleep(200 * time.Millisecond) // let it LISTEN and go idle
	id, _, err := enqueue(ctx, db, newJob{Queue: q, Kind: "noop", Payload: []byte("{}"), MaxAttempts: 5})
	if err != nil {
		t.Fatal(err)
	}
	var status string
	var secs float64
	for range 100 {
		db.QueryRow(ctx, "SELECT status, coalesce(extract(epoch FROM finished_at - created_at), 0) FROM jobs WHERE id = $1", id).Scan(&status, &secs)
		if status == "succeeded" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if status != "succeeded" {
		t.Fatalf("status %q after 1 s", status)
	}
	took := time.Duration(secs * float64(time.Second))
	// Well under the 1 s poll interval, so NOTIFY woke the worker.
	if took > 500*time.Millisecond {
		t.Fatalf("enqueue → handled took %v; NOTIFY wake-up isn't working", took)
	}
	t.Logf("enqueue → handled: %v", took)
}

func TestBackoff(t *testing.T) {
	for attempt, ceiling := range map[int]time.Duration{1: 2 * time.Second, 5: 32 * time.Second, 12: time.Hour, 40: time.Hour, 1000: time.Hour} {
		var maxSeen time.Duration
		for range 1000 {
			d := backoff(attempt)
			if d < 0 || d >= ceiling {
				t.Fatalf("attempt %d: %v outside [0, %v)", attempt, d, ceiling)
			}
			maxSeen = max(maxSeen, d)
		}
		if maxSeen < ceiling/2 { // full jitter uses the whole range
			t.Fatalf("attempt %d: max of 1000 draws %v, want near %v", attempt, maxSeen, ceiling)
		}
	}
}

// claimOne claims the queue's next job, failing the test if there is none.
func claimOne(t *testing.T, db *pgxpool.Pool, worker, q string) job {
	t.Helper()
	jobs, err := dequeue(context.Background(), db, worker, []string{q}, 1)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: %d jobs, %v", len(jobs), err)
	}
	return jobs[0]
}

func jobState(t *testing.T, db *pgxpool.Pool, id int64) (status string, attempts int, runIn time.Duration) {
	t.Helper()
	var secs float64
	if err := db.QueryRow(context.Background(), `SELECT status, attempts, extract(epoch FROM run_at - now()) FROM jobs WHERE id = $1`, id).
		Scan(&status, &attempts, &secs); err != nil {
		t.Fatal(err)
	}
	return status, attempts, time.Duration(secs * float64(time.Second))
}

func TestRetriesThenDeadLetter(t *testing.T) {
	db, q := testDB(t)
	ctx := context.Background()
	id, _, err := enqueue(ctx, db, newJob{Queue: q, Kind: "noop", Payload: []byte("{}"), MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	fail := errors.New("boom")

	j := claimOne(t, db, "w", q)
	if held, err := complete(ctx, db, j, "w", fail); err != nil || !held {
		t.Fatal(held, err)
	}
	status, attempts, runIn := jobState(t, db, id)
	if status != "queued" || attempts != 1 || runIn < 0 || runIn > 2*time.Second {
		t.Fatalf("after 1st failure: %s, %d attempts, runs in %v; want queued, 1, within the 2 s backoff", status, attempts, runIn)
	}

	db.Exec(ctx, `UPDATE jobs SET run_at = now() WHERE id = $1`, id) // skip the wait
	j = claimOne(t, db, "w", q)
	if held, err := complete(ctx, db, j, "w", fail); err != nil || !held {
		t.Fatal(held, err)
	}
	if status, _, _ := jobState(t, db, id); status != "dead" {
		t.Fatalf("after max_attempts failures: %s, want dead", status)
	}

	if ok, err := discardDead(ctx, db, id+1_000_000); ok || err != nil {
		t.Fatal("discarded a job that isn't dead:", ok, err)
	}
	if ok, err := retryDead(ctx, db, id); !ok || err != nil {
		t.Fatal("retry:", ok, err)
	}
	if status, attempts, _ := jobState(t, db, id); status != "queued" || attempts != 0 {
		t.Fatalf("after retry: %s with %d attempts, want queued with 0", status, attempts)
	}
	j = claimOne(t, db, "w", q)
	if held, err := complete(ctx, db, j, "w", nil); err != nil || !held {
		t.Fatal(held, err)
	}
	if status, _, _ := jobState(t, db, id); status != "succeeded" {
		t.Fatalf("retried job: %s, want succeeded", status)
	}
}

func TestReaperAndFencing(t *testing.T) {
	db, q := testDB(t)
	ctx := context.Background()
	id, _, err := enqueue(ctx, db, newJob{Queue: q, Kind: "noop", Payload: []byte("{}"), MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	expire := func() { db.Exec(ctx, `UPDATE jobs SET lease_until = now() - interval '1 s' WHERE id = $1`, id) }

	zombie := claimOne(t, db, "w1", q)
	if held, err := heartbeat(ctx, db, zombie, "w1"); !held || err != nil {
		t.Fatal("heartbeat on a held job:", held, err)
	}
	expire()
	if _, _, err := reap(ctx, db); err != nil {
		t.Fatal(err)
	}
	if status, _, _ := jobState(t, db, id); status != "queued" {
		t.Fatalf("expired lease: %s, want queued", status)
	}

	// The same worker id claims it again, as a restarted container would.
	fresh := claimOne(t, db, "w1", q)
	if held, _ := heartbeat(ctx, db, zombie, "w1"); held {
		t.Fatal("the zombie's heartbeat extended a lease it lost")
	}
	if held, _ := complete(ctx, db, zombie, "w1", nil); held {
		t.Fatal("the zombie completed a job it lost")
	}
	if status, _, _ := jobState(t, db, id); status != "running" {
		t.Fatalf("after the zombie's writes: %s, want still running", status)
	}

	// Its lease expires on the last attempt: dead, not another loop.
	expire()
	if _, dead, err := reap(ctx, db); err != nil || dead < 1 {
		t.Fatal("reap:", dead, err)
	}
	if status, _, _ := jobState(t, db, id); status != "dead" {
		t.Fatalf("expired on its last attempt: %s, want dead", status)
	}
	if held, _ := complete(ctx, db, fresh, "w1", nil); held {
		t.Fatal("completed a job the reaper marked dead")
	}
}

func TestHeartbeatAbandonsLostJob(t *testing.T) {
	db, q := testDB(t)
	ctx := context.Background()
	defer func(d time.Duration) { heartbeatInterval = d }(heartbeatInterval)
	heartbeatInterval = 50 * time.Millisecond
	id, _, _ := enqueue(ctx, db, newJob{Queue: q, Kind: "sleep", Payload: []byte(`{"ms":10000}`), MaxAttempts: 5})
	j := claimOne(t, db, "w", q)

	done := make(chan struct{})
	go func() { runJob(ctx, db, "w", j); close(done) }()
	time.Sleep(100 * time.Millisecond)
	db.Exec(ctx, `UPDATE jobs SET status = 'queued', lease_until = NULL WHERE id = $1`, id) // what the reaper does
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler kept running after its lease was taken")
	}
	if status, _, _ := jobState(t, db, id); status != "queued" {
		t.Fatalf("abandoned job: %s, want left queued", status)
	}
}

func TestGracefulShutdown(t *testing.T) {
	db, q := testDB(t)
	defer func(d time.Duration) { drainTimeout = d }(drainTimeout)
	drainTimeout = 300 * time.Millisecond
	bg := context.Background()
	short, _, _ := enqueue(bg, db, newJob{Queue: q, Kind: "sleep", Payload: []byte(`{"ms":100}`), MaxAttempts: 5})
	long, _, _ := enqueue(bg, db, newJob{Queue: q, Kind: "sleep", Payload: []byte(`{"ms":60000}`), MaxAttempts: 5})

	for _, tc := range []struct {
		id       int64
		status   string
		attempts int
	}{{short, "succeeded", 1}, {long, "queued", 0}} {
		ctx, cancel := context.WithCancel(bg)
		j := claimOne(t, db, "w", q)
		if j.ID != tc.id {
			t.Fatalf("claimed %d, want %d", j.ID, tc.id)
		}
		done := make(chan struct{})
		go func() { runJob(ctx, db, "w", j); close(done) }()
		time.Sleep(20 * time.Millisecond)
		cancel() // SIGTERM
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("runJob ignored the drain deadline")
		}
		// An in-flight job that fits the deadline finishes; one that doesn't is handed back without using an attempt.
		if status, attempts, _ := jobState(t, db, tc.id); status != tc.status || attempts != tc.attempts {
			t.Fatalf("job %d: %s with %d attempts, want %s with %d", tc.id, status, attempts, tc.status, tc.attempts)
		}
	}
}

func TestEffectRunsOnce(t *testing.T) {
	db, q := testDB(t)
	ctx := context.Background()
	id, _, _ := enqueue(ctx, db, newJob{Queue: q, Kind: "effect", Payload: []byte("{}"), MaxAttempts: 5})
	j := job{ID: id, Kind: "effect", Payload: []byte("{}")}
	for range 3 { // at-least-once: the same job delivered three times
		if err := handlers["effect"](ctx, db, j); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	db.QueryRow(ctx, `SELECT count(*) FROM effects WHERE job_id = $1`, id).Scan(&n)
	if n != 1 {
		t.Fatalf("%d effects for one job, want 1", n)
	}
}
