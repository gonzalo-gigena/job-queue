package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestCronKeepsWallClockAcrossDST(t *testing.T) {
	s := schedule{CronExpr: "0 9 * * *", Timezone: "America/New_York"}
	// 2026-03-08 is the spring-forward day in New York: 09:00 moves from 14:00 to 13:00 UTC.
	at := time.Date(2026, 3, 7, 12, 0, 0, 0, time.UTC)
	var got []string
	for range 3 {
		var err error
		if at, err = s.nextRun(at); err != nil {
			t.Fatal(err)
		}
		got = append(got, at.UTC().Format("01-02 15:04"))
	}
	if want := "[03-07 14:00 03-08 13:00 03-09 13:00]"; fmt.Sprint(got) != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	for _, bad := range []schedule{{CronExpr: "nope"}, {CronExpr: "* * * * *", Timezone: "Mars/Olympus"}} {
		if _, err := bad.nextRun(time.Now()); err == nil {
			t.Errorf("%+v accepted", bad)
		}
	}
}

func TestTickCatchesUpWithoutDuplicates(t *testing.T) {
	db, q := testDB(t)
	ctx := context.Background()
	s := schedule{ID: q, CronExpr: "* * * * * *", Queue: q, Kind: "noop"} // every second
	if err := s.validate(); err != nil {
		t.Fatal(err)
	}
	s.NextRun = time.Now().Add(-10 * time.Second).Truncate(time.Second) // the leader was down for 10 s
	if err := putSchedule(ctx, db, s); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec(ctx, `DELETE FROM schedules WHERE id = $1`, q) })

	// Two "leaders" tick at once, and again: every missed second is enqueued exactly once.
	errs := make(chan error, 2)
	for range 2 {
		go func() { _, err := tick(ctx, db); errs <- err }()
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tick(ctx, db); err != nil {
		t.Fatal(err)
	}
	var n, distinct int
	var span float64
	db.QueryRow(ctx, `SELECT count(*), count(DISTINCT run_at), extract(epoch FROM max(run_at) - min(run_at)) FROM jobs WHERE queue = $1`, q).Scan(&n, &distinct, &span)
	if n < 10 || n != distinct || int(span)+1 != n {
		t.Fatalf("%d runs, %d distinct, spanning %v s: want 10+ consecutive seconds, each once", n, distinct, span)
	}
	var next time.Time
	db.QueryRow(ctx, `SELECT next_run_at FROM schedules WHERE id = $1`, q).Scan(&next)
	if !next.After(time.Now().Add(-time.Second)) {
		t.Fatalf("next_run_at %v is still in the past", next)
	}
}

func TestLeaderFailover(t *testing.T) {
	db, _ := testDB(t)
	url := os.Getenv("DATABASE_URL")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{}, 2)
	for range 2 {
		go func() { leadLoop(ctx, db, url); done <- struct{}{} }()
	}
	defer func() { cancel(); <-done; <-done }()

	holder := func() (pid int) {
		db.QueryRow(ctx, `SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND objid = $1 AND granted`, leaderLockID).Scan(&pid)
		return pid
	}
	waitFor := func(cond func(int) bool) (int, time.Duration) {
		start := time.Now()
		for time.Since(start) < 5*time.Second {
			if pid := holder(); cond(pid) {
				return pid, time.Since(start)
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatal("no leader within 5 s")
		return 0, 0
	}
	first, _ := waitFor(func(pid int) bool { return pid != 0 })
	// Kill the leader's session, as if its process died.
	if _, err := db.Exec(ctx, `SELECT pg_terminate_backend($1)`, first); err != nil {
		t.Fatal(err)
	}
	second, took := waitFor(func(pid int) bool { return pid != 0 && pid != first })
	t.Logf("leader %d → %d in %v", first, second, took)
	var n int
	db.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND objid = $1 AND granted`, leaderLockID).Scan(&n)
	if n != 1 {
		t.Fatalf("%d sessions hold the lock", n)
	}
}
