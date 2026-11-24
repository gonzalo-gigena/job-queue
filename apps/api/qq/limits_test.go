package qq

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// bigPool gives each simulated worker its own connection, so claims really run concurrently.
func bigPool(t *testing.T, n int32) *pgxpool.Pool {
	cfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = n
	db, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	return db
}

func TestConcurrencyLimitHoldsAcrossWorkers(t *testing.T) {
	_, q := testDB(t)
	db := bigPool(t, 20)
	ctx := context.Background()
	max := int32(3)
	if err := setLimits(ctx, db, q, queueLimits{MaxRunning: &max, Burst: ptr(1.0)}); err != nil {
		t.Fatal(err)
	}
	db.Exec(ctx, `INSERT INTO jobs (queue, kind) SELECT $1, 'noop' FROM generate_series(1, 150)`, q)

	var running, peak, done atomic.Int32
	var wg sync.WaitGroup
	for w := range 16 {
		wg.Go(func() {
			for done.Load() < 150 {
				jobs, _, err := dequeue(ctx, db, fmt.Sprint("w", w), []string{q}, 2)
				if err != nil {
					t.Error(err)
					return
				}
				for _, j := range jobs {
					n := running.Add(1)
					for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
					}
					time.Sleep(5 * time.Millisecond)
					running.Add(-1) // before complete: the db may count it running a bit longer, never shorter
					if _, err := complete(ctx, db, j, fmt.Sprint("w", w), nil, nil); err != nil {
						t.Error(err)
					}
					done.Add(1)
				}
			}
		})
	}
	wg.Wait()
	if p := peak.Load(); p != max {
		t.Fatalf("peak %d jobs running at once, limit %d", p, max)
	}
}

func TestRateLimit(t *testing.T) {
	_, q := testDB(t)
	db := bigPool(t, 10)
	ctx := context.Background()
	rate := 50.0
	if err := setLimits(ctx, db, q, queueLimits{RatePerSec: &rate, Burst: ptr(5.0)}); err != nil {
		t.Fatal(err)
	}
	db.Exec(ctx, `INSERT INTO jobs (queue, kind) SELECT $1, 'noop' FROM generate_series(1, 1000)`, q)
	start := time.Now()
	var claimed atomic.Int32
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for time.Since(start) < time.Second {
				jobs, _, err := dequeue(ctx, db, fmt.Sprint("w", w), []string{q}, 3)
				if err != nil {
					t.Error(err)
					return
				}
				claimed.Add(int32(len(jobs)))
			}
		})
	}
	wg.Wait()
	// One second at 50/s plus a full bucket of 5: at most 55, and not far below.
	if n := claimed.Load(); n > 55 || n < 40 {
		t.Fatalf("claimed %d in 1 s at 50/s with burst 5", n)
	}
	t.Logf("claimed %d in 1 s at 50/s, burst 5", claimed.Load())
}

func TestPausedQueueClaimsNothing(t *testing.T) {
	db, q := testDB(t)
	ctx := context.Background()
	db.Exec(ctx, `INSERT INTO queue_limits (queue, paused) VALUES ($1, true)`, q)
	db.Exec(ctx, `INSERT INTO jobs (queue, kind) VALUES ($1, 'noop')`, q)
	if jobs, _, err := dequeue(ctx, db, "w", []string{q}, 1); err != nil || len(jobs) != 0 {
		t.Fatalf("claimed %d from a paused queue (%v)", len(jobs), err)
	}
}

func ptr[T any](v T) *T { return &v }
